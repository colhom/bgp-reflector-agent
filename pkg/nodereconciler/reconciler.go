package nodereconciler

import (
	"bytes"
	"fmt"
	"time"

	"strconv"

	"github.com/coreos/bgp-reflector-agent/pkg/k8sutil"
	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sort"

	"k8s.io/apimachinery/pkg/conversion/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// Maximum amount of time between reconiliation passes. 0 disables resync logic
	maxResyncPeriod = 5 * time.Minute

	// Number of times we retry updating a node
	updateMaxRetry = 10

	// How many pending triggers are allowed to queue up before we stop appending trigger events
	// Given that the informer events are never more up to date than informer cache, this should never need to be higher
	triggerWindow = 1
)

var reflectorSelector, nonReflectorSelector labels.Selector

func init() {
	var err error
	reflectorSelector, err = labels.Parse(fmt.Sprintf("%s=subnet", libcalicostub.NodeBgpReflectorLabel))
	if err != nil {
		panic(err)
	}

	nonReflectorSelector, err = labels.Parse(fmt.Sprintf("!%s", libcalicostub.NodeBgpReflectorLabel))
	if err != nil {
		panic(err)
	}
}

//TODO: expose reconciler state/metrics (last run, errors, etc) via health check and/or prom metrics endpoint
type Reconciler struct {
	clientset    kubernetes.Interface
	crdClient    dynamic.Interface
	nodeInformer informersv1.NodeInformer
	triggerQueue chan *time.Time
}

func NewReconciler(
	clientset kubernetes.Interface,
	crdClient dynamic.Interface,
) *Reconciler {

	informerFactory := informers.NewSharedInformerFactory(clientset, maxResyncPeriod)

	nodeInformer := informerFactory.Core().V1().Nodes()

	// create default reflectorspersubnet globalbgpconfig if it does not exist
	crdResource := k8sutil.GlobalBGPConfigResource(crdClient)
	if _, err := crdResource.Get(libcalicostub.ReflectorsPerSubnetName, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			glog.Warningf("reconciler: could not find reflectorspersubnet globalbgpconfig: %v", err)
			if err := k8sutil.CreateReflectorsPerSubnetConfig(crdResource, 2); err != nil {
				glog.Fatalf("reconciler: error creating globalconfig %v", err)
			}
		} else {
			glog.Fatalf("reconciler: error reading globalbgpconfig: %v", err)
		}
	}

	reconciler := &Reconciler{
		clientset:    clientset,
		crdClient:    crdClient,
		nodeInformer: nodeInformer,
		triggerQueue: make(chan *time.Time, triggerWindow),
	}

	nodeInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(node interface{}) {
				glog.Info("reconciler: node add trigger")
				reconciler.triggerResync()
			},
			UpdateFunc: func(oldNode, newNode interface{}) {
				if relevantStateChanged(k8sutil.MustCastNode(oldNode), k8sutil.MustCastNode(newNode)) {
					glog.Info("reconciler: node update trigger")
					reconciler.triggerResync()
				}
			},
			DeleteFunc: func(node interface{}) {
				glog.Info("reconciler: node delete trigger")
				reconciler.triggerResync()
			},
		})

	return reconciler
}

func (r *Reconciler) Run(stopCh <-chan struct{}) {
	defer glog.Info("reconciler: Run() - all stop channels closed")

	glog.Info("reconciler: starting reconciliation loop")
	defer close(r.triggerQueue)
	go r.reconciliationLoop()

	glog.Info("reconciler: starting node informer loop")
	informerStop := make(chan struct{})
	defer close(informerStop)
	go r.nodeInformer.Informer().Run(informerStop)

	glog.Info("reconciler: starting globalbgpconfig watch loop")
	gbcWatchStop := make(chan struct{})
	defer close(gbcWatchStop)
	go k8sutil.WatchGlobalBGPConfigs(r.crdClient, r.globalBGPConfigHandler, gbcWatchStop)

	glog.Info("reconciler: Run() waiting on stop signal")
	<-stopCh
	glog.Info("reconciler: Run() caught stop signal. waiting on stop channels")
}

func (r *Reconciler) globalBGPConfigHandler(cfg *libcalicostub.GlobalBGPConfig) error {
	switch cfg.Name {
	case libcalicostub.ReflectorsPerSubnetName: //add additional globalbgpconfig types to this case
		glog.Infof("reconciler/watch: globalbgpconfig watch event for '%s' triggered resync", cfg.Name)
		r.triggerResync()
	default:
		glog.Infof("reconciler/watch: globalbgpconfig watch event for '%s' ignored", cfg.Name)
	}
	return nil
}

func (r *Reconciler) triggerResync() {
	now := time.Now()
	select {
	case r.triggerQueue <- &now:
		glog.Infof("reconciler: sync trigger queued for %s", now)
	default:
		glog.Infof("reconciler: sync trigger ignored for %s - sync queue already full", now)
	}
}

func (r *Reconciler) reconciliationLoop() {
	resyncTimer := time.NewTimer(maxResyncPeriod)
	for {
		select {
		case <-resyncTimer.C:
			glog.Info("reconcile: max resync timer fired")
		case t := <-r.triggerQueue:
			if t == nil {
				glog.Info("reconcile: trigger queue closed")
				return
			}
			glog.Infof("reconcile: trigger queue received: %s", t.String())
		}
		r.syncSubnetReflectors()
		resyncTimer.Reset(maxResyncPeriod)
	}
}

const (
	nodeLogFmt = "reconciler: node update -> name: %-30s ready: %-5t subnet: %-18s action: %-8s"
	syncLogFmt = `
Summary:
 * kept %d reflector nodes as-is
 * activated %d reflector nodes
 * deactivated %d reflector nodes
 * left %d non-reflector nodes as-is
 -----------------------------------------
  Node Subnet CIDR   | Ready Nodes | Unready Nodes | Reflectors
 -----------------------------------------
`
	syncRowLogFmt = "%-20s |    %-5d    |     %-5d     | %-5d\n"
)

type reflectorAction string

const (
	actionNoop       reflectorAction = "noop"       // leave a non-reflector as-is
	actionKeep       reflectorAction = "keep"       // leave a reflector as- is
	actionActivate   reflectorAction = "activate"   // non-reflector --> reflector
	actionDeactivate reflectorAction = "deactivate" // reflector --> non-reflector
)

type nodeCount struct {
	Ready   int
	Unready int
}

func (r *Reconciler) syncSubnetReflectors() {
	reflectorsPerSubnet, err := r.getReflectorsPerSubnet()
	if err != nil {
		glog.Fatalf("reconciler: error getting reflectors per subnet: %v", err)
	}
	glog.Infof("starting reconciliation pass: configuring %d reflectors per subnet", reflectorsPerSubnet)

	reflectorCounts := map[string]int{}
	nodeCounts := map[string]*nodeCount{}

	reflectorNodes, err := r.nodeInformer.Lister().List(reflectorSelector)
	if err != nil {
		glog.Fatalf("error listing reflector nodes: %v", err)
	}

	nonReflectorNodes, err := r.nodeInformer.Lister().List(nonReflectorSelector)
	if err != nil {
		glog.Fatalf("error listing non-reflector nodes: %v", err)
	}
	glog.Infof("found %d reflector nodes and %d non-reflector nodes", len(reflectorNodes), len(nonReflectorNodes))
	actionLog := make(map[reflectorAction]int)

	//returns (actionTaken,err)
	transitionSubnetReflectorState := func(node *v1.Node) (reflectorAction, error) {
		network := node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel]
		reflector := node.Labels[libcalicostub.NodeBgpReflectorLabel]

		if k8sutil.NodeReady(node) && reflectorCounts[network] < reflectorsPerSubnet {
			// Should be a reflector
			switch reflector {
			case "subnet":
				return actionKeep, nil
			case "":
				node.Labels[libcalicostub.NodeBgpReflectorLabel] = "subnet"
				return actionActivate, nil
			}
		}
		// Shouldn't be a reflector
		switch reflector {
		case "subnet":
			delete(node.Labels, libcalicostub.NodeBgpReflectorLabel)
			return actionDeactivate, nil
		case "":
			return actionNoop, nil
		}

		return "", fmt.Errorf("node %s: unsupported label value for %s= %s",
			node.Name, libcalicostub.NodeBgpReflectorLabel, node.Labels[libcalicostub.NodeBgpReflectorLabel])
	}

	//It's important to always process existing reflector nodes first before any non-reflector nodes
	//so we have the opportunity to acknowledge their existence before appointing new ones to meet quota
	for _, staleNode := range append(reflectorNodes, nonReflectorNodes...) {
		retriesLeft := updateMaxRetry
		for ; retriesLeft > 0; retriesLeft-- {
			node, err := r.nodeInformer.Lister().Get(staleNode.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					glog.Warningf("reconciler: node %s no longer exists... will skip", staleNode.Name)
					break
				}
				glog.Errorf("reconciler: unexpected error fetching node %s: %v", staleNode.Name, err)
				continue
			}
			network, ok := node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel]
			if !ok {
				glog.Warningf("reconciler: node %s is missing label %s- will skip", node.Name, libcalicostub.NodeBgpIpv4NetworkLabel)
				break
			}
			if nodeCounts[network] == nil {
				nodeCounts[network] = &nodeCount{}
			}

			actionTaken, err := transitionSubnetReflectorState(node)
			if err != nil {
				glog.Errorf("reconciler: %v -will skip", err)
				break
			}
			if actionTaken == actionActivate || actionTaken == actionDeactivate {
				// Need to update API
				if _, err := r.clientset.CoreV1().Nodes().Update(node); err != nil {
					if apierrors.IsConflict(err) {
						glog.Warningf("reconciler: node %s cannot be updated, conflict error: %v", node.Name, err)
						continue
					}
					if apierrors.IsNotFound(err) {
						glog.Warningf("reconciler: node %s cannot be updated, cannot not be found: %v", node.Name, err)
						if actionTaken == actionActivate {
							continue
						}
						break
					}
					glog.Fatalf("reconciler: unexpected error updating node %s: %v", node.Name, err)
				}
				glog.Infof(nodeLogFmt, node.Name, k8sutil.NodeReady(node), network, actionTaken)
			}

			if actionTaken == actionActivate || actionTaken == actionKeep {
				// Is this node a reflector?
				reflectorCounts[network]++
			}
			actionLog[actionTaken]++
			if k8sutil.NodeReady(node) {
				nodeCounts[network].Ready++
			} else {
				nodeCounts[network].Unready++
			}
			break
		}
		if retriesLeft == 0 {
			glog.Fatalf("reconciler: exhausted retries updating node %s", staleNode.Name)
			return
		}
	}

	// sort network keys
	networks := []string{}
	for network := range nodeCounts {
		networks = append(networks, network)
	}
	sort.Strings(networks)

	glog.Info("Reconcilation loop done!")
	tbl := bytes.Buffer{}
	tbl.WriteString(fmt.Sprintf(syncLogFmt, actionLog[actionKeep], actionLog[actionActivate], actionLog[actionDeactivate], actionLog[actionNoop]))
	for _, network := range networks {
		tbl.WriteString(fmt.Sprintf(syncRowLogFmt, network, nodeCounts[network].Ready, nodeCounts[network].Unready, reflectorCounts[network]))
	}
	glog.Info(tbl.String())
}

func (r *Reconciler) getReflectorsPerSubnet() (int, error) {
	ures, err := k8sutil.GlobalBGPConfigResource(r.crdClient).
		Get(libcalicostub.ReflectorsPerSubnetName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}

	res := &libcalicostub.GlobalBGPConfig{}
	if err := unstructured.DefaultConverter.FromUnstructured(ures.Object, res); err != nil {
		glog.Fatalf("error converting from unstructured: %v", err)
	}
	cnt, err := strconv.Atoi(res.Spec.Value)
	if err != nil {
		return 0, fmt.Errorf("Non-integer value %s specified for %s/%s", res.Spec.Value, res.Kind, res.Name)
	}
	if cnt < 1 {
		return 0, fmt.Errorf("reflectorspersubnet is %d: must be >= 1", cnt)
	}

	return cnt, nil
}

func relevantStateChanged(oldNode, newNode *v1.Node) bool {
	// Check relevant labels
	for _, label := range []string{
		libcalicostub.NodeBgpReflectorLabel,
		libcalicostub.NodeBgpIpv4NetworkLabel,
	} {
		if oldNode.Labels[label] != newNode.Labels[label] {
			return true
		}
	}

	//check readiness
	if k8sutil.NodeReady(oldNode) != k8sutil.NodeReady(newNode) {
		return true
	}

	return false
}
