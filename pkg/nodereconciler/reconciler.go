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

	"k8s.io/apimachinery/pkg/conversion/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	// Maximum amount of time between reconiliation passes
	maxResyncPeriod = 3 * time.Minute

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
	clientset    *kubernetes.Clientset
	config       *rest.Config
	crdClient    *dynamic.Client
	nodeInformer informersv1.NodeInformer
	triggerQueue chan *time.Time
}

func NewReconciler(
	clientset *kubernetes.Clientset,
	config *rest.Config,
) *Reconciler {

	informerFactory := informers.NewSharedInformerFactory(clientset, maxResyncPeriod)

	nodeInformer := informerFactory.Core().V1().Nodes()

	crdClient, err := k8sutil.BuildCRDClientV1(*config)
	if err != nil {
		glog.Fatalf("error building crd dynamic client: %v", err)
	}

	reconciler := &Reconciler{
		clientset:    clientset,
		config:       config,
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

func (r *Reconciler) syncSubnetReflectors() {
	reflectorsPerSubnet, err := r.getReflectorsPerSubnet()
	if err != nil {
		glog.Fatalf("reconciler: error getting reflectors per subnet: %v", err)
	}
	glog.Infof("starting reconciliation pass: configuring %d reflectors per subnet", reflectorsPerSubnet)

	reflectorCounts := map[string]int{}
	nodeCounts := map[string]int{}

	reflectorNodes, err := r.nodeInformer.Lister().List(reflectorSelector)
	if err != nil {
		glog.Fatalf("error listing reflector nodes: %v", err)
	}

	nonReflectorNodes, err := r.nodeInformer.Lister().List(nonReflectorSelector)
	if err != nil {
		glog.Fatalf("error listing non-reflector nodes: %v", err)
	}
	glog.Infof("found %d reflector nodes and %d non-reflector nodes", len(reflectorNodes), len(nonReflectorNodes))

	actionLog := map[string]int{
		"noop":        0,
		"kept":        0,
		"activated":   0,
		"deactivated": 0,
	}

	//It's important to always process existing reflector nodes first before any non-reflector nodes
	//so we have the opportunity to acknowledge their existence before appointing new ones to meet quota
	for _, staleNode := range append(reflectorNodes, nonReflectorNodes...) {
		var retriesLeft int
		var actionTaken string
		for retriesLeft = updateMaxRetry; retriesLeft > 0; retriesLeft-- {
			node, err := r.clientset.CoreV1().Nodes().Get(staleNode.Name, metav1.GetOptions{})
			if err != nil {
				glog.Errorf("reconciler: error fetching node %s: %v", staleNode.Name, err)
				continue
			}

			network, ok := node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel]
			if !ok {
				glog.Warningf("node %s is missing %s label! Will skip this node....", node.Name, libcalicostub.NodeBgpIpv4NetworkLabel)
				break
			}

			reflector := node.Labels[libcalicostub.NodeBgpReflectorLabel]
			if _, ok := reflectorCounts[network]; !ok {
				reflectorCounts[network] = 0
			}
			if k8sutil.NodeReady(node) {
				if reflectorCounts[network] < reflectorsPerSubnet {
					// We haven't seen enough healthy reflectors for this subnet yet...
					if reflector == "subnet" {
						actionTaken = "kept"
					} else {
						node.Labels[libcalicostub.NodeBgpReflectorLabel] = "subnet"
						actionTaken = "activated"
					}
				} else {
					// We have already seen desired number of reflectors for this subnet
					if reflector == "subnet" {
						delete(node.Labels, libcalicostub.NodeBgpReflectorLabel)
						actionTaken = "deactivated"
					} else {
						actionTaken = "noop"
					}
				}
			} else {
				// Node not healthy --> make sure we're not already relying on it to be a reflector
				if reflector == "subnet" {
					delete(node.Labels, libcalicostub.NodeBgpReflectorLabel)
					actionTaken = "deactivated"
				} else {
					actionTaken = "noop"
				}
			}

			if _, err := r.clientset.CoreV1().Nodes().Update(node); err == nil {
				switch actionTaken {
				case "activated", "kept":
					reflectorCounts[network]++
				case "noop", "deactivated":
					//Lack of increment expresses this
				default:
					glog.Fatalf("reconciler: invalid action '%s' taken! this should never happen", actionTaken)
				}
				actionLog[actionTaken]++
				nodeCounts[network]++

				glog.Infof(`node %s:
  ready:  %t
  subnet: %s
  action: %s
`, node.Name, k8sutil.NodeReady(node), network, actionTaken)

				break
			} else {
				if !apierrors.IsConflict(err) {
					glog.Fatalf("reconciler: unexpected error updated node %s: %v", node.Name, err)
					return
				}
				glog.Warningf("reconciler: conflict error updating node %s: %d retries remaining", node.Name, retriesLeft)
				time.Sleep(200 * time.Millisecond)
			}
		}
		if retriesLeft == 0 {
			glog.Fatalf("reconciler: exhausted retries updating node %s", staleNode.Name)
			return
		}
	}

	glog.Info("Reconcilation loop done!")
	tbl := bytes.Buffer{}
	tbl.WriteString(fmt.Sprintf(`
Summary:
 * kept %d reflector nodes as-is
 * activated %d reflector nodes
 * deactivated %d reflector nodes
 * left %d non-reflector nodes as-is
 -----------------------------------------
  Node Subnet CIDR   | Nodes | Reflectors
 -----------------------------------------
`, actionLog["kept"], actionLog["activated"], actionLog["deactivated"], actionLog["noop"]))

	for network := range nodeCounts {
		tbl.WriteString(fmt.Sprintf(
			"%-20s | %-5d | %-5d\n",
			network, nodeCounts[network], reflectorCounts[network]))
	}
	tbl.WriteString(`
 ----------------------------------------
`)
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
