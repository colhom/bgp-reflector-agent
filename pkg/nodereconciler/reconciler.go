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

	// should only be non-nil for testing
	// will get a reconcile report every time sync loop runs
	reportChan chan<- reconcileReport
}

func NewReconciler(
	clientset kubernetes.Interface,
	crdClient dynamic.Interface,
) (*Reconciler, error) {

	informerFactory := informers.NewSharedInformerFactory(clientset, maxResyncPeriod)

	nodeInformer := informerFactory.Core().V1().Nodes()

	// create default reflectorspersubnet globalbgpconfig if it does not exist
	crdResource := k8sutil.GlobalBGPConfigResource(crdClient)
	if _, err := crdResource.Get(libcalicostub.ReflectorsPerSubnetName, metav1.GetOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			glog.Warningf("reconciler: could not find reflectorspersubnet globalbgpconfig: %v", err)
			if err := k8sutil.CreateReflectorsPerSubnetConfig(crdResource, 2); err != nil {
				return nil, fmt.Errorf("reconciler: error creating globalconfig %v", err)
			}
		} else {
			return nil, fmt.Errorf("reconciler: error reading globalbgpconfig: %v", err)
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

	return reconciler, nil
}

func (r *Reconciler) Run(stopCh <-chan struct{}) {
	defer glog.Info("reconciler: Run() - all stop channels closed")

	if r.reportChan != nil {
		glog.Warningf("reconciler: report channel is non-nil, so all reconcile passes will block on send. this better be a test!")
		defer close(r.reportChan)
	}

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
	glog.Info("reconciler: reconciliation loop up")
	defer glog.Info("reconciler: reconciliation loop down")

	resyncTimer := time.NewTimer(maxResyncPeriod)
	for {
		resyncTimer.Reset(maxResyncPeriod)
		glog.Flush()
		select {
		case <-resyncTimer.C:
			glog.Info("reconcile: maxResyncPeriod %s: timer fired", maxResyncPeriod.String())
			r.triggerResync()
		case t := <-r.triggerQueue:
			if t == nil {
				glog.Info("reconcile: trigger queue closed cleanly")
				return
			}

			glog.Infof("reconcile: trigger queue received: %s", t.String())
			reflectorsPerSubnet, err := r.getReflectorsPerSubnet()
			if err != nil {
				glog.Fatalf("reconciler: error getting reflectors per subnet: %v", err)
			}

			if err := r.syncSubnetReflectors(reflectorsPerSubnet); err != nil {
				glog.Fatalf("unexpected error syncing subnet reflectors: %v", err)
			}
		}
	}
	glog.Fatalf("reconciler: reconciliation loop select loop broke out")
}

type reflectorAction string

const (
	actionNoop       reflectorAction = "noop"       // leave a non-reflector as-is
	actionKeep       reflectorAction = "keep"       // leave a reflector as- is
	actionActivate   reflectorAction = "activate"   // non-reflector --> reflector
	actionDeactivate reflectorAction = "deactivate" // reflector --> non-reflector

	actionSkip reflectorAction = "skip" // node state invalid, skip it
)

// reflector-action --> how many times
type actionReport map[reflectorAction]int

// ipv4-network --> how many ready/unready/reflector nodes
type nodeCount struct {
	ready, unready, reflector int
}
type nodeReport map[string]*nodeCount

type reconcileReport struct {
	actionCnt           actionReport
	nodeCnt             nodeReport
	reflectorsPerSubnet int
	start, end          time.Time
}

const (
	nodeLogFmt = "reconciler: node %s: ready= %-5t subnet= %-18s action= %-8s"

	summaryLogFmt = `
Reflector State Summary:

    ReflectorsPerSubnet: %d

    Start:    %s
    Duration: %s

    Reflector State Transition
 -------------------------------------
 Nodes | Start State   | End State
 -------------------------------------
  %-4d | reflector     | reflector
  %-4d | non-reflector | non-reflector
  %-4d | non-reflector | reflector
  %-4d | reflector     | non-reflector
  %-4d | INVALID       | INVALID

    Network Reflector State
 -------------------------------------------------------------------
  Node Subnet CIDR   | Ready Nodes | Unready Nodes | Reflector Nodes
 -------------------------------------------------------------------`
	networkLogFmt = `
   %            -20s |    %-4d     |     %-4d      | %-4d`
)

func (report *reconcileReport) summary() string {
	// sort network keys
	networks := []string{}
	for network := range report.nodeCnt {
		networks = append(networks, network)
	}
	sort.Strings(networks)

	tbl := bytes.Buffer{}
	tbl.WriteString(fmt.Sprintf(summaryLogFmt,
		report.reflectorsPerSubnet,
		report.start.String(), report.end.Sub(report.start).String(),
		report.actionCnt[actionKeep], report.actionCnt[actionNoop],
		report.actionCnt[actionActivate], report.actionCnt[actionDeactivate],
		report.actionCnt[actionSkip],
	))
	for _, network := range networks {
		tbl.WriteString(fmt.Sprintf(networkLogFmt, network, report.nodeCnt[network].ready,
			report.nodeCnt[network].unready, report.nodeCnt[network].reflector))
	}
	tbl.WriteString("\n")

	return tbl.String()
}

func (r *Reconciler) syncSubnetReflectors(reflectorsPerSubnet int) error {
	glog.Infof("starting reconciliation pass: configuring %d reflectors per subnet", reflectorsPerSubnet)

	state := reconcileReport{
		actionCnt:           make(actionReport),
		nodeCnt:             make(nodeReport),
		reflectorsPerSubnet: reflectorsPerSubnet,
	}

	if r.reportChan != nil {
		// signal reconcile pass would like to start
		r.reportChan <- state
		state.start = time.Now()
		// signal reconcile pass is now waiting to start
		r.reportChan <- state
	}
	defer func() {
		state.end = time.Now()
		if r.reportChan != nil {
			// signal reconcile pass is done
			r.reportChan <- state
		}
		glog.Info(state.summary())
	}()

	reflectorNodes, err := r.nodeInformer.Lister().List(reflectorSelector)
	if err != nil {
		return fmt.Errorf("error listing reflector nodes: %v", err)
	}

	nonReflectorNodes, err := r.nodeInformer.Lister().List(nonReflectorSelector)
	if err != nil {
		return fmt.Errorf("error listing non-reflector nodes: %v", err)
	}
	glog.Infof("found %d reflector nodes and %d non-reflector nodes", len(reflectorNodes), len(nonReflectorNodes))

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
			actionTaken, err := transitionSubnetReflectorState(node, &state)
			if err != nil {
				glog.Fatalf("reconciler: node %s: unexpected error transitioning subnet reflector state: %v", node.Name, err)
			}
			network := node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel]

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
					return fmt.Errorf("reconciler: unexpected error updating node %s: %v", node.Name, err)
				}
				glog.Infof(nodeLogFmt, node.Name, k8sutil.NodeReady(node), network, actionTaken)
			}

			if actionTaken == actionActivate || actionTaken == actionKeep {
				// Is this node a reflector?
				state.nodeCnt[network].reflector++
			}
			state.actionCnt[actionTaken]++
			if actionTaken != actionSkip {
				if k8sutil.NodeReady(node) {
					state.nodeCnt[network].ready++
				} else {
					state.nodeCnt[network].unready++
				}
			}
			break
		}
		if retriesLeft == 0 {
			return fmt.Errorf("reconciler: exhausted retries updating node %s", staleNode.Name)
		}
	}

	return nil
}

func (r *Reconciler) getReflectorsPerSubnet() (int, error) {
	ures, err := k8sutil.GlobalBGPConfigResource(r.crdClient).
		Get(libcalicostub.ReflectorsPerSubnetName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}

	res := &libcalicostub.GlobalBGPConfig{}
	if err := unstructured.DefaultConverter.FromUnstructured(ures.Object, res); err != nil {
		return 0, fmt.Errorf("error converting from unstructured: %v", err)
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

//returns (actionTaken,err)
func transitionSubnetReflectorState(node *v1.Node, state *reconcileReport) (reflectorAction, error) {
	if !state.end.IsZero() {
		glog.Fatalf("reconciler: transitionSubnetReflector- report state has already ended. %v", state)
	}

	network, err := k8sutil.NodeNetworkValue(node)
	if err != nil {
		glog.Warningf("reconciler: node %s: error parsing network label: %v", node.Name, err)
		return actionSkip, nil
	}
	if _, ok := state.nodeCnt[network]; !ok {
		state.nodeCnt[network] = &nodeCount{}
	}

	reflector, _ := k8sutil.NodeReflectorValue(node)

	if k8sutil.NodeReady(node) && state.nodeCnt[network].reflector < state.reflectorsPerSubnet {
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

	glog.Errorf("reconciler: node %s: unsupported reflector label value for %s= %s",
		node.Name, libcalicostub.NodeBgpReflectorLabel, reflector)
	return actionSkip, nil
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
