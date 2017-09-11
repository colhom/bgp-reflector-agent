package nodereconciler

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"reflect"
	"strconv"

	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"

	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
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

	crdClient, err := buildCRDClientV1(*config)
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
				if relevantStateChanged(castNode(oldNode), castNode(newNode)) {
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
	defer close(r.triggerQueue)
	go r.reconciliationLoop()

	var stopWatch func(bool)
	var startWatch func() bool
	var doWatch func()

	{
		globalbcLock := &sync.Mutex{}
		var globalbcWatch watch.Interface
		watchTerminated := false

		stopWatch = func(terminate bool) {
			globalbcLock.Lock()
			defer globalbcLock.Unlock()
			glog.Info("reconciler: stop rps watch")

			watchTerminated = terminate
			if globalbcWatch != nil {
				globalbcWatch.Stop()
				globalbcWatch = nil
			}
		}

		startWatch = func() bool {
			globalbcLock.Lock()
			defer globalbcLock.Unlock()
			if watchTerminated {
				return false
			}
			glog.Info("reconciler: start rps watch")
			crdResource := r.crdClient.Resource(&metav1.APIResource{
				Name: libcalicostub.GlobalBGPConfigCRDName,
				Kind: libcalicostub.GlobalBGPConfigResourceName,
			}, v1.NamespaceAll)

			if _, err := crdResource.Get(libcalicostub.ReflectorsPerSubnetName, metav1.GetOptions{}); err != nil {
				if apierrors.IsNotFound(err) {
					glog.Warningf("could not find reflectorspersubnet globalbgpconfig: %v", err)
					glog.Warning("creating default reflectorspersubnet globalbgpconfig")

					defaultRPS := libcalicostub.GlobalBGPConfig{
						Spec: libcalicostub.GlobalBGPConfigSpec{
							Name:  libcalicostub.ReflectorsPerSubnetProperName,
							Value: "2",
						},
					}
					defaultRPS.Name = libcalicostub.ReflectorsPerSubnetName
					defaultRPS.Kind = libcalicostub.GlobalBGPConfigResourceName
					defaultRPS.APIVersion = libcalicostub.GroupVersion.String()

					jsonBytes, err := json.Marshal(&defaultRPS)
					if err != nil {
						glog.Fatalf("error marshaling json: %v", err)
					}
					uObj := &unstructured.Unstructured{}
					if err := uObj.UnmarshalJSON(jsonBytes); err != nil {
						glog.Fatalf("error unmarshaling json: %v", err)
					}
					if _, err := crdResource.Create(uObj); err != nil {
						glog.Fatalf("error creating globalbgpconfig: %v", err)
					}
				} else {
					glog.Fatalf("error getting reflectorspersubnet: %v", err)
				}
			}

			//Watch
			var err error
			globalbcWatch, err = crdResource.Watch(metav1.ListOptions{})
			if err != nil {
				glog.Fatalf("error creating globalbgpconfig watch: %v", err)
			}

			return true
		}

		doWatch = func() {
			for startWatch() {
				evtChan := globalbcWatch.ResultChan()
				for {
					evt := <-evtChan
					if evt.Type != "" {
						if evt.Type == watch.Error {
							glog.Errorf("globalbgpconfig watch detected error: %+v", evt)
							break
						}
						cfg, ok := evt.Object.(*libcalicostub.GlobalBGPConfig)
						if !ok {
							glog.Fatalf("could not cast event object as libcalicostub.GlobalBGPConfig: %+v", evt)
						}
						glog.Infof("reconciler: watch event received for %s/%s", cfg.Kind, cfg.Name)
						switch cfg.Name {
						case libcalicostub.ReflectorsPerSubnetName:
							glog.Info("reconciler: watch event triggered re-sync")
							r.triggerResync()
						default:
							glog.Info("reconciler: watch event ignored")
						}
					} else {
						glog.Info("reconciler: globalbgpconfig watch done")
						break
					}
				}
				stopWatch(false)
			}
			glog.Info("reconciler: globalbgpconfig watch terminated")
		}
	}

	defer stopWatch(true)
	go doWatch()

	glog.Info("reconciler: starting informer loop")
	r.nodeInformer.Informer().Run(stopCh)
	glog.Info("reconciler: informer loop done")
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
		"existing":    0,
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
				continue
			}

			reflector := node.Labels[libcalicostub.NodeBgpReflectorLabel]
			if _, ok := reflectorCounts[network]; !ok {
				reflectorCounts[network] = 0
			}

			glog.Infof("[%s] examining node %s", network, node.Name)

			if nodeReady(node) {

				// Node is healthy
				glog.Infof("  * %s is ready!", node.Name)
				if reflectorCounts[network] < reflectorsPerSubnet {
					// We haven't seen enough healthy reflectors for this subnet yet...
					if reflector == "subnet" {
						glog.Infof(" -> [%s] recognized existing reflector %s", node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel], node.Name)
						actionTaken = "existing"
					} else {
						glog.Infof(" -> [%s] activating new reflector %s", network, node.Name)
						node.Labels[libcalicostub.NodeBgpReflectorLabel] = "subnet"
						actionTaken = "activated"
					}
				} else {
					// We have already seen desired number of reflectors for this subnet
					if reflector == "subnet" {
						glog.Infof(" -> [%s] deactivating superfluous reflector %s", network, node.Name)
						delete(node.Labels, libcalicostub.NodeBgpReflectorLabel)
						actionTaken = "deactivated"
					} else {
						glog.Infof(" -> [%s] no action taken for healthy node %s", network, node.Name)
						actionTaken = "noop"
					}
				}

			} else {

				// Node not healthy --> make sure we're not already relying on it to be a reflector
				glog.Infof("  * %s is not ready!", node.Name)
				if reflector == "subnet" {
					glog.Infof(" -> [%s] deactivating unhealthy reflector %s", network, node.Name)
					delete(node.Labels, libcalicostub.NodeBgpReflectorLabel)
					actionTaken = "deactivated"
				} else {
					glog.Infof(" -> [%s] no action taken for unhealthy node %s", network, node.Name)
					actionTaken = "noop"
				}
			}

			if _, err := r.clientset.CoreV1().Nodes().Update(node); err == nil {
				switch actionTaken {
				case "activated", "existing":
					reflectorCounts[network]++
				case "noop", "deactivated":
					//Lack of increment expresses this
				default:
					glog.Fatalf("reconciler: invalid action '%s' taken! this should never happen", actionTaken)
				}
				actionLog[actionTaken]++
				nodeCounts[network]++

				glog.Infof("[%s] node %s updated", network, node.Name)
				break
			} else {
				if !apierrors.IsConflict(err) {
					glog.Fatalf("reconciler: unexpected error updated node %s: %v", node.Name, err)
					return
				}
				glog.Warningf("reconciler: conflict error updating node %s: %d retries remaining", node.Name, retriesLeft)
			}
			time.Sleep(500 * time.Millisecond)
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
 * kept %d existing reflector nodes
 * added %d new reflector nodes
 * removed %d existing reflector nodes
 * observed %d non-reflecting nodes
 -----------------------------------------
  Node Subnet CIDR   | Nodes | Reflectors
 -----------------------------------------
`, actionLog["existing"], actionLog["activated"], actionLog["deactivated"], actionLog["noop"]))

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
	ures, err := r.crdClient.Resource(&metav1.APIResource{
		Name: libcalicostub.GlobalBGPConfigCRDName,
		Kind: libcalicostub.GlobalBGPConfigResourceName,
	}, v1.NamespaceAll).Get(libcalicostub.ReflectorsPerSubnetName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}

	jsonBytes, err := ures.MarshalJSON()
	if err != nil {
		return 0, err
	}

	res := libcalicostub.GlobalBGPConfig{}
	if err := json.Unmarshal(jsonBytes, &res); err != nil {
		return 0, err
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

func nodeReady(node *v1.Node) bool {
	for _, nc := range node.Status.Conditions {
		if nc.Type == v1.NodeReady && nc.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func castNode(nodeI interface{}) *v1.Node {
	node, ok := nodeI.(*v1.Node)
	if !ok {
		//Fail fast- this should never happen
		glog.Fatalf("Could not cast %+v as %s", nodeI, reflect.TypeOf(node))
		return &v1.Node{}
	}
	return node
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
	if nodeReady(oldNode) != nodeReady(newNode) {
		return true
	}

	return false
}

func buildCRDClientV1(cfg rest.Config) (*dynamic.Client, error) {

	// Generate config using the base config.
	cfg.GroupVersion = &libcalicostub.GroupVersion

	schemeBuilder := runtime.NewSchemeBuilder(
		func(scheme *runtime.Scheme) error {
			scheme.AddKnownTypes(
				*cfg.GroupVersion,
				&libcalicostub.GlobalBGPConfig{},
				&libcalicostub.GlobalBGPConfigList{},
			)
			metav1.AddToGroupVersion(scheme, *cfg.GroupVersion)
			return nil
		})
	schemeBuilder.AddToScheme(scheme.Scheme)
	cfg.APIPath = "/apis"
	cfg.NegotiatedSerializer = serializer.DirectCodecFactory{CodecFactory: scheme.Codecs}

	cli, err := dynamic.NewClient(&cfg)
	if err != nil {
		return nil, err
	}
	return cli, nil
}
