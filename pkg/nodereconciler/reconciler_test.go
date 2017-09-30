package nodereconciler

import (
	"flag"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"math/rand"

	"os"
	"path/filepath"
	"strconv"

	"github.com/coreos/bgp-reflector-agent/pkg/k8sutil"
	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	extfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	dfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	ctesting "k8s.io/client-go/testing"
)

const (
	defaultNodeCnt             = 100
	defaultReflectorsPerSubnet = 3
	defaultActionCnt           = 50
	defaultSubnetCnt           = 8

	defaultLogDir = "./reconciler-test-logs"

	// max time a single reconciler sync run is allowed to take
	maxReconcileDuration = 500 * time.Millisecond
)

var (
	nodeCnt, actionCnt, subnetCnt, reflectorsPerSubnet int

	gbcCrd = &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s.%s", libcalicostub.GlobalBGPConfigCRDName, libcalicostub.GroupVersion.Group),
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   libcalicostub.GroupVersion.Group,
			Version: libcalicostub.GroupVersion.Version,
			Scope:   apiextensionsv1beta1.ClusterScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: libcalicostub.GlobalBGPConfigCRDName,
				Kind:   reflect.TypeOf(libcalicostub.GlobalBGPConfig{}).Name(),
			},
		},
	}
)

func init() {
	logDir := defaultLogDir
	if os.Getenv("LOG_DIR") != "" {
		logDir = os.Getenv("LOG_DIR")
	}
	absLogDir, err := filepath.Abs(logDir)
	if err != nil {
		glog.Fatalf("error getting abs path of log dir %s: %v", logDir, err)
	}
	flag.Set("log_dir", absLogDir)
	if err := os.MkdirAll(absLogDir, 0755); err != nil {
		glog.Fatalf("error creating log output directory %s: %v", absLogDir, err)
	}
	fmt.Fprintf(os.Stderr, "Using reconciler glog output directory %s:\n -> check there for detailed reconciler logs\n", absLogDir)

	if testing.Verbose() {
		flag.Set("alsologtostderr", "true")
		flag.Set("stderrthreshold", "ERROR")
	}
	flag.Parse()

	parsePositiveInt("NODE_COUNT", &nodeCnt, defaultNodeCnt)
	parsePositiveInt("ACTION_COUNT", &actionCnt, defaultActionCnt)
	parsePositiveInt("SUBNET_COUNT", &subnetCnt, defaultSubnetCnt)
	parsePositiveInt("REFLECTORS_PER_SUBNET", &reflectorsPerSubnet, defaultReflectorsPerSubnet)

	if nodeCnt < subnetCnt {
		glog.Fatalf("invalid arguments: nodeCount (%d) must be >= subnetCount (%d)",
			nodeCnt, subnetCnt)
	}
	if nodeCnt/subnetCnt <= reflectorsPerSubnet {
		glog.Fatalf("invalid arguments: reflectorsPerSubnet (%d) must be > (nodeCount/subnetCount) (%d)",
			reflectorsPerSubnet, nodeCnt/subnetCnt)
	}
}

type coreTestClientset struct {
	*fake.Clientset
}
type extTestClientset struct {
	*extfake.Clientset
}

//TODO: move this to test-utils package and export
type testClientset struct {
	coreTestClientset
	extTestClientset
}

func (c *testClientset) Discovery() discovery.DiscoveryInterface {
	return nil
}

func TestIntegrationFull(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping full integration test")
	}
	client := newClientset(t)
	r, reportChan := newReconciler(t, client)

	reconcilerStopCh := make(chan struct{})
	defer close(reconcilerStopCh)
	go r.Run(reconcilerStopCh)
	go runReflectorCheck(t, client, reportChan)

	wg := &sync.WaitGroup{}
	for nodeNumber := 0; nodeNumber < nodeCnt; nodeNumber++ {
		nodeName := fmt.Sprintf("test-node-%d", nodeNumber)
		nodeNetwork := fmt.Sprintf("10.11.%d.0-24", nodeNumber%subnetCnt)
		wg.Add(1)
		go simulateNodeEvents(t, client, nodeName, nodeNetwork, actionCnt, wg)
	}
	t.Logf("waiting for node event simulation runs to finish...")
	wg.Wait()
	t.Logf("wait group exited, cooling down")
	time.Sleep(3 * time.Second)
}

func TestScaleUpDown(t *testing.T) {
	client := newClientset(t)
	r, reportChan := newReconciler(t, client)

	reconcilerStopCh := make(chan struct{})
	defer close(reconcilerStopCh)
	go r.Run(reconcilerStopCh)
	go runReflectorCheck(t, client, reportChan)

	wg := &sync.WaitGroup{}
	for nodeNumber := 0; nodeNumber < nodeCnt; nodeNumber++ {
		nodeName := fmt.Sprintf("test-node-%d", nodeNumber)
		nodeNetwork := fmt.Sprintf("10.11.%d.0-24", nodeNumber%subnetCnt)
		wg.Add(1)
		go simulateNodeLifecycle(t, client, nodeName, nodeNetwork, wg)
	}
	t.Logf("waiting for node lifecycle simulation runs to finish...")
	wg.Wait()
	t.Logf("wait group exited, cooling down")
	time.Sleep(3 * time.Second)
}

func newClientset(t *testing.T) *testClientset {
	ot := ctesting.NewObjectTracker(scheme.Scheme, scheme.Codecs.UniversalDecoder())
	fakePtr := ctesting.Fake{}
	fakeWatch := watch.NewRaceFreeFake()
	fakePtr.AddReactor("*", "*", ctesting.ObjectReaction(ot))
	fakePtr.AddWatchReactor("*", func(action ctesting.Action) (bool, watch.Interface, error) {
		return true, fakeWatch, nil
	})
	fakePtr.PrependReactor("*", "*", func(action ctesting.Action) (bool, runtime.Object, error) {
		switch action := action.(type) {
		case ctesting.CreateActionImpl:
			fakeWatch.Action(watch.Added, action.GetObject().DeepCopyObject())
		case ctesting.UpdateActionImpl:
			fakeWatch.Action(watch.Modified, action.GetObject().DeepCopyObject())
		case ctesting.DeleteActionImpl:
			obj, err := ot.Get(action.GetResource(), action.GetNamespace(), action.GetName())
			if err != nil {
				return false, nil, fmt.Errorf("error getting resource: %v", err)
			}
			fakeWatch.Action(watch.Deleted, obj.DeepCopyObject())
		}
		return false, nil, nil
	})

	return &testClientset{
		coreTestClientset{&fake.Clientset{Fake: fakePtr}},
		extTestClientset{&extfake.Clientset{Fake: fakePtr}},
	}
}

func newTestNode(name string) v1.Node {
	node := v1.Node{}
	node.Name = name
	node.CreationTimestamp = metav1.Now()
	node.Labels = make(map[string]string)
	node.Annotations = make(map[string]string)
	node.Status.Conditions = []v1.NodeCondition{}
	return node
}

func parsePositiveInt(envVar string, val *int, defaultVal int) {
	envVal := os.Getenv(envVar)
	if envVal == "" {
		*val = defaultVal
		return
	}
	var err error
	*val, err = strconv.Atoi(envVal)
	if err != nil {
		glog.Fatalf("error parsing %s=%s as integer: %v", envVar, envVal, err)
	}

	if *val <= 0 {
		glog.Fatalf("%s=%s invalid: must be > 0", envVar, envVal)
	}
}

func newReconciler(t *testing.T, client *testClientset) (*Reconciler, <-chan reconcileReport) {
	crdClient := &dfake.FakeClient{libcalicostub.GroupVersion, &client.coreTestClientset.Fake}
	if _, err := client.ApiextensionsV1beta1().CustomResourceDefinitions().Create(gbcCrd); err != nil {
		t.Fatalf("error creating crd: %v", err)
	}
	if err := k8sutil.CreateReflectorsPerSubnetConfig(k8sutil.GlobalBGPConfigResource(crdClient), reflectorsPerSubnet); err != nil {
		t.Fatalf("could not create default globalbgpconfig: %v", err)
	}
	r, err := NewReconciler(client, crdClient)
	if err != nil {
		t.Fatalf("could not create reconciler: %v", err)
	}
	reportChan := make(chan reconcileReport)
	r.reportChan = reportChan
	return r, reportChan
}

func listNodes(t *testing.T, client kubernetes.Interface) []v1.Node {
	nl, err := client.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		t.Fatalf("error creating node: %v", err)
	}
	return nl.Items

}

func getNode(t *testing.T, client kubernetes.Interface, name string) v1.Node {
	node, err := client.CoreV1().Nodes().Get(name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error creating node: %v", err)
	}
	return *k8sutil.MustCastNode(node.DeepCopyObject())
}

var writeLock = &sync.RWMutex{}

func addNode(t *testing.T, client kubernetes.Interface, node v1.Node) {
	writeLock.RLock()
	defer writeLock.RUnlock()
	if _, err := client.CoreV1().Nodes().Create(&node); err != nil {
		t.Fatalf("error creating node: %v", err)
	}
}

func updateNode(t *testing.T, client kubernetes.Interface, node v1.Node) {
	writeLock.RLock()
	defer writeLock.RUnlock()
	if _, err := client.CoreV1().Nodes().Update(&node); err != nil {
		t.Fatalf("error updating node: %v", err)
	}
}

func deleteNode(t *testing.T, client kubernetes.Interface, name string) {
	writeLock.RLock()
	defer writeLock.RUnlock()
	if err := client.CoreV1().Nodes().Delete(name, nil); err != nil {
		t.Fatalf("error deleting node: %v", err)
	}
}

func randSleep(base, vari int, unit time.Duration) {
	sleepDur := time.Duration(base+rand.Intn(vari)) * unit
	time.Sleep(sleepDur)
}

func nodeSetReady(node *v1.Node, ready bool) {
	condition := v1.ConditionFalse
	if ready {
		condition = v1.ConditionTrue
	}
	for i, nc := range node.Status.Conditions {
		if nc.Type == v1.NodeReady {
			node.Status.Conditions[i].Status = condition
			return
		}
	}
	node.Status.Conditions = append(node.Status.Conditions, v1.NodeCondition{
		Type:   v1.NodeReady,
		Status: condition,
	})
}

func checkSubnetReflectors(nodes []v1.Node, reflectorsPerSubnet int) (errors []error) {
	nodeCounts := make(nodeReport)
	errors = []error{}
	for _, node := range nodes {
		network, err := k8sutil.NodeNetworkValue(&node)
		if err != nil {
			continue
		}
		if nodeCounts[network] == nil {
			nodeCounts[network] = &nodeCount{}
		}

		reflector, _ := k8sutil.NodeReflectorValue(&node)
		if reflector == "subnet" {
			nodeCounts[network].reflector++
		}

		if k8sutil.NodeReady(&node) {
			nodeCounts[network].ready++
		} else {
			nodeCounts[network].unready++
			if reflector != "" {
				errors = append(errors, fmt.Errorf("reflector check failed: node %s status is not ready but node is enabled as reflector", node.Name))
			}
		}
	}

	for network, count := range nodeCounts {
		var expectedReflectorCnt int
		if count.ready >= reflectorsPerSubnet {
			expectedReflectorCnt = reflectorsPerSubnet
		} else {
			expectedReflectorCnt = count.ready
		}
		if count.reflector != expectedReflectorCnt {
			errors = append(errors, fmt.Errorf("network %s: expected reflector count mismatch, expected=%d , observed=%d", network, expectedReflectorCnt, count.reflector))
		}
	}

	return
}

// Run through node lifecycle exactly once, then exit
func simulateNodeLifecycle(t *testing.T, client kubernetes.Interface, nodeName, nodeNetwork string, wg *sync.WaitGroup) {
	defer wg.Done()
	node := newTestNode(nodeName)

	randSleep(2, 3, time.Second)
	addNode(t, client, node)

	randSleep(2, 3, time.Second)
	node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel] = nodeNetwork
	updateNode(t, client, node)

	randSleep(2, 3, time.Second)
	nodeSetReady(&node, true)
	updateNode(t, client, node)

	// Node is healthy, wait here for awhile
	time.Sleep(15 * time.Second)

	if rand.Intn(2) > 0 {
		// probabalistically set node to NotReady before deletion
		nodeSetReady(&node, false)
		updateNode(t, client, node)
		randSleep(2, 3, time.Second)
	}

	deleteNode(t, client, node.Name)
}

// Randomly transition node state <actionCnt> times, then exit
func simulateNodeEvents(t *testing.T, client kubernetes.Interface, nodeName, nodeNetwork string, actionCnt int, wg *sync.WaitGroup) {
	defer wg.Done()
	exists := false
	createNode := func() {
		node := newTestNode(nodeName)
		nodeSetReady(&node, false)
		node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel] = nodeNetwork
		addNode(t, client, node)
		exists = true
	}

	actions := []func(){
		func() {
			node := getNode(t, client, nodeName)
			nodeSetReady(&node, true)
			updateNode(t, client, node)
		},
		func() {
			node := getNode(t, client, nodeName)
			nodeSetReady(&node, false)
			updateNode(t, client, node)
		},
		func() {
			deleteNode(t, client, nodeName)
			exists = false
		},
	}
	for k := 1; k <= actionCnt; k++ {
		randSleep(1000, 2000, time.Millisecond)
		if !exists {
			createNode()
		} else {
			actions[rand.Intn(len(actions))]()
		}
	}
}

func runReflectorCheck(t *testing.T, client kubernetes.Interface, reportChan <-chan reconcileReport) {
	t.Logf("reflector check has started")
	defer t.Logf("reflector check has exited!")

	// How long we'll tolerate no reports happening
	zombieTimer := time.NewTimer(maxResyncPeriod)
	for {
		zombieTimer.Reset(maxResyncPeriod)
		reconcileNeeded := true

		select {
		case <-zombieTimer.C:
			t.Fatalf("reflector check: zombie timer expired, no reconcile events for last %s", maxResyncPeriod.String())
		case report := <-reportChan:
			if report.reflectorsPerSubnet == 0 {
				t.Logf("reflector check: report channel closed, will exit")
				return
			} else if report.end.IsZero() && report.start.IsZero() {
				// this means a reconcile pass is requesting to start
				// stop all test generated write events until it's done and we've grabbed the node list
				writeLock.Lock()

				nodes := listNodes(t, client)
				if len(checkSubnetReflectors(nodes, report.reflectorsPerSubnet)) == 0 {
					// We can expect reconciler to not change anything on this run
					reconcileNeeded = false
				}

			} else if !report.start.IsZero() && !report.end.IsZero() {
				// As this is testing code, we can trust the reconciler to always end with this case
				// before the channel is closed and loop exits

				// Grab view of nodes to audit
				nodes := listNodes(t, client)
				writeLock.Unlock()

				runDur := report.end.Sub(report.start)
				if runDur > maxReconcileDuration {
					t.Logf("reconcile report: %v", report.summary())
					t.Errorf("reconcile took too long: %s > %s", runDur, maxReconcileDuration)
				}

				errs := checkSubnetReflectors(nodes, report.reflectorsPerSubnet)
				for _, err := range errs {
					t.Error(err)
				}

				if report.actionCnt[actionActivate] > 0 || report.actionCnt[actionDeactivate] > 0 {
					if !reconcileNeeded {
						t.Errorf("reconciler made changes when not needed: these tests indicate it did not need to activate/deactivate any reflectors on this run:\n%s", report.summary())
					}
				}
			} else {
				// Noop- this means reconcile pass has started
			}
		}
	}
}
