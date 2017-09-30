package main

import (
	"flag"
	"os"
	"time"

	"github.com/coreos/bgp-reflector-agent/pkg/k8sutil"
	"github.com/coreos/bgp-reflector-agent/pkg/nodereconciler"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
)

const (
	configMapName = "bgp-reflector-agent"
	componentName = "bgp-reflector-agent"
)

func init() {
	flag.Set("logtostderr", "true")
}

func main() {
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatalf("error creating in-cluster config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("error creating clientset: %v", err)
	}
	crdClient, err := k8sutil.BuildCRDClientV1(config)
	if err != nil {
		glog.Fatalf("error building crd dynamic client: %v", err)
	}

	nodeName := os.Getenv("NODENAME")
	if nodeName == "" {
		glog.Fatalf("NODENAME environment variable required")
	}
	podName := os.Getenv("PODNAME")
	if podName == "" {
		glog.Fatalf("PODNAME environment variable required")
	}
	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		glog.Fatalf("NAMESPACE environment variable required")
	}

	glog.Infof("initializing reflector agent: node = %s", nodeName)
	// hook this leader election convention up to the event stream
	broadcaster := record.NewBroadcaster()
	broadcaster.StartLogging(glog.Infof)
	broadcaster.StartRecordingToSink(&clientv1.EventSinkImpl{Interface: clientset.CoreV1().Events(namespace)})
	recorder := broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: componentName})

	// instantiate resource lock on configmap namespaceName/configMapName
	cmLock, err := resourcelock.New(
		resourcelock.ConfigMapsResourceLock,
		namespace,
		configMapName,
		clientset.CoreV1(),
		resourcelock.ResourceLockConfig{
			Identity:      podName,
			EventRecorder: recorder,
		},
	)
	if err != nil {
		glog.Fatalf("error creating resource lock: %v", err)
	}

	// set up leader election context
	lec := leaderelection.LeaderElectionConfig{
		Lock:          cmLock,
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 15 * time.Second,
		RetryPeriod:   5 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(stopCh <-chan struct{}) {
				glog.Info("leaderelect: now leading!")
				reconciler, err := nodereconciler.NewReconciler(clientset, crdClient)
				if err != nil {
					glog.Fatalf("error creating reconciler: %v", err)
				}
				reconciler.Run(stopCh)
				glog.Info("reconciler: run loop terminated! should no longer be leader")
			},
			OnStoppedLeading: func() {
				glog.Info("leaderelect: stopped leading!")
			},

			OnNewLeader: func(leader string) {
				glog.Infof("leaderelect: new leader (%s) detected!", leader)
			},
		},
	}

	glog.Infof("starting reflector agent: node = %s", nodeName)
	leaderelection.RunOrDie(lec)
}
