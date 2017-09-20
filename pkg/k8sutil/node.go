package k8sutil

import (
	"reflect"

	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

func NodeReady(node *v1.Node) bool {
	for _, nc := range node.Status.Conditions {
		if nc.Type == v1.NodeReady && nc.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func MustCastNode(nodeI interface{}) *v1.Node {
	node, ok := nodeI.(*v1.Node)
	if !ok {
		//Fail fast- this should never happen
		glog.Fatalf("Could not cast %+v as %s", nodeI, reflect.TypeOf(node))
		return &v1.Node{}
	}
	return node
}

func BuildCRDClientV1(cfg rest.Config) (*dynamic.Client, error) {

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
