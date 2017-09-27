package k8sutil

import (
	"fmt"

	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type GlobalBGPConfigHandler func(*libcalicostub.GlobalBGPConfig) error

func WatchGlobalBGPConfigs(crdClient dynamic.Interface, handler GlobalBGPConfigHandler, stopCh <-chan struct{}) error {
	crdResource := GlobalBGPConfigResource(crdClient)
	castHandler := func(evt watch.Event) error {
		cfg, ok := evt.Object.(*libcalicostub.GlobalBGPConfig)
		if !ok {
			return fmt.Errorf("could not cast event object as libcalicostub.GlobalBGPConfig: %+v", evt)
		}
		return handler(cfg)
	}

	return WatchCRDResources(crdResource, castHandler, stopCh)
}

func GlobalBGPConfigResource(crdClient dynamic.Interface) dynamic.ResourceInterface {
	return crdClient.Resource(&metav1.APIResource{
		Name: libcalicostub.GlobalBGPConfigCRDName,
		Kind: libcalicostub.GlobalBGPConfigResourceName,
	}, v1.NamespaceAll)
}

func CreateReflectorsPerSubnetConfig(crdResource dynamic.ResourceInterface, reflectorsPerSubnet int) error {
	if reflectorsPerSubnet < 1 {
		return fmt.Errorf("reflectorsPerSubnet must be >= 1")
	}

	glog.Infof("creating reflectorspersubnet=%d globalbgpconfig", reflectorsPerSubnet)
	defaultRPS := libcalicostub.GlobalBGPConfig{
		Spec: libcalicostub.GlobalBGPConfigSpec{
			Name:  libcalicostub.ReflectorsPerSubnetProperName,
			Value: fmt.Sprintf("%d", reflectorsPerSubnet),
		},
	}
	defaultRPS.Name = libcalicostub.ReflectorsPerSubnetName
	defaultRPS.Kind = libcalicostub.GlobalBGPConfigResourceName
	defaultRPS.APIVersion = libcalicostub.GroupVersion.String()

	uObj, err := unstructured.DefaultConverter.ToUnstructured(&defaultRPS)
	if err != nil {
		return fmt.Errorf("error converting globalbgpconfig to unstructured: %v", err)
	}
	if _, err := crdResource.Create(&metav1unstructured.Unstructured{Object: uObj}); err != nil {
		return fmt.Errorf("error creating globalbgpconfig: %v", err)
	}

	return nil
}
