package k8sutil

import (
	"fmt"

	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
	"github.com/golang/glog"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1unstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion/unstructured"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type GlobalBGPConfigHandler func(*libcalicostub.GlobalBGPConfig) error

func WatchGlobalBGPConfigs(crdClient *dynamic.Client, handler GlobalBGPConfigHandler, stopCh <-chan struct{}) error {
	crdResource := GlobalBGPConfigResource(crdClient)
	if err := ensureReflectorsPerSubnetConfig(crdResource); err != nil {
		return err
	}

	castHandler := func(evt watch.Event) error {
		cfg, ok := evt.Object.(*libcalicostub.GlobalBGPConfig)
		if !ok {
			return fmt.Errorf("could not cast event object as libcalicostub.GlobalBGPConfig: %+v", evt)
		}
		return handler(cfg)
	}

	return WatchCRDResources(crdResource, castHandler, stopCh)
}

func GlobalBGPConfigResource(crdClient *dynamic.Client) dynamic.ResourceInterface {
	return crdClient.Resource(&metav1.APIResource{
		Name: libcalicostub.GlobalBGPConfigCRDName,
		Kind: libcalicostub.GlobalBGPConfigResourceName,
	}, v1.NamespaceAll)
}

func ensureReflectorsPerSubnetConfig(crdResource dynamic.ResourceInterface) error {
	// create default reflectorspersubnet globalbgpconfig if it does not exist
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

			uObj, err := unstructured.DefaultConverter.ToUnstructured(&defaultRPS)
			if err != nil {
				return fmt.Errorf("error converting to unstructured: %v", err)
			}
			if _, err := crdResource.Create(&metav1unstructured.Unstructured{Object: uObj}); err != nil {
				return fmt.Errorf("error creating globalbgpconfig: %v", err)
			}
		} else {
			return fmt.Errorf("error getting reflectorspersubnet: %v", err)
		}
	}

	return nil
}
