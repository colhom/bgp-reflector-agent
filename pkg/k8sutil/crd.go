package k8sutil

import (
	"fmt"

	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
)

type CRDHandler func(watch.Event) error

func WatchCRDResources(crdResource dynamic.ResourceInterface, handler CRDHandler, stopCh <-chan struct{}) error {
	for {
		glog.Info("WatchCRDResource: creating new watch")
		crdWatch, err := crdResource.Watch(metav1.ListOptions{})
		if err != nil {
			return fmt.Errorf("error creating crd watch: %v", err)
		}

		consumerExit := make(chan error)
		consumeEvents := func() error {
			glog.Info("WatchCRDResource/crdEventConsumer: reading event channel")
			for evt := range crdWatch.ResultChan() {
				glog.Infof("WatchCRDResource/crdEventConsumer: event type %s received", evt.Type)
				if evt.Type == watch.Error {
					return fmt.Errorf("globalbgpconfig crd watch detected error: %+v", evt)
				}
				if err = handler(evt); err != nil {
					return err
				}
			}
			glog.Info("WatchCRDResource/crdEventConsumer: crd watch event channel closed normally")
			return nil
		}

		go func() {
			consumerExit <- consumeEvents()
		}()

		select {
		case <-stopCh:
			glog.Info("WatchCRDResource: crd watch stop signal received, stopping event consumer...")
			crdWatch.Stop()
			glog.Info("WatchCRDResource: event consumer has stopped!")
			return <-consumerExit
		case err := <-consumerExit:
			if err != nil {
				glog.Warning("WatchCRDResource: watch encountered unexpected error, will terminate")
				return err
			}
			glog.Info("WatchCRDResource: crd watch exited unexpectedly, will restart")
		}
	}
	return fmt.Errorf("WatchCRDResource: broke out of loop")
}
