package k8sutil

import (
	"reflect"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"
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
