package k8sutil

import (
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/golang/glog"
	"k8s.io/api/core/v1"

	"github.com/coreos/bgp-reflector-agent/pkg/libcalicostub"
)

func NodeReady(node *v1.Node) bool {
	for _, nc := range node.Status.Conditions {
		if nc.Type == v1.NodeReady && nc.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

func NodeReflectorValue(node *v1.Node) (string, bool) {
	v, ok := node.Labels[libcalicostub.NodeBgpReflectorLabel]
	return v, ok
}

func NodeNetworkValue(node *v1.Node) (string, error) {
	v, ok := node.Labels[libcalicostub.NodeBgpIpv4NetworkLabel]
	if !ok {
		return "", fmt.Errorf("node %s: %s label is not defined", node.Name, libcalicostub.NodeBgpIpv4NetworkLabel)
	}
	realV := strings.Replace(v, "-", "/", -1)
	_, _, err := net.ParseCIDR(realV)
	if err != nil {
		return "", fmt.Errorf("node %s: error parsing network cidr %s: %v", node.Name, realV, err)
	}
	return v, nil
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
