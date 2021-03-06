// +build !ignore_autogenerated

/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// This file was autogenerated by deepcopy-gen. Do not edit it manually!

package libcalicostub

import (
	reflect "reflect"

	conversion "k8s.io/apimachinery/pkg/conversion"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// GetGeneratedDeepCopyFuncs returns the generated funcs, since we aren't registering them.
//
// Deprecated: deepcopy registration will go away when static deepcopy is fully implemented.
func GetGeneratedDeepCopyFuncs() []conversion.GeneratedDeepCopyFunc {
	return []conversion.GeneratedDeepCopyFunc{
		{Fn: func(in interface{}, out interface{}, c *conversion.Cloner) error {
			in.(*GlobalBGPConfig).DeepCopyInto(out.(*GlobalBGPConfig))
			return nil
		}, InType: reflect.TypeOf(&GlobalBGPConfig{})},
		{Fn: func(in interface{}, out interface{}, c *conversion.Cloner) error {
			in.(*GlobalBGPConfigList).DeepCopyInto(out.(*GlobalBGPConfigList))
			return nil
		}, InType: reflect.TypeOf(&GlobalBGPConfigList{})},
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *GlobalBGPConfig) DeepCopyInto(out *GlobalBGPConfig) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new GlobalBGPConfig.
func (in *GlobalBGPConfig) DeepCopy() *GlobalBGPConfig {
	if in == nil {
		return nil
	}
	out := new(GlobalBGPConfig)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *GlobalBGPConfig) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	} else {
		return nil
	}
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *GlobalBGPConfigList) DeepCopyInto(out *GlobalBGPConfigList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]GlobalBGPConfig, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	return
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new GlobalBGPConfigList.
func (in *GlobalBGPConfigList) DeepCopy() *GlobalBGPConfigList {
	if in == nil {
		return nil
	}
	out := new(GlobalBGPConfigList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *GlobalBGPConfigList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	} else {
		return nil
	}
}
