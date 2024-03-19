/*
Copyright 2023 The KubeVela Authors.

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

package multicluster

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion/queryparams"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var _ client.Client = &remoteClusterClient{}

// DefaultDisableRemoteClusterClient whether to disable RemoteClusterClient for default multicluster client
var DefaultDisableRemoteClusterClient = false

// RemoteClusterClientCacheTimeout the timeout of remote cluster's RESTMapper cache
var RemoteClusterClientCacheTimeout = 30 * time.Minute

// RemoteClusterClientCachePruneProbability the cache prune probability of remote clusters' RESTMapper
var RemoteClusterClientCachePruneProbability = 0.01

type remoteClusterClient struct {
	config     *rest.Config
	paramCodec runtime.ParameterCodec
	codecs     serializer.CodecFactory

	defaultClient client.Client

	restMappers *ttlcache.Cache[string, meta.RESTMapper]
	restClients *ttlcache.Cache[schema.GroupVersionKind, rest.Interface]
}

// NewRemoteClusterClient create a client that will use separate RESTMappers for
// remote cluster requests.
func NewRemoteClusterClient(cfg *rest.Config, options client.Options) (client.Client, error) {
	defaultClient, err := client.New(cfg, options)
	if err != nil {
		return nil, err
	}

	if options.Scheme == nil {
		options.Scheme = scheme.Scheme
	}

	restMappers := ttlcache.New[string, meta.RESTMapper](
		ttlcache.WithTTL[string, meta.RESTMapper](RemoteClusterClientCacheTimeout),
	)

	restClients := ttlcache.New[schema.GroupVersionKind, rest.Interface](
		ttlcache.WithTTL[schema.GroupVersionKind, rest.Interface](RemoteClusterClientCacheTimeout),
	)

	return &remoteClusterClient{
		config:        cfg,
		paramCodec:    NewNoConversionParamCodec(),
		codecs:        serializer.NewCodecFactory(options.Scheme),
		defaultClient: defaultClient,
		restMappers:   restMappers,
		restClients:   restClients,
	}, nil
}

// GetRESTMapper get RESTMapper for the target cluster. If not initialized,
// bootstrap it and place it in the cache.
func (in *remoteClusterClient) GetRESTMapper(cluster string) (meta.RESTMapper, error) {
	if rand.Float64() < RemoteClusterClientCachePruneProbability {
		in.restMappers.DeleteExpired()
	}
	item := in.restMappers.Get(cluster)
	if item == nil {
		copied := rest.CopyConfig(in.config)
		copied.Wrap(NewTransportWrapper(ForCluster(cluster)))
		mapper, err := apiutil.NewDynamicRESTMapper(copied)
		if err != nil {
			return nil, err
		}
		item = in.restMappers.Set(cluster, mapper, ttlcache.DefaultTTL)
	}
	return item.Value(), nil
}

// GetMapping get mapping for given gvk from target cluster. It will trigger
// RESTMapper initialization if not exist
func (in *remoteClusterClient) GetMapping(cluster string, gvk schema.GroupVersionKind) (*meta.RESTMapping, error) {
	mapper, err := in.GetRESTMapper(cluster)
	if err != nil {
		return nil, err
	}
	mappings, err := mapper.RESTMappings(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return nil, err
	}
	if len(mappings) == 0 {
		return nil, runtime.NewNotRegisteredErrForKind(cluster, gvk)
	}
	return mappings[0], nil
}

// GetRESTClient for given gvk
func (in *remoteClusterClient) GetRESTClient(gvk schema.GroupVersionKind) (rest.Interface, error) {
	if rand.Float64() < RemoteClusterClientCachePruneProbability {
		in.restClients.DeleteExpired()
	}
	item := in.restClients.Get(gvk)
	if item == nil {
		restClient, err := apiutil.RESTClientForGVK(gvk, true, in.config, in.codecs)
		if err != nil {
			return nil, err
		}
		item = in.restClients.Set(gvk, restClient, ttlcache.DefaultTTL)
	}
	return item.Value(), nil
}

func (in *remoteClusterClient) getObjMeta(cluster string, u *unstructured.Unstructured) (*objMeta, error) {
	gvk := u.GroupVersionKind()
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	mapping, err := in.GetMapping(cluster, gvk)
	if err != nil {
		return nil, err
	}
	restClient, err := in.GetRESTClient(gvk)
	if err != nil {
		return nil, err
	}
	return &objMeta{
		resourceMeta: &resourceMeta{Interface: restClient, mapping: mapping, gvk: gvk},
		Unstructured: u,
	}, nil
}

type remoteClusterStatusWriter struct {
	client.StatusWriter
}

// Status implements client.Client.
func (in *remoteClusterClient) Status() client.StatusWriter {
	return &remoteClusterStatusWriter{
		StatusWriter: in.defaultClient.Status(),
	}
}

// Scheme implements client.Client.
func (in *remoteClusterClient) Scheme() *runtime.Scheme {
	return in.defaultClient.Scheme()
}

// RESTMapper implements client.Client.
func (in *remoteClusterClient) RESTMapper() meta.RESTMapper {
	return in.defaultClient.RESTMapper()
}

// Create implements client.Client.
func (in *remoteClusterClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.Unstructured)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.Create(ctx, obj, opts...)
	}

	o, err := in.getObjMeta(cluster, u)
	if err != nil {
		return err
	}

	createOpts := &client.CreateOptions{}
	createOpts.ApplyOptions(opts)

	result := o.Post().
		NamespaceIfScoped(o.GetNamespace(), o.isNamespaced()).
		Resource(o.resource()).
		Body(obj).
		VersionedParams(createOpts.AsCreateOptions(), in.paramCodec).
		Do(ctx).
		Into(obj)

	u.SetGroupVersionKind(o.gvk)
	return result
}

// Update implements client.Client.
func (in *remoteClusterClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.Unstructured)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.Update(ctx, obj, opts...)
	}

	o, err := in.getObjMeta(cluster, u)
	if err != nil {
		return err
	}

	updateOpts := client.UpdateOptions{}
	updateOpts.ApplyOptions(opts)

	result := o.Put().
		NamespaceIfScoped(o.GetNamespace(), o.isNamespaced()).
		Resource(o.resource()).
		Name(o.GetName()).
		Body(obj).
		VersionedParams(updateOpts.AsUpdateOptions(), in.paramCodec).
		Do(ctx).
		Into(obj)

	u.SetGroupVersionKind(o.gvk)
	return result
}

// Delete implements client.Client.
func (in *remoteClusterClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.Unstructured)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.Delete(ctx, obj, opts...)
	}

	o, err := in.getObjMeta(cluster, u)
	if err != nil {
		return err
	}

	deleteOpts := client.DeleteOptions{}
	deleteOpts.ApplyOptions(opts)

	return o.Delete().
		NamespaceIfScoped(o.GetNamespace(), o.isNamespaced()).
		Resource(o.resource()).
		Name(o.GetName()).
		Body(deleteOpts.AsDeleteOptions()).
		Do(ctx).
		Error()
}

// DeleteAllOf implements client.Client.
func (in *remoteClusterClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.Unstructured)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.DeleteAllOf(ctx, obj, opts...)
	}

	o, err := in.getObjMeta(cluster, u)
	if err != nil {
		return err
	}

	deleteAllOfOpts := client.DeleteAllOfOptions{}
	deleteAllOfOpts.ApplyOptions(opts)

	return o.Delete().
		NamespaceIfScoped(deleteAllOfOpts.ListOptions.Namespace, o.isNamespaced()).
		Resource(o.resource()).
		VersionedParams(deleteAllOfOpts.AsListOptions(), in.paramCodec).
		Body(deleteAllOfOpts.AsDeleteOptions()).
		Do(ctx).
		Error()
}

// Patch implements client.Client.
func (in *remoteClusterClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.Unstructured)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.Patch(ctx, obj, patch, opts...)
	}

	o, err := in.getObjMeta(cluster, u)
	if err != nil {
		return err
	}

	data, err := patch.Data(obj)
	if err != nil {
		return err
	}

	patchOpts := &client.PatchOptions{}
	patchOpts.ApplyOptions(opts)

	return o.Patch(patch.Type()).
		NamespaceIfScoped(o.GetNamespace(), o.isNamespaced()).
		Resource(o.resource()).
		Name(o.GetName()).
		VersionedParams(patchOpts.AsPatchOptions(), in.paramCodec).
		Body(data).
		Do(ctx).
		Into(obj)
}

// Get implements client.Client.
func (in *remoteClusterClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.Unstructured)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.Get(ctx, key, obj)
	}

	gvk := u.GroupVersionKind()

	o, err := in.getObjMeta(cluster, u)
	if err != nil {
		return err
	}

	result := o.Get().
		NamespaceIfScoped(key.Namespace, o.isNamespaced()).
		Resource(o.resource()).
		Name(key.Name).
		Do(ctx).
		Into(obj)

	u.SetGroupVersionKind(gvk)

	return result
}

// List implements client.Client.
func (in *remoteClusterClient) List(ctx context.Context, obj client.ObjectList, opts ...client.ListOption) error {
	cluster, _ := ClusterFrom(ctx)
	u, ok := obj.(*unstructured.UnstructuredList)
	if IsLocal(cluster) || !ok {
		return in.defaultClient.List(ctx, obj, opts...)
	}

	_u := &unstructured.Unstructured{}
	_u.SetGroupVersionKind(u.GroupVersionKind())
	o, err := in.getObjMeta(cluster, _u)
	if err != nil {
		return err
	}

	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)

	return o.Get().
		NamespaceIfScoped(listOpts.Namespace, o.isNamespaced()).
		Resource(o.resource()).
		VersionedParams(listOpts.AsListOptions(), in.paramCodec).
		Do(ctx).
		Into(obj)
}

// resourceMeta caches state for a Kubernetes type.
type resourceMeta struct {
	// client is the rest client used to talk to the apiserver
	rest.Interface
	// gvk is the GroupVersionKind of the resourceMeta
	gvk schema.GroupVersionKind
	// mapping is the rest mapping
	mapping *meta.RESTMapping
}

// isNamespaced returns true if the type is namespaced.
func (r *resourceMeta) isNamespaced() bool {
	return r.mapping.Scope.Name() != meta.RESTScopeNameRoot
}

// resource returns the resource name of the type.
func (r *resourceMeta) resource() string {
	return r.mapping.Resource.Resource
}

// objMeta stores type and object information about a Kubernetes type.
type objMeta struct {
	// resourceMeta contains type information for the object
	*resourceMeta

	// Object contains meta data for the object instance
	*unstructured.Unstructured
}

var _ runtime.ParameterCodec = noConversionParamCodec{}

// noConversionParamCodec is a no-conversion codec for serializing parameters into URL query strings.
// it's useful in scenarios with the unstructured client and arbitrary resources.
type noConversionParamCodec struct{}

func (noConversionParamCodec) EncodeParameters(obj runtime.Object, to schema.GroupVersion) (url.Values, error) {
	return queryparams.Convert(obj)
}

func (noConversionParamCodec) DecodeParameters(parameters url.Values, from schema.GroupVersion, into runtime.Object) error {
	return fmt.Errorf("DecodeParameters not implemented on noConversionParamCodec")
}

// NewNoConversionParamCodec create a empty ParameterCodec
func NewNoConversionParamCodec() runtime.ParameterCodec {
	return noConversionParamCodec{}
}
