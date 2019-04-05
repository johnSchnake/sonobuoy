package dynamic

import (
	"encoding/json"
	"os"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"

	"fmt"
)

// A scoped down meta.MetadataAccessor
type MetadataAccessor interface {
	Namespace(runtime.Object) (string, error)
	Name(runtime.Object) (string, error)
	ResourceVersion(runtime.Object) (string, error)
}

// A scoped down meta.RESTMapper
type mapper interface {
	RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error)
}

// APIHelper wraps the client-go dynamic client and exposes a simple interface.
type APIHelper struct {
	Client          dynamic.Interface
	DiscoveryClient discovery.DiscoveryInterface

	Mapper   mapper
	Accessor MetadataAccessor
}

// NewAPIHelperFromRESTConfig creates a new APIHelper with default objects
// from client-go.
func NewAPIHelperFromRESTConfig(cfg *rest.Config) (*APIHelper, error) {
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not create dynamic client")
	}
	discover, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not create discovery client")
	}
	groupResources, err := restmapper.GetAPIGroupResources(discover)
	if err != nil {
		return nil, errors.Wrap(err, "could not get api group resources")
	}
	mapper := restmapper.NewDiscoveryRESTMapper(groupResources)
	return &APIHelper{
		Client:          dynClient,
		Mapper:          mapper,
		Accessor:        meta.NewAccessor(),
		DiscoveryClient: discover,
	}, nil
}

// NewAPIHelper returns an APIHelper with the internals instantiated.
func NewAPIHelper(dyn dynamic.Interface, mapper mapper, accessor MetadataAccessor) (*APIHelper, error) {
	return &APIHelper{
		Client:   dyn,
		Mapper:   mapper,
		Accessor: accessor,
	}, nil
}

// CreateObject attempts to create any kubernetes object.
func (a *APIHelper) CreateObject(obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	restMapping, err := a.Mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
	if err != nil {
		return nil, errors.Wrap(err, "could not get restMapping")
	}
	name, err := a.Accessor.Name(obj)
	if err != nil {
		return nil, errors.Wrap(err, "could not get name for object")
	}
	namespace, err := a.Accessor.Namespace(obj)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't get namespace for object %s", name)
	}

	rsc := a.Client.Resource(restMapping.Resource)
	if rsc == nil {
		return nil, errors.New("failed to get a resource interface")
	}
	ri := rsc.Namespace(namespace)
	return ri.Create(obj, metav1.CreateOptions{})
}

// if filterResources is empty it queries them all
func (a *APIHelper) ListNSResources(ns string, filterResources []string) (map[string][]unstructured.Unstructured, error) {
	resourceMap, err := a.DiscoveryClient.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	listOpt := metav1.ListOptions{}
	if len(ns) > 0 {
		listOpt.FieldSelector = "metadata.namespace=" + ns
	}

	allResources := []schema.GroupVersionResource{}
	for _, apiResourceList := range resourceMap {
		version, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			//fmt.Println("schnake parsing scheme err", err)
		}
		for _, apiResource := range apiResourceList.APIResources {
			//fmt.Printf("apiresource info: %#v\n\n", apiResource)
			if len(filterResources) > 0 && !sliceContains(filterResources, apiResource.Name) {
				continue
			}

			if !apiResource.Namespaced {
				continue
			}

			listable := false
			for _, v := range apiResource.Verbs {
				if v == "list" {
					listable = true
					break
				}
			}
			if listable {
				allResources = append(allResources, version.WithResource(apiResource.Name))
			}
		}
	}

	//fmt.Println("all resources:", allResources)

	results := map[string][]unstructured.Unstructured{}
	for _, gvr := range allResources {
		fmt.Println("gvr:", gvr)
		resourceClient := a.Client.Resource(gvr)

		resources, err := resourceClient.List(listOpt)
		if err != nil {
			return nil, errors.Wrapf(err, "listing resource %v", gvr)
		}
		/*
			// schnake nice for debug but should just be marshal for size
			b, err := json.MarshalIndent(stuff.Items, "", "  ")
			if err != nil {
				return nil, errors.Wrapf(err, "marshalling resource %v", gvr)
			}
			fmt.Printf("--- ITEMs NS: %v Details: %v\n", string(b))
			fmt.Println()
		*/

		results[gvr.String()] = resources.Items
	}
	return results, nil
}

func sliceContains(set []string, val string) bool {
	for _, v := range set {
		if v == val {
			return true
		}
	}
	return false
}

func (a *APIHelper) ListAll() {

	//indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	//target := dynamiclister.New(indexer, test.gvrToSync).Namespace(test.namespaceToSync)

	resourceMap, err := a.DiscoveryClient.ServerPreferredResources()
	_ = err
	allResources := map[schema.GroupVersionResource]bool{}
	for _, apiResourceList := range resourceMap {
		version, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			fmt.Println("schnake parsing scheme err", err)
		}
		for _, apiResource := range apiResourceList.APIResources {
			fmt.Printf("apiresource info: %#v\n\n", apiResource)

			listable, gettable := false, false
			for _, v := range apiResource.Verbs {
				if v == "list" {
					listable = true
					break
				}
				if v == "get" {
					gettable = true
				}
			}
			switch {
			case listable:
				allResources[version.WithResource(apiResource.Name)] = true
			case gettable:
				fmt.Println("not listable BUT gettable:", version.WithResource(apiResource.Name))
				fmt.Printf("apiresource info: %#v\n\n", apiResource)
				allResources[version.WithResource(apiResource.Name)] = false
			default:
				fmt.Println("not listable or gettable:", version.WithResource(apiResource.Name))
				fmt.Printf("apiresource info: %#v\n\n", apiResource)

				continue
			}
		}
	}

	fmt.Println("all resources:", allResources)

	for gvr, listable := range allResources {
		fmt.Println("gvr:", gvr)
		resourceClient := a.Client.Resource(gvr)

		if listable {
			stuff, err := resourceClient.List(metav1.ListOptions{
				//FieldSelector: "metadata.namespace=",
			})
			if err != nil {
				fmt.Println("SCHNAKE ERR stuff:", err)
				os.Exit(1)
			}
			b, err := json.MarshalIndent(stuff.Items, "", "  ")
			if err != nil {
				fmt.Println("schnake marshal err", err)
				continue
			}
			fmt.Printf("--- ITEMs NS: %v Details: %v\n", string(b))
			fmt.Println()
		} else {
			v, err := resourceClient.Get("", metav1.GetOptions{})
			if err != nil {
				fmt.Println("SCHNAKE ERR get thing", gvr, gvr.String(), err)
				os.Exit(1)
			}
			b, err := v.MarshalJSON()
			fmt.Println("schnake marshal err", err)
			fmt.Printf("--- --- GETTABLE ONLY ITEM NS: %v Details: %v\n", v.GetNamespace(), string(b))
			fmt.Println()
		}
	}
	os.Exit(1)

	// not getting things like pod logs or healthz etc since they are not listable

	//dump it into a structure (write the json out)
	// namespaced or not
	// namespace (if one)
	// gvk
	// item

	fmt.Println("allresources?", allResources)
	os.Exit(1)
}

// Name returns the name of the kubernetes object.
func (a *APIHelper) Name(obj *unstructured.Unstructured) (string, error) {
	return a.Accessor.Name(obj)
}

// Namespace returns the namespace of the kubernetes object.
func (a *APIHelper) Namespace(obj *unstructured.Unstructured) (string, error) {
	return a.Accessor.Namespace(obj)
}

// ResourceVersion returns the resource version of a kubernetes object.
func (a *APIHelper) ResourceVersion(obj *unstructured.Unstructured) (string, error) {
	restMapping, err := a.Mapper.RESTMapping(obj.GroupVersionKind().GroupKind(), obj.GroupVersionKind().Version)
	if err != nil {
		return "", errors.Wrap(err, "could not get restMapping")
	}
	return restMapping.Resource.GroupResource().Resource, nil
}
