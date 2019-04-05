/*
Copyright 2018 Heptio Inc.

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

package discovery

import (
	"fmt"
	"os"
	"path"
	"time"

	"k8s.io/client-go/kubernetes"

	"github.com/heptio/sonobuoy/pkg/dynamic"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/heptio/sonobuoy/pkg/config"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// NSResourceLocation is the place under which namespaced API resources (pods, etc) are stored
	NSResourceLocation = "resources/ns"
	// ClusterResourceLocation is the place under which non-namespaced API resources (nodes, etc) are stored
	ClusterResourceLocation = "resources/cluster"
	// HostsLocation is the place under which host information (configz, healthz) is stored
	HostsLocation = "hosts"
	// MetaLocation is the place under which snapshot metadata (query times, config) is stored
	MetaLocation = "meta"
)

type listQuery func() (*unstructured.UnstructuredList, error)
type objQuery func() (interface{}, error)

// timedListQuery performs a list query and serialize the results
func timedListQuery(outpath string, file string, f listQuery) (time.Duration, error) {
	start := time.Now()
	list, err := f()
	duration := time.Since(start)
	if err != nil {
		return duration, err
	}

	if len(list.Items) > 0 {
		err = errors.WithStack(SerializeObj(list.Items, outpath, file))
	}
	return duration, err
}

func timedObjectQuery(outpath string, file string, f objQuery) (time.Duration, error) {
	start := time.Now()
	obj, err := f()
	duration := time.Since(start)
	if err != nil {
		return duration, err
	}

	return duration, errors.WithStack(SerializeObj(obj, outpath, file))
}

// timedQuery Wraps the execution of the function with a recorded timed snapshot
func timedQuery(recorder *QueryRecorder, name string, ns string, fn func() (time.Duration, error)) {
	duration, fnErr := fn()
	recorder.RecordQuery(name, ns, duration, fnErr)
}

func sliceContains(set []string, val string) bool {
	for _, v := range set {
		if v == val {
			return true
		}
	}
	return false
}

// given the filter options and a query against the given ns; what resources should we query? resourceNameList being empty means all. Only kept that for backwards compat.
func getResources(client *dynamic.APIHelper, ns *string, filterOpts config.FilterOptions, resourceNameList []string) ([]schema.GroupVersionResource, error) {
	// if not in a chosen ns; just return nil

	resourceMap, err := client.DiscoveryClient.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	listOpt := metav1.ListOptions{}
	if ns != nil && len(*ns) > 0 {
		listOpt.FieldSelector = "metadata.namespace=" + *ns
	}
	listOpt.LabelSelector = filterOpts.LabelSelector

	allResources := []schema.GroupVersionResource{}
	for _, apiResourceList := range resourceMap {
		version, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			return nil, errors.Wrap(err, "parsing schema")
		}
		for _, apiResource := range apiResourceList.APIResources {
			if len(resourceNameList) > 0 && !sliceContains(resourceNameList, apiResource.Name) {
				continue
			}

			// Only look at either NS objects or cluster objects
			if (ns != nil && !apiResource.Namespaced) ||
				ns == nil && apiResource.Namespaced {
				continue
			}

			// Double check the resources are listable.
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
	return allResources, nil
}

// QueryNSResources will query namespace-specific resources in the cluster,
// writing them out to <resultsdir>/resources/ns/<ns>/*.json
// TODO: Eliminate dependencies from config.Config and pass in data
func QueryResources(client *dynamic.APIHelper, recorder *QueryRecorder, ns *string, cfg *config.Config) error {
	if ns != nil {
		logrus.Infof("Running ns query (%v)", *ns)
	} else {
		logrus.Info("Running cluster queries")
	}

	// 1. Create the parent directory we will use to store the results
	outdir := path.Join(cfg.OutputDir(), ClusterResourceLocation)
	if ns != nil {
		outdir = path.Join(cfg.OutputDir(), NSResourceLocation, *ns)
	}

	if err := os.MkdirAll(outdir, 0755); err != nil {
		return errors.WithStack(err)
	}

	// 2. Setup label filter if there is one.
	opts := metav1.ListOptions{}
	if len(cfg.Filters.LabelSelector) > 0 {
		if _, err := labels.Parse(cfg.Filters.LabelSelector); err != nil {
			logrus.Warningf("Labelselector %v failed to parse with error %v", cfg.Filters.LabelSelector, err)
		} else {
			opts.LabelSelector = cfg.Filters.LabelSelector
		}
	}
	if ns != nil && len(*ns) > 0 {
		opts.FieldSelector = "metadata.namespace=" + *ns
	}

	resources, err := getResources(client, ns, cfg.Filters, nil)
	if err != nil {
		return errors.Wrap(err, "choosing resources to gather")
	}

	// 3. Execute the ns-query
	for _, gvk := range resources {
		lister := func() (*unstructured.UnstructuredList, error) {
			resourceClient := client.Client.Resource(gvk)
			resources, err := resourceClient.List(opts)

			return resources, errors.Wrapf(err, "listing resource %v", gvk)
		}
		query := func() (time.Duration, error) { return timedListQuery(outdir+"/", gvk.Resource+".json", lister) }
		timedQuery(recorder, gvk.Resource, fmt.Sprint(ns), query)
	}

	return nil
}

func QueryPodLogs(kubeClient kubernetes.Interface, recorder *QueryRecorder, ns string, cfg *config.Config) error {
	start := time.Now()

	opts := metav1.ListOptions{}
	if len(cfg.Filters.LabelSelector) > 0 {
		if _, err := labels.Parse(cfg.Filters.LabelSelector); err != nil {
			logrus.Warningf("Labelselector %v failed to parse with error %v", cfg.Filters.LabelSelector, err)
		} else {
			opts.LabelSelector = cfg.Filters.LabelSelector
		}
	}

	err := gatherPodLogs(kubeClient, ns, opts, cfg)
	if err != nil {
		return err
	}
	duration := time.Since(start)
	recorder.RecordQuery("PodLogs", ns, duration, err)
	return nil
}

func QueryHostData(kubeClient kubernetes.Interface, recorder *QueryRecorder, cfg *config.Config) error {
	start := time.Now()

	// TODO(chuckha) look at FieldSelector for list options{}
	nodeList, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get node list")
	}
	nodeNames := make([]string, len(nodeList.Items))
	for i, node := range nodeList.Items {
		nodeNames[i] = node.Name
	}
	err = gatherNodeData(nodeNames, kubeClient.CoreV1().RESTClient(), cfg)
	duration := time.Since(start)
	recorder.RecordQuery("Nodes", "", duration, err)

	return nil
}

/*
// QueryClusterResources queries non-namespace resources in the cluster, writing
// them out to <resultsdir>/resources/non-ns/*.json
// TODO: Eliminate dependencies from config.Config and pass in data
func QueryClusteresources(kubeClient kubernetes.Interface, recorder *QueryRecorder, cfg *config.Config) error {
	logrus.Infof("Running non-ns query")

	resources := cfg.FilterResources(config.ClusterResources)

	// 1. Create the parent directory we will use to store the results
	outdir := path.Join(cfg.OutputDir(), ClusterResourceLocation)
	if len(resources) > 0 {
		if err := os.MkdirAll(outdir, 0755); err != nil {
			return errors.WithStack(err)
		}
	}

	// 2. Execute the non-ns-query
	for _, resourceKind := range resources {
		switch resourceKind {
		case "ServerVersion":
			objqry := func() (interface{}, error) { return kubeClient.Discovery().ServerVersion() }
			query := func() (time.Duration, error) {
				return untypedQuery(cfg.OutputDir(), "serverversion.json", objqry)
			}
			timedQuery(recorder, "serverversion", "", query)
			continue
		case "ServerGroups":
			objqry := func() (interface{}, error) { return kubeClient.Discovery().ServerGroups() }
			query := func() (time.Duration, error) {
				return untypedQuery(cfg.OutputDir(), "servergroups.json", objqry)
			}
			timedQuery(recorder, "servergroups", "", query)
			continue
		case "Nodes":
			// cfg.Nodes configures whether users want to gather the Nodes resource in the
			// cluster, but we also use that option to guide whether we get node data such
			// as configz and healthz endpoints.

			// TODO(chuckha) Use a separate configuration like NodeConfiguration for node configz/healthz to make
			// this switch flow less confusing. "Nodes" is responsible for too much.

			// NOTE: Node data collection is an aggregated time b/c propagating that detail back up
			// is odd and would pollute some of the output.

			start := time.Now()
			// TODO(chuckha) look at FieldSelector for list options{}
			nodeList, err := kubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
			if err != nil {
				errlog.LogError(fmt.Errorf("failed to get node list: %v", err))
				// Do not return or continue because we also want to query nodes as resources
				break
			}
			nodeNames := make([]string, len(nodeList.Items))
			for i, node := range nodeList.Items {
				nodeNames[i] = node.Name
			}
			err = gatherNodeData(nodeNames, kubeClient.CoreV1().RESTClient(), cfg)
			duration := time.Since(start)
			recorder.RecordQuery("Nodes", "", duration, err)
			// do not continue because we want to now query nodes as resources
		}
		lister := func() (runtime.Object, error) { return queryNonNsResource(resourceKind, kubeClient) }
		query := func() (time.Duration, error) { return objListQuery(outdir+"/", resourceKind+".json", lister) }
		timedQuery(recorder, resourceKind, "", query)
	}

	return nil
}
*/
