/*
Copyright 2025 The KCP Authors.

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

package sync

import (
	"strings"
	"testing"

	"github.com/kcp-dev/logicalcluster/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func createNewObject(name, namespace string) metav1.Object {
	obj := &unstructured.Unstructured{}
	obj.SetName(name)
	obj.SetNamespace(namespace)

	return obj
}

// TestRelatedCopyLabelsAreValidLabelSet guards the invariant that lets relatedCopyLabels use the
// related-resource identifier verbatim as a label value: as long as the identifier stays within the
// bounds the CRD enforces (a lowercase RFC 1123 label of at most 63 characters), the resulting
// provenance label set must always be accepted by the API server. Otherwise the apply/create of the
// copy fails and sync silently breaks for that related resource.
func TestRelatedCopyLabelsAreValidLabelSet(t *testing.T) {
	// Names and namespaces are hashed, so they can be arbitrarily long/invalid; use overlong ones to
	// make sure the hashing keeps the label set valid.
	longName := strings.Repeat("a", 300)

	// maxIdentifier is the longest identifier the CRD accepts (63 chars, matching the pattern).
	maxIdentifier := "a" + strings.Repeat("b", 61) + "c"

	testcases := []struct {
		name             string
		primary          ctrlruntimeclient.Object
		clusterName      logicalcluster.Name
		publishedResName string
		identifier       string
		agentName        string
	}{
		{
			name:             "typical namespaced primary",
			primary:          createNewUnstructured("my-primary", "kube-system"),
			clusterName:      "root",
			publishedResName: "my-published-resource",
			identifier:       "connection-details",
			agentName:        "agent-1",
		},
		{
			// clusterName is a kcp cluster ID (a colon-free hash), used verbatim like the main
			// object's remote-object-cluster label; the workspace path (with colons) is never used here.
			name:             "max-length identifier and overlong name/namespace/published-resource",
			primary:          createNewUnstructured(longName, longName),
			clusterName:      "kvdk2spgmbld9mnc",
			publishedResName: longName,
			identifier:       maxIdentifier,
			agentName:        "",
		},
		{
			name:             "cluster-scoped primary without agent name",
			primary:          createNewUnstructured(longName, ""),
			clusterName:      "abc123",
			publishedResName: "another-published-resource",
			identifier:       "credentials",
			agentName:        "",
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			set := relatedCopyLabels(testcase.primary, testcase.clusterName, testcase.publishedResName, testcase.identifier, testcase.agentName)

			if errs := metav1validation.ValidateLabels(set, field.NewPath("metadata", "labels")); len(errs) > 0 {
				t.Fatalf("relatedCopyLabels produced an invalid label set: %v", errs)
			}
		})
	}
}

func createNewUnstructured(name, namespace string) ctrlruntimeclient.Object {
	obj := &unstructured.Unstructured{}
	obj.SetName(name)
	obj.SetNamespace(namespace)

	return obj
}

func TestObjectKey(t *testing.T) {
	testcases := []struct {
		object        metav1.Object
		clusterName   logicalcluster.Name
		workspacePath logicalcluster.Path
		expected      string
	}{
		{
			object:      createNewObject("test", ""),
			clusterName: "",
			expected:    "test",
		},
		{
			object:      createNewObject("test", "namespace"),
			clusterName: "",
			expected:    "namespace/test",
		},
		{
			object:      createNewObject("test", ""),
			clusterName: "abc123",
			expected:    "abc123|test",
		},
		{
			object:      createNewObject("test", "namespace"),
			clusterName: "abc123",
			expected:    "abc123|namespace/test",
		},
		{
			object:        createNewObject("test", "namespace"),
			clusterName:   "abc123",
			workspacePath: logicalcluster.NewPath("this:should:not:appear:in:the:key"),
			expected:      "abc123|namespace/test",
		},
	}

	for _, testcase := range testcases {
		t.Run("", func(t *testing.T) {
			key := newObjectKey(testcase.object, testcase.clusterName, testcase.workspacePath)

			if stringified := key.String(); stringified != testcase.expected {
				t.Fatalf("Expected %q but got %q.", testcase.expected, stringified)
			}
		})
	}
}
