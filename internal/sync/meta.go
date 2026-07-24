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
	"context"
	"strings"

	"go.uber.org/zap"

	"github.com/kcp-dev/api-syncagent/internal/crypto"

	"github.com/kcp-dev/logicalcluster/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
)

func ensureLabels(obj metav1.Object, desiredLabels map[string]string) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	for k, v := range desiredLabels {
		labels[k] = v
	}

	obj.SetLabels(labels)
}

func ensureAnnotations(obj metav1.Object, desiredAnnotations map[string]string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}

	for k, v := range desiredAnnotations {
		annotations[k] = v
	}

	obj.SetAnnotations(annotations)
}

func ensureFinalizer(ctx context.Context, log *zap.SugaredLogger, client ctrlruntimeclient.Client, obj *unstructured.Unstructured, finalizer string) (updated bool, err error) {
	finalizers := sets.New(obj.GetFinalizers()...)
	if finalizers.Has(deletionFinalizer) {
		return false, nil
	}

	original := obj.DeepCopy()

	finalizers.Insert(deletionFinalizer)
	obj.SetFinalizers(sets.List(finalizers))

	log.Debugw("Adding finalizer…", "on", newObjectKey(obj, "", logicalcluster.None), "finalizer", finalizer)
	if err := client.Patch(ctx, obj, ctrlruntimeclient.MergeFrom(original)); err != nil {
		return false, err
	}

	return true, nil
}

func removeFinalizer(ctx context.Context, log *zap.SugaredLogger, client ctrlruntimeclient.Client, obj *unstructured.Unstructured, finalizer string) (updated bool, err error) {
	finalizers := sets.New(obj.GetFinalizers()...)
	if !finalizers.Has(deletionFinalizer) {
		return false, nil
	}

	original := obj.DeepCopy()

	finalizers.Delete(deletionFinalizer)
	obj.SetFinalizers(sets.List(finalizers))

	log.Debugw("Removing finalizer…", "on", newObjectKey(obj, "", logicalcluster.None), "finalizer", finalizer)
	if err := client.Patch(ctx, obj, ctrlruntimeclient.MergeFrom(original)); err != nil {
		return false, err
	}

	return true, nil
}

type objectKey struct {
	ClusterName   logicalcluster.Name
	WorkspacePath logicalcluster.Path
	Namespace     string
	Name          string
}

func newObjectKey(obj metav1.Object, clusterName logicalcluster.Name, workspacePath logicalcluster.Path) objectKey {
	return objectKey{
		ClusterName:   clusterName,
		WorkspacePath: workspacePath,
		Namespace:     obj.GetNamespace(),
		Name:          obj.GetName(),
	}
}

func (k objectKey) String() string {
	result := k.Name
	if k.Namespace != "" {
		result = k.Namespace + "/" + result
	}
	if k.ClusterName != "" {
		result = string(k.ClusterName) + "|" + result
	}

	return result
}

func (k objectKey) Key() string {
	return crypto.Hash(k)
}

func (k objectKey) Labels() labels.Set {
	// Name and namespace can be more than 63 characters long, so we must hash them
	// to turn them into valid label values. The full, original value is kept as an annotation.
	s := labels.Set{
		remoteObjectClusterLabel:  string(k.ClusterName),
		remoteObjectNameHashLabel: crypto.Hash(k.Name),
	}

	if k.Namespace != "" {
		s[remoteObjectNamespaceHashLabel] = crypto.Hash(k.Namespace)
	}

	return s
}

func (k objectKey) Annotations() labels.Set {
	s := labels.Set{
		remoteObjectNameAnnotation: k.Name,
	}

	if k.Namespace != "" {
		s[remoteObjectNamespaceAnnotation] = k.Namespace
	}

	if !k.WorkspacePath.Empty() {
		s[remoteObjectWorkspacePathAnnotation] = k.WorkspacePath.String()
	}

	return s
}

// relatedCopyLabels builds the provenance labels put on a destination copy of a related resource.
// They tie the copy to its owning primary object and the related resource identifier so that all
// copies of a given (primary, identifier) can be enumerated via relatedCopySelector. The owner tuple
// — the primary's logical cluster (workspace), the owning PublishedResource and the primary's
// namespace/name — is hashed into a single related-owner label; because the prune List is
// cluster-wide on the shared destination, all of these dimensions must be part of the identity, and
// folding them into one hash makes that uniqueness inherent in the hash input (and keeps the value a
// valid label regardless of the source lengths or characters). The identifier is used verbatim (the
// API constrains it to a valid label value) and the agent name (when set) is included so that each
// agent only ever prunes its own copies.
func relatedCopyLabels(primary ctrlruntimeclient.Object, clusterName logicalcluster.Name, publishedResourceName, identifier, agentName string) map[string]string {
	set := map[string]string{
		relatedOwnerLabel:      relatedOwnerHash(clusterName, publishedResourceName, primary.GetNamespace(), primary.GetName()),
		relatedIdentifierLabel: identifier,
	}

	if agentName != "" {
		set[agentNameLabel] = agentName
	}

	return set
}

// relatedOwnerHash hashes the owner tuple (cluster, PublishedResource, primary namespace, primary
// name) into a single label value. The NUL separator keeps the dimensions unambiguous, so e.g.
// cluster "a" + name "bc" cannot collide with cluster "ab" + name "c". A cluster-scoped primary has
// an empty namespace, which folds in cleanly and cannot collide with a namespaced primary (whose
// namespace is never empty).
func relatedOwnerHash(clusterName logicalcluster.Name, publishedResourceName, namespace, name string) string {
	return crypto.Hash(strings.Join([]string{string(clusterName), publishedResourceName, namespace, name}, "\x00"))
}

// relatedCopyAnnotations builds the human-facing provenance annotations (plaintext primary
// namespace/name) for a destination copy of a related resource.
func relatedCopyAnnotations(primary ctrlruntimeclient.Object) map[string]string {
	set := map[string]string{
		relatedPrimaryNameAnnotation: primary.GetName(),
	}

	if namespace := primary.GetNamespace(); namespace != "" {
		set[relatedPrimaryNamespaceAnnotation] = namespace
	}

	return set
}

// relatedCopySelector returns a label selector matching exactly the destination copies created for
// the given primary object and related resource identifier (scoped to the agent when set). Because
// it mirrors relatedCopyLabels, only objects the agent itself labelled are ever selected, so
// hand-created objects are never in scope for pruning.
func relatedCopySelector(primary ctrlruntimeclient.Object, clusterName logicalcluster.Name, publishedResourceName, identifier, agentName string) labels.Selector {
	return labels.SelectorFromSet(relatedCopyLabels(primary, clusterName, publishedResourceName, identifier, agentName))
}
