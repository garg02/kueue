/*
Copyright The Kubernetes Authors.

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

package tas

import (
	"fmt"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

func TestElasticTopologyRegrowth(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesWithTAS, true)
	for name, staleRequest := range map[string]bool{
		"reconcile the latest slice": false,
		"reconcile the origin slice": true,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			gvk := batchv1.SchemeGroupVersion.WithKind("Job")
			kc := utiltesting.NewClientBuilder().
				WithStatusSubresource(&kueue.Workload{}).
				WithIndex(&corev1.Pod{}, coreindexer.WorkloadSliceNameKey, coreindexer.IndexPodWorkloadSliceName).
				WithIndex(&kueue.Workload{}, coreindexer.OwnerReferenceIndexKey(gvk), coreindexer.WorkloadOwnerIndexFunc(gvk)).Build()
			r := newTopologyUngater(kc, nil, nil)
			h := podHandler{expectationsStore: r.expectationsStore}
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()
			var previous *kueue.Workload
			survivors := map[string]string{}
			for step, count := range []int32{2, 4, 2, 6} {
				name := fmt.Sprintf("slice-%d", step)
				if previous != nil {
					previous.Status.Conditions = append(previous.Status.Conditions, metav1.Condition{Type: kueue.WorkloadFinished, Status: metav1.ConditionTrue, Reason: "Scaled", LastTransitionTime: metav1.Now()})
					if err := kc.Status().Update(ctx, previous); err != nil {
						t.Fatal(err)
					}
				}
				wl := utiltestingapi.MakeWorkload(name, "ns").
					Annotation(workloadslicing.EnabledAnnotationKey, "true").
					Annotation(kueue.WorkloadSliceNameAnnotation, "slice-0").
					ControllerReference(gvk, "elastic", "job-uid").
					PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, int(count)).Request(corev1.ResourceCPU, "1").PodIndexLabel(new(batchv1.JobCompletionIndexAnnotation)).Obj()).
					ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(utiltestingapi.MakePodSetAssignment(kueue.DefaultPodSetName).Count(count).
						Assignment(corev1.ResourceCPU, "flavor", fmt.Sprint(count)).
						TopologyAssignment(utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).Domains(utiltestingapi.MakeTopologyDomainAssignment([]string{"node"}, count).Obj()).Obj()).Obj()).Obj(), time.Now()).
					AdmittedAt(true, time.Now()).Obj()
				wl.CreationTimestamp = metav1.NewTime(time.Unix(int64(step), 0))
				if err := kc.Create(ctx, wl); err != nil {
					t.Fatal(err)
				}
				for i := int32(0); i < 6; i++ {
					key := client.ObjectKey{Namespace: "ns", Name: fmt.Sprintf("pod-%d", i)}
					p := &corev1.Pod{}
					err := kc.Get(ctx, key, p)
					if i >= count {
						if err == nil {
							if err := kc.Delete(ctx, p); err != nil {
								t.Fatal(err)
							}
							delete(survivors, key.Name)
						}
						continue
					}
					if err != nil {
						p = testingpod.MakePod(key.Name, "ns").Annotation(kueue.PodSetUnconstrainedTopologyAnnotation, "true").Annotation(kueue.WorkloadAnnotation, "slice-0").Annotation(kueue.WorkloadSliceNameAnnotation, "slice-0").Label(constants.PodSetLabel, string(kueue.DefaultPodSetName)).Label(batchv1.JobCompletionIndexAnnotation, fmt.Sprint(i)).TopologySchedulingGate().Obj()
						p.UID = types.UID(fmt.Sprintf("%d-%d", step, i))
						if err := kc.Create(ctx, p); err != nil {
							t.Fatal(err)
						}
					}
				}
				requestName := name
				if staleRequest {
					requestName = "slice-0"
				}
				req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: requestName}}
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatalf("resize to %d: %v", count, err)
				}
				for i := int32(0); i < count; i++ {
					p := &corev1.Pod{}
					if err := kc.Get(ctx, client.ObjectKey{Namespace: "ns", Name: fmt.Sprintf("pod-%d", i)}, p); err != nil {
						t.Fatal(err)
					}
					if utilpod.HasGate(p, kueue.TopologySchedulingGate) {
						t.Fatalf("resize to %d left %s gated", count, p.Name)
					}
					if old, ok := survivors[p.Name]; ok && old != string(p.UID)+p.Spec.NodeSelector[corev1.LabelHostname] {
						t.Fatalf("survivor changed: %s", p.Name)
					}
					survivors[p.Name] = string(p.UID) + p.Spec.NodeSelector[corev1.LabelHostname]
					h.Update(ctx, event.UpdateEvent{ObjectNew: p}, q)
				}
				if _, err := r.Reconcile(ctx, req); err != nil {
					t.Fatalf("resize to %d: observed pod updates did not clear expectations: %v", count, err)
				}
				previous = wl
			}
		})
	}
}
