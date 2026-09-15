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

package flavorassigner

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestElasticTASDoesNotDoubleCountReplacedSlice(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, true)
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesWithTAS, true)
	for name, other := range map[string]string{
		"replacement fits when only the old slice occupies the node":       "0",
		"replacement does not fit when another workload occupies the node": "1",
	} {
		t.Run(name, func(t *testing.T) {
			ctx, log := utiltesting.ContextWithLog(t)
			cq := newBookmarkSnapshot(ctx, t, log, "10", "0", kueue.FlavorFungibility{})
			old := utiltestingapi.MakeWorkload("old", "default").Annotation("kueue.x-k8s.io/elastic-job", "true").PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 2).Request(corev1.ResourceCPU, "1").Obj()).
				ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(utiltestingapi.MakePodSetAssignment(kueue.DefaultPodSetName).Count(2).Assignment(corev1.ResourceCPU, "flavor-1", "2").TopologyAssignment(utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).Domains(utiltestingapi.MakeTopologyDomainAssignment([]string{"node-1"}, 2).Obj()).Obj()).Obj()).Obj(), time.Now()).AdmittedAt(true, time.Now()).Obj()
			oldInfo := workload.NewInfo(log, old)
			cq.AddUsage(oldInfo.Usage())
			cq.AddUsage(workload.Usage{TAS: nodeUsageOnFlavorOne(other)})
			before, err := cq.TASFlavors["flavor-1"].SerializeFreeCapacityPerDomain()
			if err != nil {
				t.Fatal(err)
			}
			next := workload.NewInfo(log, utiltestingapi.MakeWorkload("new", "default").Annotation("kueue.x-k8s.io/elastic-job", "true").PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 4).Request(corev1.ResourceCPU, "1").UnconstrainedTopologyRequest().Obj()).Obj())
			a := New(next, cq, bookmarkTestFlavors(), false, &testOracle{}, oldInfo, configapi.QuotaCheckBlockUndeclared, resources.NewResourceFormatter(), bookmarkTestCycle).Assign(ctx, nil)
			if got := a.RepresentativeMode() == Fit; got != (other == "0") {
				t.Errorf("mode=%s, want fit=%t", a.RepresentativeMode(), other == "0")
			}
			after, err := cq.TASFlavors["flavor-1"].SerializeFreeCapacityPerDomain()
			if err != nil {
				t.Fatal(err)
			}
			if before != after {
				t.Errorf("assignment changed shared snapshot capacity: before=%s after=%s", before, after)
			}
		})
	}
}
