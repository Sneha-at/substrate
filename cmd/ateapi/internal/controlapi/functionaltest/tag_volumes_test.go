// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package functionaltest

import (
	"context"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restoreRecordingPlugin is the mock volume plugin, recording the snapshot
// each volume was created from, keyed by the volume's storage name.
type restoreRecordingPlugin struct {
	*volume.MockVolumePlugin

	mu      sync.Mutex
	sources map[string]string
}

func newRestoreRecordingPlugin() *restoreRecordingPlugin {
	return &restoreRecordingPlugin{MockVolumePlugin: volume.NewMockVolumePlugin(), sources: map[string]string{}}
}

func (p *restoreRecordingPlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	p.mu.Lock()
	p.sources[req.Name] = req.SourceSnapshotID
	p.mu.Unlock()
	return p.MockVolumePlugin.CreateVolume(ctx, req)
}

// sourceOf returns the snapshot the named volume of an Actor was created from,
// "" if it was provisioned empty, and whether it was created at all.
func (p *restoreRecordingPlugin) sourceOf(actor *ateapipb.Actor, volName string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	src, ok := p.sources["substrate-"+actor.GetMetadata().GetUid()+"-"+volName]
	return src, ok
}

// setupTagVolumesTest starts a server whose mock volume plugin records restore
// sources, and creates a template declaring the external volumes "data" and
// "cache" plus a worker to run its Actors on.
func setupTagVolumesTest(t *testing.T, ns string) (*testContext, *restoreRecordingPlugin, string) {
	t.Helper()
	plugin := newRestoreRecordingPlugin()
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	var volumes []*ateapipb.Volume
	var mounts []*ateapipb.VolumeMount
	for _, name := range []string{"data", "cache"} {
		volumes = append(volumes, &ateapipb.Volume{
			Name:                   name,
			ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{StorageClassName: "standard", Capacity: "1Gi"},
		})
		mounts = append(mounts, &ateapipb.VolumeMount{Name: name, MountPath: "/mnt/" + name})
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	return tc, plugin, workerName
}

// cloneAndResume creates an Actor from tagName, resumes it so its volumes are
// provisioned, and returns it.
func cloneAndResume(t *testing.T, tc *testContext, workerName, tagName, name string) *ateapipb.Actor {
	t.Helper()
	ctx := context.Background()
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		SourceTag:     &ateapipb.ObjectRef{Atespace: testAtespace, Name: tagName},
	}}); err != nil {
		t.Fatalf("CreateActor(%s) from tag %s failed: %v", name, tagName, err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	resumed, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor(%s) failed: %v", name, err)
	}
	for _, vol := range resumed.GetActor().GetStatus().GetActorVolumes() {
		if vol.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED {
			t.Fatalf("ResumeActor(%s): volume %q is %v, want CREATED", name, vol.GetVolumeName(), vol.GetStatus())
		}
	}
	return resumed.GetActor()
}

// tagVolumeHandles returns the snapshot handle a tag records per volume.
func tagVolumeHandles(tag *ateapipb.Tag) map[string]string {
	handles := map[string]string{}
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		handles[snap.GetSourceVolumeName()] = snap.GetStorageSnapshotId()
	}
	return handles
}

// TestCreateActorFromTag_RestoresVolumes runs the whole path: a tag taken with
// external volumes snapshots each of them, and an Actor created from that tag
// gets each volume restored from its snapshot when it is first resumed.
func TestCreateActorFromTag_RestoresVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-restore-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	tag, err := tc.client.CreateTag(ctx, &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "with-volumes"},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		IncludeExternalVolumes: true,
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}
	handles := tagVolumeHandles(tag)
	if len(handles) != 2 || handles["data"] == "" || handles["cache"] == "" {
		t.Fatalf("tag volume snapshots = %v, want a handle for each of data and cache", tag.GetStatus().GetSnapshot().GetVolumeSnapshots())
	}

	clone := cloneAndResume(t, tc, workerName, "with-volumes", "clone")
	got := map[string]string{}
	for _, name := range []string{"data", "cache"} {
		src, ok := plugin.sourceOf(clone, name)
		if !ok {
			t.Fatalf("volume %q of the clone was never created", name)
		}
		got[name] = src
	}
	if diff := cmp.Diff(handles, got); diff != "" {
		t.Errorf("restore sources mismatch (-tag handles +restored from):\n%s", diff)
	}
}

// TestCreateActorFromTag_RestoresNamedVolumes verifies that a tag taken with a
// named subset of the volumes captures only those, and that an Actor created
// from it restores them while provisioning the rest empty.
func TestCreateActorFromTag_RestoresNamedVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-restore-named-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	tag, err := tc.client.CreateTag(ctx, &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "data-only"},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		IncludeExternalVolumes: true,
		ExternalVolumeNames:    []string{"data"},
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}
	handles := tagVolumeHandles(tag)
	if len(handles) != 1 || handles["data"] == "" {
		t.Fatalf("tag volume snapshots = %v, want a handle for data only", tag.GetStatus().GetSnapshot().GetVolumeSnapshots())
	}

	clone := cloneAndResume(t, tc, workerName, "data-only", "clone")
	got := map[string]string{}
	for _, name := range []string{"data", "cache"} {
		src, ok := plugin.sourceOf(clone, name)
		if !ok {
			t.Fatalf("volume %q of the clone was never created", name)
		}
		got[name] = src
	}
	want := map[string]string{"data": handles["data"], "cache": ""}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("restore sources mismatch (-want +got):\n%s", diff)
	}
}

// TestCreateTag_UnknownVolumeName verifies that naming a volume the source
// Actor does not have fails the create and leaves no tag behind.
func TestCreateTag_UnknownVolumeName(t *testing.T) {
	ns := namespaceForTest("ns-tag-unknown-volume")
	tc, _, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	_, err := tc.client.CreateTag(ctx, &ateapipb.CreateTagRequest{
		Tag: &ateapipb.Tag{
			Metadata:    &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "typo"},
			Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		IncludeExternalVolumes: true,
		ExternalVolumeNames:    []string{"data", "dta"},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateTag() = %v, want InvalidArgument", err)
	}
	_, err = tc.client.GetTag(ctx, &ateapipb.GetTagRequest{Tag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: "typo"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTag() after a failed create = %v, want NotFound", err)
	}
}
