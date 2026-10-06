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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testVolumeDriver = "substrate.io/mock"

// fakeSnapshotPlugin is a volume plugin that records the snapshot calls made
// against it, so a test can assert on what the workflow asked the storage
// system to do rather than on the workflow's own bookkeeping.
type fakeSnapshotPlugin struct {
	caps volume.Capabilities

	// failSnapshotOfVolume makes CreateSnapshot fail for one source volume ID,
	// standing in for a driver that cannot capture a particular volume.
	failSnapshotOfVolume string
	// goneVolumes are source volume IDs CreateSnapshot reports as NotFound,
	// standing in for a source volume deleted since the tag was created.
	goneVolumes map[string]bool
	// readyToUse is what CreateSnapshot reports. A driver that finishes the
	// copy in the background returns false here.
	readyToUse bool
	// missingSnapshots are handles GetSnapshot reports as gone, standing in for
	// a snapshot deleted behind the control plane's back.
	missingSnapshots map[string]bool
	// failDeleteSnapshot makes DeleteSnapshot fail, standing in for a driver
	// that cannot release snapshots.
	failDeleteSnapshot bool
	// rendezvous, when set, makes each CreateSnapshot wait until that many
	// calls are in flight at once, failing if they never are. It proves the
	// caller issues the snapshots concurrently rather than one at a time.
	rendezvous int

	mu         sync.Mutex
	created    []string
	deleted    []string
	arrived    int
	allArrived chan struct{}
}

func newFakeSnapshotPlugin() *fakeSnapshotPlugin {
	return &fakeSnapshotPlugin{
		caps:       volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		readyToUse: true,
		allArrived: make(chan struct{}),
	}
}

func (f *fakeSnapshotPlugin) DriverName(context.Context) (string, error) {
	return testVolumeDriver, nil
}

func (f *fakeSnapshotPlugin) CreateVolume(_ context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	return volume.CreateVolumeResponse{
		VolumeID:                "vol-" + req.Name,
		VolumeContext:           req.Parameters,
		ContentSourceSnapshotID: req.SourceSnapshotID,
	}, nil
}

func (f *fakeSnapshotPlugin) DeleteVolume(context.Context, string) error { return nil }
func (f *fakeSnapshotPlugin) AttachVolume(context.Context, volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	return volume.AttachVolumeResponse{}, nil
}
func (f *fakeSnapshotPlugin) DetachVolume(context.Context, string, string) error { return nil }

func (f *fakeSnapshotPlugin) CreateSnapshot(ctx context.Context, req volume.CreateSnapshotRequest) (volume.Snapshot, error) {
	if f.rendezvous > 0 {
		f.mu.Lock()
		f.arrived++
		if f.arrived == f.rendezvous {
			close(f.allArrived)
		}
		f.mu.Unlock()
		select {
		case <-f.allArrived:
		case <-time.After(5 * time.Second):
			f.mu.Lock()
			defer f.mu.Unlock()
			return volume.Snapshot{}, fmt.Errorf("only %d of %d snapshots were in flight together", f.arrived, f.rendezvous)
		case <-ctx.Done():
			return volume.Snapshot{}, ctx.Err()
		}
	}
	if req.SourceVolumeID == f.failSnapshotOfVolume {
		return volume.Snapshot{}, fmt.Errorf("simulated snapshot failure for %q", req.SourceVolumeID)
	}
	if f.goneVolumes[req.SourceVolumeID] {
		return volume.Snapshot{}, fmt.Errorf("CSI CreateSnapshot failed: %w", status.Errorf(codes.NotFound, "source volume %q not found", req.SourceVolumeID))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := "snap-" + req.Name
	f.created = append(f.created, id)
	return volume.Snapshot{
		SnapshotID:     id,
		SourceVolumeID: req.SourceVolumeID,
		ReadyToUse:     f.readyToUse,
		SizeBytes:      1024,
	}, nil
}

func (f *fakeSnapshotPlugin) GetSnapshot(_ context.Context, snapshotID string) (volume.Snapshot, bool, error) {
	if f.missingSnapshots[snapshotID] {
		return volume.Snapshot{}, false, nil
	}
	return volume.Snapshot{SnapshotID: snapshotID, ReadyToUse: f.readyToUse}, true, nil
}

func (f *fakeSnapshotPlugin) DeleteSnapshot(_ context.Context, snapshotID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDeleteSnapshot {
		return fmt.Errorf("simulated delete failure for %q", snapshotID)
	}
	f.deleted = append(f.deleted, snapshotID)
	return nil
}

func (f *fakeSnapshotPlugin) ControllerCapabilities(context.Context) (volume.Capabilities, error) {
	return f.caps, nil
}

func (f *fakeSnapshotPlugin) snapshotsCreated() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.created...)
}

func (f *fakeSnapshotPlugin) snapshotsDeleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

var _ volume.VolumePluginControlPlane = (*fakeSnapshotPlugin)(nil)

// newVolumeTagWorkflow builds a tag workflow wired to a single fake driver.
func newVolumeTagWorkflow(persistence store.Interface, plugin *fakeSnapshotPlugin) (*ActorWorkflow, *objectstoretest.Fake) {
	objects := objectstoretest.New()
	return &ActorWorkflow{
		store:       persistence,
		objectStore: objects,
		pluginRegistry: &mockPluginRegistry{
			plugins: map[string]volume.VolumePluginControlPlane{testVolumeDriver: plugin},
		},
	}, objects
}

// seedTagSourceWithVolumes is seedTagSource plus provisioned external volumes,
// the state an actor is in when it can be tagged with its volumes.
func seedTagSourceWithVolumes(t *testing.T, ctx context.Context, persistence store.Interface, objects *objectstoretest.Fake, template *ateapipb.ActorTemplate, name string, volumeNames ...string) *ateapipb.Actor {
	t.Helper()
	actor, _ := seedTagSource(t, ctx, persistence, objects, template, name, "manifest.json")
	return mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		for _, volName := range volumeNames {
			s.ActorVolumes = append(s.ActorVolumes, &ateapipb.ExternalVolume{
				VolumeName:      volName,
				StorageVolumeId: "vol-" + volName,
				VolumeType:      testVolumeDriver,
				Status:          ateapipb.ExternalVolume_STATUS_CREATED,
			})
		}
	})
}

// TestTagActorSnapshot_CapturesVolumes verifies that a tag asked to include
// external volumes snapshots every one of the actor's external volumes and
// publishes a handle for each on its own snapshot.
func TestTagActorSnapshot_CapturesVolumes(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	snapshot := tag.GetStatus().GetSnapshot()

	var gotVolumes []string
	for _, snap := range snapshot.GetVolumeSnapshots() {
		gotVolumes = append(gotVolumes, snap.GetSourceVolumeName())
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetSourceVolumeName())
		}
		if got, want := snap.GetSourceVolumeId(), "vol-"+snap.GetSourceVolumeName(); got != want {
			t.Errorf("volume %q source volume id = %q, want %q", snap.GetSourceVolumeName(), got, want)
		}
		if got, want := snap.GetVolumeType(), testVolumeDriver; got != want {
			t.Errorf("volume %q snapshot driver = %q, want %q", snap.GetSourceVolumeName(), got, want)
		}
		if !snap.GetReadyToUse() {
			t.Errorf("volume %q snapshot ready_to_use = false, want the driver's true", snap.GetSourceVolumeName())
		}
	}
	if diff := cmp.Diff([]string{"data", "cache"}, gotVolumes); diff != "" {
		t.Errorf("captured volumes mismatch (-want +got):\n%s", diff)
	}
	if got, want := tag.GetStatus().GetState(), ateapipb.TagState_TAG_STATE_READY; got != want {
		t.Errorf("tag state = %v, want %v", got, want)
	}

	wantCreated := []string{
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "data"),
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "cache"),
	}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("snapshots taken mismatch (-want +got):\n%s", diff)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted = %v, want none on a successful create", got)
	}
}

// TestTagActorSnapshot_SnapshotNotReadyIsCaptured verifies a tag whose volume
// snapshot the driver is still processing is created in TAG_STATE_CAPTURED
// rather than TAG_STATE_READY: creation does not wait for the copy.
func TestTagActorSnapshot_SnapshotNotReadyIsCaptured(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.readyToUse = false
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	if got, want := tag.GetStatus().GetState(), ateapipb.TagState_TAG_STATE_CAPTURED; got != want {
		t.Errorf("tag state = %v, want %v", got, want)
	}
	if tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" {
		t.Error("tag snapshot uri is unset, want the captured tag finalized")
	}
}

// TestTagActorSnapshot_VolumesNotRequested verifies volumes are captured only
// when asked for: an actor with volumes tagged without including them produces
// a tag with no volume snapshot entries.
func TestTagActorSnapshot_VolumesNotRequested(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), false, nil)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	if got := tag.GetStatus().GetSnapshot().GetVolumeSnapshots(); len(got) != 0 {
		t.Errorf("volume snapshots = %v, want none", got)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none", got)
	}
}

// TestTagActorSnapshot_CapturesNamedVolumes verifies that naming volumes
// captures only those, in the actor's order, and leaves the rest off the tag.
func TestTagActorSnapshot_CapturesNamedVolumes(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache", "scratch")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, []string{"scratch", "data"})
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	var gotVolumes []string
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		gotVolumes = append(gotVolumes, snap.GetSourceVolumeName())
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetSourceVolumeName())
		}
	}
	if diff := cmp.Diff([]string{"data", "scratch"}, gotVolumes); diff != "" {
		t.Errorf("captured volumes mismatch (-want +got):\n%s", diff)
	}
	wantCreated := []string{
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "data"),
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "scratch"),
	}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("snapshots taken mismatch (-want +got):\n%s", diff)
	}
}

// TestTagActorSnapshot_UnknownVolumeName verifies a name the actor has no
// external volume for fails the call before the tag is reserved, so nothing is
// copied or snapshotted and the name stays free.
func TestTagActorSnapshot_UnknownVolumeName(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")
	before := objects.Objects()

	_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, []string{"data", "missing"})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("TagActorSnapshot error = %v, want InvalidArgument", err)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none", got)
	}
	if diff := cmp.Diff(before, objects.Objects()); diff != "" {
		t.Errorf("objects changed by the rejected create (-want +got):\n%s", diff)
	}
	if _, getErr := persistence.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "v1"}); !errors.Is(getErr, store.ErrNotFound) {
		t.Errorf("GetTag = %v, want ErrNotFound: the rejected create reserved its tag", getErr)
	}
}

// TestSelectTagVolumes covers which of the actor's volumes a tag captures.
func TestSelectTagVolumes(t *testing.T) {
	actor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.ActorStatus{ActorVolumes: []*ateapipb.ExternalVolume{
			{VolumeName: "data"}, {VolumeName: "cache"}, {VolumeName: "scratch"},
		}},
	}
	tests := []struct {
		name     string
		include  bool
		names    []string
		want     []string
		wantCode codes.Code
	}{
		{name: "not requested", include: false, want: nil},
		{name: "not requested ignores names", include: false, names: []string{"data"}, want: nil},
		{name: "all", include: true, want: []string{"data", "cache", "scratch"}},
		{name: "named keeps actor order", include: true, names: []string{"scratch", "data"}, want: []string{"data", "scratch"}},
		{name: "unknown name", include: true, names: []string{"data", "missing"}, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectTagVolumes(actor, tt.include, tt.names)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("selectTagVolumes() error = %v (code %v), want code %v", err, code, tt.wantCode)
			}
			var gotNames []string
			for _, vol := range got {
				gotNames = append(gotNames, vol.GetVolumeName())
			}
			if diff := cmp.Diff(tt.want, gotNames); diff != "" {
				t.Errorf("selected volumes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTagActorSnapshot_DriverCannotSnapshot verifies the capability check
// rejects the tag before any snapshot is attempted, rather than discovering the
// driver's limits partway through. The tag is kept, marked failed.
func TestTagActorSnapshot_DriverCannotSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.caps = volume.Capabilities{}
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")

	_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil)
	if apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("TagActorSnapshot error = %v, want FailedPrecondition", err)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none when the driver cannot snapshot", got)
	}
	stored, getErr := persistence.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "v1"})
	if getErr != nil {
		t.Fatalf("GetTag: %v", getErr)
	}
	if got, want := stored.GetStatus().GetState(), ateapipb.TagState_TAG_STATE_FAILED; got != want {
		t.Errorf("tag state = %v, want %v", got, want)
	}
}

// TestTagActorSnapshot_PartialVolumeFailureMarksFailed verifies capture is
// all-or-nothing: a driver that fails on one volume fails the create, which
// leaves the tag failed and unpublished. The volume that succeeded keeps its
// handle and the failed one has none. Deleting the tag then releases every
// snapshot and the copied objects, and frees the name for a retry.
func TestTagActorSnapshot_PartialVolumeFailureMarksFailed(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.failSnapshotOfVolume = "vol-cache"
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}
	before := objects.Objects()

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil); err == nil {
		t.Fatal("TagActorSnapshot succeeded, want the failed volume to fail the whole create")
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted by the failed create = %v, want none: cleanup is DeleteTag's", got)
	}

	stored, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if got, want := stored.GetStatus().GetState(), ateapipb.TagState_TAG_STATE_FAILED; got != want {
		t.Errorf("tag state = %v, want %v", got, want)
	}
	if got := stored.GetStatus().GetSnapshot().GetSnapshotUri(); got != "" {
		t.Errorf("tag was published with snapshot URI %q, want it unpublished", got)
	}
	uid := stored.GetMetadata().GetUid()
	handles := map[string]string{}
	for _, snap := range stored.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		handles[snap.GetSourceVolumeName()] = snap.GetStorageSnapshotId()
	}
	want := map[string]string{
		"data":  "snap-" + tagVolumeSnapshotID(uid, "data"),
		"cache": "",
	}
	if diff := cmp.Diff(want, handles); diff != "" {
		t.Errorf("recorded volume snapshot handles mismatch (-want +got):\n%s", diff)
	}

	// Once the driver recovers, deleting the tag releases the recorded
	// snapshot, and the one it re-requests for the entry without a handle.
	plugin.failSnapshotOfVolume = ""
	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	sortStrings := cmpopts.SortSlices(func(a, b string) bool { return a < b })
	wantDeleted := []string{want["data"], "snap-" + tagVolumeSnapshotID(uid, "cache")}
	if diff := cmp.Diff(wantDeleted, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(before, objects.Objects()); diff != "" {
		t.Errorf("objects after the delete mismatch, want the copy collected (-want +got):\n%s", diff)
	}

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil); err != nil {
		t.Fatalf("TagActorSnapshot retry: %v", err)
	}
}

// TestDeleteTag_ReleasesVolumeSnapshots verifies a delete collects the volume
// snapshots the tag owns, not just its object-storage prefix.
func TestDeleteTag_ReleasesVolumeSnapshots(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")
	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	created := plugin.snapshotsCreated()

	if _, err := w.DeleteTag(ctx, resources.TagRefFromTag(tag), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if diff := cmp.Diff(created, plugin.snapshotsDeleted()); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
}

// seedCreatingVolumeTag stores a tag the way a create that died midway leaves
// it: in TAG_STATE_CREATING, unpublished, with the given volume snapshot
// entries recorded.
func seedCreatingVolumeTag(t *testing.T, ctx context.Context, persistence store.Interface, entries ...*ateapipb.ExternalVolumeSnapshot) *ateapipb.Tag {
	t.Helper()
	return storetest.MustCreateTag(t, ctx, persistence, &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "pending"},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.TagStatus{
			StorageLocation: testStorageLocation,
			State:           ateapipb.TagState_TAG_STATE_CREATING,
			Snapshot:        &ateapipb.ExternalSnapshot{VolumeSnapshots: entries},
		},
	})
}

// TestDeleteTag_RecoversUnrecordedVolumeSnapshots verifies a tag left behind
// by a create that died still has every snapshot collected: recorded handles
// are released directly, and an entry without one has its handle recovered by
// repeating the create's idempotent CreateSnapshot against the recorded source
// volume.
func TestDeleteTag_RecoversUnrecordedVolumeSnapshots(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	plugin := newFakeSnapshotPlugin()
	w, _ := newVolumeTagWorkflow(persistence, plugin)

	pending := seedCreatingVolumeTag(t, ctx, persistence,
		&ateapipb.ExternalVolumeSnapshot{SourceVolumeName: "data", SourceVolumeId: "vol-data", StorageSnapshotId: "snap-stranded", VolumeType: testVolumeDriver},
		&ateapipb.ExternalVolumeSnapshot{SourceVolumeName: "cache", SourceVolumeId: "vol-cache", VolumeType: testVolumeDriver},
	)

	if _, err := w.DeleteTag(ctx, resources.TagRefFromTag(pending), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	recovered := "snap-" + tagVolumeSnapshotID(pending.GetMetadata().GetUid(), "cache")
	if diff := cmp.Diff([]string{recovered}, plugin.snapshotsCreated()); diff != "" {
		t.Errorf("snapshots re-requested mismatch (-want +got):\n%s", diff)
	}
	sortStrings := cmpopts.SortSlices(func(a, b string) bool { return a < b })
	if diff := cmp.Diff([]string{"snap-stranded", recovered}, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
}

// TestDeleteTag_SourceVolumeGone verifies a snapshot whose handle was never
// recorded and whose source volume is gone does not block the delete: it can
// no longer be found, so it is skipped and the tag is still removed.
func TestDeleteTag_SourceVolumeGone(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	plugin := newFakeSnapshotPlugin()
	plugin.goneVolumes = map[string]bool{"vol-cache": true}
	w, _ := newVolumeTagWorkflow(persistence, plugin)

	pending := seedCreatingVolumeTag(t, ctx, persistence,
		&ateapipb.ExternalVolumeSnapshot{SourceVolumeName: "cache", SourceVolumeId: "vol-cache", VolumeType: testVolumeDriver},
	)
	tagRef := resources.TagRefFromTag(pending)

	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted = %v, want none: nothing could be found", got)
	}
	if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetTag after the delete = %v, want ErrNotFound", err)
	}
}

// TestDeleteTag_RecoveryFailureKeepsTag verifies that when a handle cannot be
// recovered for a reason other than the source volume being gone, the delete
// fails and keeps the row, so a retry can still find the snapshot.
func TestDeleteTag_RecoveryFailureKeepsTag(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	plugin := newFakeSnapshotPlugin()
	plugin.failSnapshotOfVolume = "vol-cache"
	w, _ := newVolumeTagWorkflow(persistence, plugin)

	pending := seedCreatingVolumeTag(t, ctx, persistence,
		&ateapipb.ExternalVolumeSnapshot{SourceVolumeName: "cache", SourceVolumeId: "vol-cache", VolumeType: testVolumeDriver},
	)
	tagRef := resources.TagRefFromTag(pending)

	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err == nil {
		t.Fatal("DeleteTag succeeded, want the failed recovery to fail it")
	}
	stored, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag after the failed delete: %v", err)
	}
	if got, want := stored.GetStatus().GetState(), ateapipb.TagState_TAG_STATE_DELETING; got != want {
		t.Errorf("tag state = %v, want %v", got, want)
	}

	plugin.failSnapshotOfVolume = ""
	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("retried DeleteTag: %v", err)
	}
	want := []string{"snap-" + tagVolumeSnapshotID(pending.GetMetadata().GetUid(), "cache")}
	if diff := cmp.Diff(want, plugin.snapshotsDeleted()); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
}

// updateTagCountingStore wraps a store, counting UpdateTag calls and failing
// the one numbered failCall (1-based; 0 never fails).
type updateTagCountingStore struct {
	store.Interface
	failCall int

	mu    sync.Mutex
	calls int
}

func (s *updateTagCountingStore) UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(*ateapipb.Tag) error) (*ateapipb.Tag, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == s.failCall {
		return nil, fmt.Errorf("simulated UpdateTag failure on call %d", n)
	}
	return s.Interface.UpdateTag(ctx, tagRef, precondition, mutate)
}

func (s *updateTagCountingStore) updateTagCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestTagActorSnapshot_SnapshotsVolumesConcurrently verifies every volume's
// snapshot is in flight at once, and that the handles are recorded with a
// fixed number of tag writes however many volumes there are.
func TestTagActorSnapshot_SnapshotsVolumesConcurrently(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.rendezvous = 3
	counting := &updateTagCountingStore{Interface: persistence}
	w, objects := newVolumeTagWorkflow(counting, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache", "scratch")
	before := counting.updateTagCalls()

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetSourceVolumeName())
		}
	}
	// One write records the volumes, one records every handle, one finalizes.
	if got, want := counting.updateTagCalls()-before, 3; got != want {
		t.Errorf("UpdateTag calls = %d, want %d", got, want)
	}
}

// TestTagActorSnapshot_HandleWriteFailureIsRecoveredOnDelete verifies that
// when the single write recording the handles fails, the snapshots already
// taken are not released inline: the tag is marked failed with its entries
// still naming the source volumes, and deleting it recovers and releases them.
func TestTagActorSnapshot_HandleWriteFailureIsRecoveredOnDelete(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	counting := &updateTagCountingStore{Interface: persistence}
	w, objects := newVolumeTagWorkflow(counting, plugin)
	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}
	// The first UpdateTag records the volumes; the second records the handles.
	counting.failCall = counting.updateTagCalls() + 2

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true, nil); err == nil {
		t.Fatal("TagActorSnapshot succeeded, want the failed handle write to fail the create")
	}
	created := plugin.snapshotsCreated()
	if len(created) != 2 {
		t.Fatalf("snapshots taken = %v, want both volumes'", created)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted by the failed create = %v, want none: cleanup is DeleteTag's", got)
	}
	stored, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if got, want := stored.GetStatus().GetState(), ateapipb.TagState_TAG_STATE_FAILED; got != want {
		t.Errorf("tag state = %v, want %v", got, want)
	}

	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	sortStrings := cmpopts.SortSlices(func(a, b string) bool { return a < b })
	if diff := cmp.Diff(created, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("snapshots created but not deleted (-created +deleted):\n%s", diff)
	}
}

// TestValidateTagVolumeCompatibility covers the check that stops an Actor from
// being seeded from a tag that cannot fill its volumes.
func TestValidateTagVolumeCompatibility(t *testing.T) {
	externalVolumeTemplate := func(names ...string) *ateapipb.ActorTemplate {
		tmpl := &ateapipb.ActorTemplate{}
		for _, name := range names {
			tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{
				Name:                   name,
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{StorageClassName: "standard"},
			})
		}
		return tmpl
	}
	// tagWith builds a tag whose volume snapshots are named by the keys of
	// handles, each with the handle it maps to ("" for one that did not finish).
	tagWith := func(handles map[string]string) *ateapipb.Tag {
		snapshot := &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/tag"}
		for name, id := range handles {
			snapshot.VolumeSnapshots = append(snapshot.VolumeSnapshots, &ateapipb.ExternalVolumeSnapshot{SourceVolumeName: name, StorageSnapshotId: id})
		}
		return &ateapipb.Tag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "v1"},
			Status:   &ateapipb.TagStatus{Snapshot: snapshot},
		}
	}

	tests := []struct {
		name     string
		tag      *ateapipb.Tag
		template *ateapipb.ActorTemplate
		wantCode codes.Code
	}{
		{
			name:     "template declares no external volumes",
			tag:      tagWith(nil),
			template: &ateapipb.ActorTemplate{Volumes: []*ateapipb.Volume{{Name: "scratch"}}},
			wantCode: codes.OK,
		},
		{
			name:     "every volume captured",
			tag:      tagWith(map[string]string{"data": "snap-data", "cache": "snap-cache"}),
			template: externalVolumeTemplate("data", "cache"),
			wantCode: codes.OK,
		},
		{
			// Volumes the tag did not capture are provisioned empty.
			name:     "tag captured no volumes",
			tag:      tagWith(nil),
			template: externalVolumeTemplate("data"),
			wantCode: codes.OK,
		},
		{
			name:     "volume snapshot did not finish",
			tag:      tagWith(map[string]string{"data": "snap-data", "cache": ""}),
			template: externalVolumeTemplate("data", "cache"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "tag captured a subset of volumes",
			tag:      tagWith(map[string]string{"data": "snap-data"}),
			template: externalVolumeTemplate("data", "cache"),
			wantCode: codes.OK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTagVolumeCompatibility(tt.tag, tt.template)
			if got := apierror.Code(err); got != tt.wantCode {
				t.Errorf("validateTagVolumeCompatibility() = %v (code %v), want code %v", err, got, tt.wantCode)
			}
		})
	}
}

// TestResolveVolumeSource covers picking the snapshot a restored volume is
// seeded from, and the readiness check deferred here from tag creation.
func TestResolveVolumeSource(t *testing.T) {
	snapshots := []*ateapipb.ExternalVolumeSnapshot{
		{SourceVolumeName: "data", StorageSnapshotId: "snap-data", VolumeType: testVolumeDriver, ReadyToUse: true},
	}

	t.Run("volume with no snapshot is provisioned empty", func(t *testing.T) {
		got, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), snapshots, "cache", testVolumeDriver)
		if err != nil {
			t.Fatalf("resolveVolumeSource: %v", err)
		}
		if got != "" {
			t.Errorf("source snapshot = %q, want empty", got)
		}
	})

	t.Run("volume with a snapshot is restored from it", func(t *testing.T) {
		got, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), snapshots, "data", testVolumeDriver)
		if err != nil {
			t.Fatalf("resolveVolumeSource: %v", err)
		}
		if want := "snap-data"; got != want {
			t.Errorf("source snapshot = %q, want %q", got, want)
		}
	})

	t.Run("volume whose snapshot did not finish is rejected", func(t *testing.T) {
		unfinished := []*ateapipb.ExternalVolumeSnapshot{{SourceVolumeName: "data", VolumeType: testVolumeDriver}}
		_, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), unfinished, "data", testVolumeDriver)
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})

	t.Run("snapshot from another driver is rejected", func(t *testing.T) {
		_, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), snapshots, "data", "substrate.io/other")
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})

	t.Run("snapshot still copying is rejected", func(t *testing.T) {
		plugin := newFakeSnapshotPlugin()
		plugin.readyToUse = false
		_, err := resolveVolumeSource(context.Background(), plugin, snapshots, "data", testVolumeDriver)
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})

	t.Run("snapshot deleted behind our back is rejected", func(t *testing.T) {
		plugin := newFakeSnapshotPlugin()
		plugin.missingSnapshots = map[string]bool{"snap-data": true}
		_, err := resolveVolumeSource(context.Background(), plugin, snapshots, "data", testVolumeDriver)
		if apierror.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})
}
