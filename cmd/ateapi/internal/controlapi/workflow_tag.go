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
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 4 phases:
//  1. Reserve the tag in TAG_STATE_CREATING and record its storage location.
//  2. Copy the snapshot under the reserved tag's UID.
//  3. Snapshot the requested external volumes: none, the named ones, or all.
//  4. Finalize: write the completed snapshot object to the tag and move it to
//     TAG_STATE_READY, or TAG_STATE_CAPTURED while a volume snapshot is not
//     yet ready to use.
//
// The tag captures whichever snapshot the actor holds when the workflow runs.
// An actor keeps no snapshot history, so a suspend that lands first moves what
// gets tagged; that race is inherent to naming an actor rather than a snapshot.
//
// Not idempotent: the name is taken as soon as phase 1 lands. If a later phase
// fails, the tag is moved to TAG_STATE_FAILED and kept, along with whatever the
// failed phases left behind. A create whose process dies leaves the tag in
// TAG_STATE_CREATING. Either way every later create under that name is
// AlreadyExists; to retry, delete the tag, which collects what it stranded,
// and create it again.
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapipb.Tag, includeExternalVolumes bool, volumeNames []string) (_ *ateapipb.Tag, err error) {
	actorRef := resources.ActorRefFromObjectRef(tag.GetSourceActor())

	// Serializes against a suspend of the same actor, which would otherwise
	// collect the snapshot out from under the copy.
	leaseCtx, lease, err := w.acquireActorLease(ctx, actorRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	// Serializes against a delete of the tag this creates, which would
	// otherwise collect the copy while it is being written.
	tagRef := resources.TagRef{Atespace: actorRef.Atespace, Name: tag.GetMetadata().GetName()}
	leaseCtx, tagLease, err := acquireTagLease(leaseCtx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer tagLease.Close()

	actor, actorTemplate, err := w.loadActorForTag(leaseCtx, actorRef)
	if err != nil {
		return nil, err
	}
	snapshot := actor.GetStatus().GetExternalSnapshot()
	// Resolved before the tag is reserved, so a name the actor does not have
	// fails the call before anything is copied.
	volumes, err := selectTagVolumes(actor, includeExternalVolumes, volumeNames)
	if err != nil {
		return nil, err
	}

	reserved, err := w.ensureTagReserved(leaseCtx, tagRef, actor, actorTemplate, tag)
	if err != nil {
		return nil, err
	}
	// From here on the row exists, so a failure marks it failed. The deferred
	// call runs before the leases are released.
	defer func() {
		if err != nil {
			w.markTagFailed(leaseCtx, reserved)
		}
	}()

	dst, err := resources.NewTagSnapshotURI(reserved.GetStatus().GetStorageLocation(), tagRef.Atespace, reserved.GetMetadata().GetUid())
	if err != nil {
		return nil, fmt.Errorf("while building the snapshot URI for tag %s: %w", tagRef, err)
	}
	if err := w.ensureTagSnapshotCopied(leaseCtx, reserved, snapshot, dst); err != nil {
		return nil, err
	}
	snapshotted, err := w.ensureTagVolumesSnapshotted(leaseCtx, reserved, actor, volumes)
	if err != nil {
		return nil, err
	}
	return w.ensureTagFinalized(leaseCtx, snapshotted, snapshot, dst)
}

// selectTagVolumes returns the source actor's external volumes a tag captures:
// none unless includeExternalVolumes is set, then the ones named, or all of
// them when names is empty. The result keeps the actor's volume order.
//
// All volumes means status.actor_volumes rather than the mounted subset: a
// template may declare a volume no container mounts, and leaving it out would
// produce a tag missing a volume.
func selectTagVolumes(actor *ateapipb.Actor, includeExternalVolumes bool, names []string) ([]*ateapipb.ExternalVolume, error) {
	if !includeExternalVolumes {
		return nil, nil
	}
	volumes := actor.GetStatus().GetActorVolumes()
	if len(names) == 0 {
		return volumes, nil
	}
	requested := make(map[string]bool, len(names))
	for _, name := range names {
		requested[name] = true
	}
	var selected []*ateapipb.ExternalVolume
	for _, vol := range volumes {
		if requested[vol.GetVolumeName()] {
			selected = append(selected, vol)
			delete(requested, vol.GetVolumeName())
		}
	}
	if len(requested) > 0 {
		var unknown []string
		for _, name := range names {
			if requested[name] {
				unknown = append(unknown, name)
			}
		}
		return nil, apierror.InvalidArgument("Actor %s has no external volumes named %q", resources.ActorRefFromActor(actor), unknown)
	}
	return selected, nil
}

// DeleteTag releases the external snapshot and volume snapshots the tag owns
// and then removes the row, in that order: the row is the only handle on them,
// so dropping it first would leak.
//
// The workflow is built in 4 phases:
//  1. Load the tag (which names what to collect).
//  2. Move it to TAG_STATE_DELETING, so readers see it is going away.
//  3. Release the snapshot and volume snapshots, tolerating a previous attempt
//     partly collected and recovering volume snapshot handles a failed create
//     never recorded.
//  4. Finalize: drop the row.
//
// It cleans up a tag in any state, including one a failed or crashed create
// left in TAG_STATE_FAILED or TAG_STATE_CREATING.
//
// Idempotent: a failure at any phase leaves the row in place, so the same
// delete run again rediscovers the work from it and resumes over whatever is
// left.
//
// The tag stays resolvable while its snapshot is being collected, so a
// CreateActor racing this delete can seed an Actor from content that is going
// away. That race is accepted for now.
//
// Note that this destroys the external snapshot: an Actor created from the tag
// and never suspended is still borrowing it and becomes unrecoverable. Do not
// delete a tag while clones of it exist.
func (w *ActorWorkflow) DeleteTag(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (*ateapipb.Tag, error) {
	// Serializes against a create of the same tag, whose copy would otherwise
	// keep writing into the prefix this is collecting.
	ctx, lease, err := acquireTagLease(ctx, w.store, tagRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	tag, err := w.loadTagForDelete(ctx, tagRef)
	if err != nil {
		return nil, err
	}
	// Checked before the snapshot is collected: a stale caller must not
	// reach that step.
	if err := precondition.Check(tag.GetMetadata()); err != nil {
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		return nil, apierror.Aborted("concurrent update conflict, please retry")
	}
	tag, err = w.ensureTagDeleting(ctx, tag)
	if err != nil {
		return nil, err
	}
	if err := w.ensureTagSnapshotReleased(ctx, tag); err != nil {
		return nil, err
	}
	if err := w.ensureTagVolumeSnapshotsReleased(ctx, tag); err != nil {
		return nil, err
	}
	// The caller's precondition was checked against the row before it was
	// marked; marking it moved its version on.
	return w.finalizeTagDeleted(ctx, tagRef, store.DeletePreconditions{UID: tag.GetMetadata().GetUid(), Version: tag.GetMetadata().GetVersion()})
}

// ensureTagDeleting moves the tag to TAG_STATE_DELETING, which is terminal.
func (w *ActorWorkflow) ensureTagDeleting(ctx context.Context, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "MarkTagDeleting")
	defer func() { err = done(err) }()

	if tag.GetStatus().GetState() == ateapipb.TagState_TAG_STATE_DELETING {
		markSkipped(ctx, "tag is already being deleted")
		return tag, nil
	}
	tagRef := resources.TagRefFromTag(tag)
	updated, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		toUpdate.Status.State = ateapipb.TagState_TAG_STATE_DELETING
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) || errors.Is(err, store.ErrUIDConflict) || errors.Is(err, store.ErrNotFound) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while marking tag %s deleting: %w", tagRef, err)
	}
	return updated, nil
}

// loadTagForDelete fetches the row the delete works from. The row records where
// the snapshot lives, so the work is rediscovered from it rather than rebuilt
// from the source actor, which may be long gone.
func (w *ActorWorkflow) loadTagForDelete(ctx context.Context, tagRef resources.TagRef) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "LoadTagForDelete")
	defer func() { err = done(err) }()

	tag, err := w.store.GetTag(ctx, tagRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Tag %s not found", tagRef)
		}
		return nil, fmt.Errorf("while getting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// ensureTagSnapshotReleased deletes the objects the tag's external snapshot is
// made of. It tolerates a partly-collected snapshot, so a retry finishes
// cleanly. It collects the in-progress snapshot of a pending tag too.
func (w *ActorWorkflow) ensureTagSnapshotReleased(ctx context.Context, tag *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStore == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	uri, err := resources.NewTagSnapshotURI(tag.GetStatus().GetStorageLocation(), tagRef.Atespace, tag.GetMetadata().GetUid())
	if err != nil {
		return fmt.Errorf("while resolving the external snapshot of tag %s: %w", tagRef, err)
	}
	if err := objectstore.DeletePrefix(ctx, w.objectStore, uri.Prefix()); err != nil {
		return fmt.Errorf("while releasing the external snapshot %q of tag %s: %w", uri, tagRef, err)
	}
	return nil
}

// ensureTagVolumeSnapshotsReleased deletes the volume snapshots the tag owns.
//
// status.snapshot.volume_snapshots names them whether the tag is finalized or
// was left behind by a failed create, so a failed create's snapshots are
// collected too. An entry without a handle may still name a snapshot that was
// cut but never recorded; its handle is recovered first (see
// recoverVolumeSnapshotHandle). The row is dropped only after this succeeds, so
// a partial failure leaves every handle reachable for a retry.
func (w *ActorWorkflow) ensureTagVolumeSnapshotsReleased(ctx context.Context, tag *ateapipb.Tag) (err error) {
	ctx, done := stepSpan(ctx, "ReleaseTagVolumeSnapshots")
	defer func() { err = done(err) }()

	snapshots := tag.GetStatus().GetSnapshot().GetVolumeSnapshots()
	if len(snapshots) == 0 {
		markSkipped(ctx, "tag holds no volume snapshots")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	var errs []error
	toRelease := make([]*ateapipb.ExternalVolumeSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		if snap.GetStorageSnapshotId() != "" {
			toRelease = append(toRelease, snap)
			continue
		}
		recovered, err := w.recoverVolumeSnapshotHandle(ctx, tag.GetMetadata().GetUid(), snap)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if recovered != nil {
			toRelease = append(toRelease, recovered)
		}
	}
	if err := w.releaseVolumeSnapshots(ctx, toRelease); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("while releasing the volume snapshots of tag %s: %w", tagRef, err)
	}
	return nil
}

// recoverVolumeSnapshotHandle finds the handle of a volume snapshot the tag
// records without one. Such an entry names either a snapshot that failed or one
// the storage system cut whose handle a failed or crashed create never wrote
// back. CreateSnapshot is idempotent on the snapshot name and source volume, so
// issuing the create's request again returns the handle of a snapshot that
// exists. When none does, this cuts one that the caller then deletes.
//
// The storage system cannot look a snapshot up by name alone, so once the
// source volume is gone a snapshot cut from it can no longer be found. That
// snapshot is leaked: it is logged and skipped, returning nil, so the tag can
// still be deleted.
func (w *ActorWorkflow) recoverVolumeSnapshotHandle(ctx context.Context, tagUID string, snap *ateapipb.ExternalVolumeSnapshot) (*ateapipb.ExternalVolumeSnapshot, error) {
	volName := snap.GetSourceVolumeName()
	plugin, err := w.pluginRegistry.GetPlugin(ctx, snap.GetVolumeType())
	if err != nil {
		return nil, fmt.Errorf("while getting volume plugin for driver %q: %w", snap.GetVolumeType(), err)
	}
	name := tagVolumeSnapshotID(tagUID, volName)
	got, err := plugin.CreateSnapshot(ctx, volume.CreateSnapshotRequest{
		Name:           name,
		SourceVolumeID: snap.GetSourceVolumeId(),
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "source volume is gone, so a volume snapshot the tag never recorded cannot be found; it may be leaked",
				slog.String("snapshot_name", name), slog.String("source_volume_id", snap.GetSourceVolumeId()), slog.Any("error", err))
			return nil, nil
		}
		return nil, fmt.Errorf("while recovering the snapshot of volume %q: %w", volName, err)
	}
	if got.SnapshotID == "" {
		return nil, fmt.Errorf("driver %q returned no snapshot handle while recovering the snapshot of volume %q", snap.GetVolumeType(), volName)
	}
	return &ateapipb.ExternalVolumeSnapshot{
		SourceVolumeName:  volName,
		SourceVolumeId:    snap.GetSourceVolumeId(),
		StorageSnapshotId: got.SnapshotID,
		VolumeType:        snap.GetVolumeType(),
	}, nil
}

// finalizeTagDeleted drops the row, once nothing it names is left behind.
func (w *ActorWorkflow) finalizeTagDeleted(ctx context.Context, tagRef resources.TagRef, precondition store.DeletePreconditions) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTagDeleted")
	defer func() { err = done(err) }()

	tag, err := w.store.DeleteTag(ctx, tagRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("Tag %s not found", tagRef)
		}
		if errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s does not have uid %s", tagRef, precondition.UID)
		}
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		return nil, fmt.Errorf("while deleting tag %s: %w", tagRef, err)
	}
	return tag, nil
}

// markTagFailedTimeout bounds the write that marks a failed create's tag after
// the caller's context may be gone.
const markTagFailedTimeout = 30 * time.Second

// markTagFailed moves a tag whose create failed to TAG_STATE_FAILED. It is
// best effort: the create is already failing, so an error here is logged and
// the tag is left in TAG_STATE_CREATING, which DeleteTag cleans up the same
// way.
//
// It keeps ctx's values but not its cancellation, so a create that failed
// because its RPC was canceled still records the failure. The row is read
// again rather than taken from the caller, since earlier phases moved its
// version on; a row that is gone or carries another UID is left alone.
func (w *ActorWorkflow) markTagFailed(ctx context.Context, reserved *ateapipb.Tag) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markTagFailedTimeout)
	defer cancel()
	ctx, done := stepSpan(ctx, "MarkTagFailed")

	tagRef := resources.TagRefFromTag(reserved)
	err := func() error {
		tag, err := w.store.GetTag(ctx, tagRef)
		if err != nil {
			return fmt.Errorf("while getting tag %s: %w", tagRef, err)
		}
		if tag.GetMetadata().GetUid() != reserved.GetMetadata().GetUid() {
			return nil
		}
		_, err = w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
			toUpdate.Status.State = ateapipb.TagState_TAG_STATE_FAILED
			return nil
		})
		return err
	}()
	if err = done(err); err != nil {
		slog.ErrorContext(ctx, "failed to mark a failed tag create; the tag is left creating", slog.String("tag", tagRef.String()), slog.Any("error", err))
	}
}

// loadActorForTag fetches the actor to tag and its template, and checks that
// the actor holds an external snapshot a tag can be made from.
func (w *ActorWorkflow) loadActorForTag(ctx context.Context, actorRef resources.ActorRef) (_ *ateapipb.Actor, _ *ateapipb.ActorTemplate, err error) {
	ctx, done := stepSpan(ctx, "LoadActorForTag")
	defer func() { err = done(err) }()

	actor, err := w.store.GetActor(ctx, actorRef)
	if err != nil {
		return nil, nil, err
	}
	// Only a suspended actor's snapshot is complete. A running or
	// suspending actor's is either stale or still being written.
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		return nil, nil, apierror.FailedPrecondition("Actor %s must be %s to be tagged (got: %v)", actorRef, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, got)
	}
	snapshotURI := actor.GetStatus().GetExternalSnapshot().GetSnapshotUri()
	if snapshotURI == "" {
		return nil, nil, apierror.FailedPrecondition("Actor %s holds no external snapshot to tag", actorRef)
	}
	// Every way an Actor comes to hold an external snapshot records the
	// template its guest state was built under: a suspend through
	// ensureSuspendedFinalized, a create from a tag through the tag's own UID.
	// A snapshot without one is a broken row, and tagging it would mint a tag
	// that names no template.
	if actor.GetStatus().GetExternalSnapshot().GetActorTemplateUid() == "" {
		return nil, nil, apierror.Internal("Actor %s holds an external snapshot but records no template it was built under", actorRef)
	}
	actorTemplate, err := resolveActorTemplate(ctx, w.store, actor)
	if err != nil {
		return nil, nil, err
	}
	return actor, actorTemplate, nil
}

// ensureTagReserved takes the tag's name in TAG_STATE_CREATING and records the
// storage location.
//
// A name already taken is AlreadyExists, whatever state the tag holding it is
// in, including one a failed or crashed create left behind. Resuming such a
// row would mean deciding whether the objects under it still belong to the
// snapshot being tagged, and the row may not even be this actor's; deleting
// the tag collects them and frees the name, so a retry is a delete followed by
// a create.
func (w *ActorWorkflow) ensureTagReserved(ctx context.Context, tagRef resources.TagRef, actor *ateapipb.Actor, actorTemplate *ateapipb.ActorTemplate, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "ReserveTag")
	defer func() { err = done(err) }()

	location := actorTemplate.GetSnapshotConfig().GetStorageLocation()
	if err := resources.ValidateSnapshotLocation(location); err != nil {
		return nil, fmt.Errorf("invalid storage location for tag %s: %w", tagRef, err)
	}
	tagToCreate := &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:       tag.GetScope(),
		SourceActor: resources.ActorRefFromActor(actor).ToObjectRef(),
		Status: &ateapipb.TagStatus{
			// The tag records the template the snapshot's guest state was built under, not
			// the one the actor currently points at. A suspended actor can be repointed,
			// and a tag that claimed the new template would hand clones the old template's
			// memory under the new one's identity, past the data-only downgrade a resume of
			// the actor itself would take.
			ActorTemplateUid: actor.GetStatus().GetExternalSnapshot().GetActorTemplateUid(),
			StorageLocation:  location,
			State:            ateapipb.TagState_TAG_STATE_CREATING,
		},
	}

	stored, err := w.store.CreateTag(ctx, tagToCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("Atespace %s not found", tagRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, apierror.AlreadyExists("Tag %s already exists; delete it and create it again to retry", tagRef)
	}
	return nil, fmt.Errorf("while reserving tag %s: %w", tagRef, err)
}

// ensureTagSnapshotCopied copies the actor's external snapshot to the tag's own
// prefix, derived from the reserved row's freshly minted UID. The prefix is
// empty by construction, so the copy never blends with another attempt's objects.
func (w *ActorWorkflow) ensureTagSnapshotCopied(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.ExternalSnapshot, dst resources.SnapshotURI) (err error) {
	ctx, done := stepSpan(ctx, "CopyTagSnapshot")
	defer func() { err = done(err) }()

	if w.objectStore == nil {
		markSkipped(ctx, "no object store configured")
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	src, err := resources.ParseSnapshotURI(snapshot.GetSnapshotUri())
	if err != nil {
		return fmt.Errorf("while parsing the external snapshot %q of the source actor: %w", snapshot.GetSnapshotUri(), err)
	}
	if err := objectstore.CopyPrefix(ctx, w.objectStore, src.Prefix(), dst.Prefix()); err != nil {
		return fmt.Errorf("while copying the external snapshot for tag %s: %w", tagRef, err)
	}
	return nil
}

// ensureTagVolumesSnapshotted captures the given external volumes of the
// source actor, as picked by selectTagVolumes, and records them on the pending
// tag's status.snapshot.volume_snapshots. Volumes left out are not recorded.
//
// Every volume is checked before any is recorded or snapshotted, so a volume
// that cannot be captured fails the call without touching the storage system.
// The full list of volumes to capture is then recorded, each entry with its
// source volume ID and an empty storage_snapshot_id. The snapshots are taken
// concurrently, and every handle obtained is filled in with a single write
// once all of them return, so the store sees two writes however many volumes
// there are.
//
// An entry left without a handle names a snapshot that failed, or one that was
// cut but whose handle was never recorded because the create died or the write
// failed. Deleting the tag recovers the latter from the entry's source volume
// ID, so nothing is released here.
//
// Capture is all-or-nothing: a clone that silently came up with one empty disk
// would be worse than a failed create. Any failure fails the create, which
// marks the tag failed. The handles of volumes that did succeed are recorded
// before the failure is returned.
//
// It does not wait for the storage system to finish copying. A driver may
// return a handle with ready_to_use false and finish in the background, and
// blocking here would put an unbounded storage operation inside a synchronous
// RPC. Readiness is checked instead when an Actor seeded from the tag
// provisions its volumes, which is the first point the data is actually needed.
//
// No quiesce step is required. A SUSPENDED actor has already had its volumes
// unmounted by atelet and detached by the control plane, so the filesystem is
// cleanly unmounted with no dirty page cache, and the actor lease held by the
// caller keeps a concurrent resume from re-attaching them mid-capture.
func (w *ActorWorkflow) ensureTagVolumesSnapshotted(ctx context.Context, tag *ateapipb.Tag, actor *ateapipb.Actor, volumes []*ateapipb.ExternalVolume) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "SnapshotVolumes")
	defer func() { err = done(err) }()

	if len(volumes) == 0 {
		markSkipped(ctx, "no external volumes to capture")
		return tag, nil
	}

	plugins, err := w.tagVolumeSnapshotPlugins(ctx, actor, volumes)
	if err != nil {
		return nil, err
	}

	tagRef := resources.TagRefFromTag(tag)
	tag, err = w.recordTagVolumeSnapshots(ctx, tag, volumes)
	if err != nil {
		return nil, err
	}

	// Siblings are not canceled when one fails: a canceled call may still
	// have cut a snapshot whose handle would never come back, so each runs to
	// completion and reports its result.
	snaps := make([]volume.Snapshot, len(volumes))
	snapErrs := make([]error, len(volumes))
	var wg sync.WaitGroup
	for i, vol := range volumes {
		wg.Go(func() {
			snaps[i], snapErrs[i] = createTagVolumeSnapshot(ctx, plugins[i], tag.GetMetadata().GetUid(), vol)
		})
	}
	wg.Wait()

	if slices.ContainsFunc(snapErrs, func(err error) bool { return err == nil }) {
		updated, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
			entries := toUpdate.GetStatus().GetSnapshot().GetVolumeSnapshots()
			for i, vol := range volumes {
				if snapErrs[i] != nil {
					continue
				}
				volName := vol.GetVolumeName()
				if i >= len(entries) || entries[i].GetSourceVolumeName() != volName {
					return fmt.Errorf("tag %s does not record volume %q at index %d", tagRef, volName, i)
				}
				snap := snaps[i]
				entry := entries[i]
				entry.StorageSnapshotId = snap.SnapshotID
				entry.ReadyToUse = snap.ReadyToUse
				entry.SizeBytes = snap.SizeBytes
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("while recording the volume snapshots on tag %s: %w", tagRef, err)
		}
		tag = updated
	}
	if err := errors.Join(snapErrs...); err != nil {
		return nil, apierror.Internal("%v", err)
	}
	return tag, nil
}

// createTagVolumeSnapshot snapshots one volume under the name the tag derives
// for it, failing if the driver returns no handle.
func createTagVolumeSnapshot(ctx context.Context, plugin volume.VolumePluginControlPlane, tagUID string, vol *ateapipb.ExternalVolume) (volume.Snapshot, error) {
	volName := vol.GetVolumeName()
	// CSI CreateSnapshot is idempotent on (name, source volume), so a retry
	// returns the snapshot the previous attempt made; DeleteTag relies on this
	// to recover handles that were never recorded. The tag UID keeps two
	// attempts under the same tag name apart.
	snap, err := plugin.CreateSnapshot(ctx, volume.CreateSnapshotRequest{
		Name:           tagVolumeSnapshotID(tagUID, volName),
		SourceVolumeID: vol.GetStorageVolumeId(),
	})
	if err != nil {
		return volume.Snapshot{}, fmt.Errorf("failed to snapshot volume %q: %w", volName, err)
	}
	if snap.SnapshotID == "" {
		return volume.Snapshot{}, fmt.Errorf("driver %q returned no snapshot handle for volume %q", vol.GetVolumeType(), volName)
	}
	return snap, nil
}

// tagVolumeSnapshotPlugins checks that every volume can be snapshotted and
// returns the plugin to snapshot each one with, indexed like volumes.
func (w *ActorWorkflow) tagVolumeSnapshotPlugins(ctx context.Context, actor *ateapipb.Actor, volumes []*ateapipb.ExternalVolume) ([]volume.VolumePluginControlPlane, error) {
	plugins := make([]volume.VolumePluginControlPlane, 0, len(volumes))
	for _, vol := range volumes {
		volName := vol.GetVolumeName()
		if vol.GetStatus() != ateapipb.ExternalVolume_STATUS_CREATED || vol.GetStorageVolumeId() == "" {
			return nil, apierror.FailedPrecondition("cannot snapshot volume %q of Actor %s: it has not been provisioned", volName, resources.ActorRefFromActor(actor))
		}
		plugin, err := w.pluginRegistry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return nil, apierror.FailedPrecondition("failed to get volume plugin for driver %q: %v", vol.GetVolumeType(), err)
		}
		caps, err := plugin.ControllerCapabilities(ctx)
		if err != nil {
			return nil, apierror.FailedPrecondition("failed to read capabilities of driver %q for volume %q: %v", vol.GetVolumeType(), volName, err)
		}
		if !caps.CreateDeleteSnapshot {
			return nil, apierror.FailedPrecondition("volume %q uses driver %q, which does not support snapshots", volName, vol.GetVolumeType())
		}
		plugins = append(plugins, plugin)
	}
	return plugins, nil
}

// recordTagVolumeSnapshots records on the pending tag the volumes it is about
// to capture, each with its source volume ID and without a handle yet.
// Recording them before any snapshot is taken is what lets a reader tell "not
// requested" (no entry) from "requested and not finished" (an entry without a
// handle), and what lets DeleteTag recover a handle that never got recorded.
func (w *ActorWorkflow) recordTagVolumeSnapshots(ctx context.Context, tag *ateapipb.Tag, volumes []*ateapipb.ExternalVolume) (*ateapipb.Tag, error) {
	tagRef := resources.TagRefFromTag(tag)
	entries := make([]*ateapipb.ExternalVolumeSnapshot, 0, len(volumes))
	for _, vol := range volumes {
		entries = append(entries, &ateapipb.ExternalVolumeSnapshot{
			SourceVolumeName: vol.GetVolumeName(),
			SourceVolumeId:   vol.GetStorageVolumeId(),
			VolumeType:       vol.GetVolumeType(),
		})
	}
	updated, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		if toUpdate.GetStatus().GetSnapshot() != nil {
			return fmt.Errorf("tag %s already records a snapshot", tagRef)
		}
		toUpdate.Status.Snapshot = &ateapipb.ExternalSnapshot{VolumeSnapshots: entries}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("while recording the volumes to snapshot on tag %s: %w", tagRef, err)
	}
	return updated, nil
}

// releaseVolumeSnapshots deletes a set of volume snapshots, tolerating ones
// already gone and joining the failures so one bad handle does not strand the
// rest. Entries without a handle are skipped.
func (w *ActorWorkflow) releaseVolumeSnapshots(ctx context.Context, snapshots []*ateapipb.ExternalVolumeSnapshot) error {
	var errs []error
	for _, snap := range snapshots {
		if snap.GetStorageSnapshotId() == "" {
			continue
		}
		plugin, err := w.pluginRegistry.GetPlugin(ctx, snap.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("while getting volume plugin for driver %q: %w", snap.GetVolumeType(), err))
			continue
		}
		if err := plugin.DeleteSnapshot(ctx, snap.GetStorageSnapshotId()); err != nil {
			errs = append(errs, fmt.Errorf("while deleting volume snapshot %q: %w", snap.GetStorageSnapshotId(), err))
		}
	}
	return errors.Join(errs...)
}

// ensureTagFinalized publishes the copy by setting status.snapshot.snapshot_uri
// and moves the tag out of TAG_STATE_CREATING: to TAG_STATE_READY when every
// volume snapshot was ready to use, or TAG_STATE_CAPTURED while one is still
// being processed. Until this lands the tag is unusable; deleting it collects
// any partial copy.
//
// The volume snapshots recorded by ensureTagVolumesSnapshotted are kept as is.
// status.snapshot is immutable once snapshot_uri is set, so a tag can never
// come to name a different set of volumes than the one it published.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.ExternalSnapshot, dst resources.SnapshotURI) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTag")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		if toUpdate.Status.Snapshot == nil {
			toUpdate.Status.Snapshot = &ateapipb.ExternalSnapshot{}
		}
		toUpdate.Status.Snapshot.SnapshotUri = dst.String()
		// The copy is byte-identical to the source, so it carries the same
		// content.
		toUpdate.Status.Snapshot.ContentScope = snapshot.GetContentScope()
		toUpdate.Status.State = finalizedTagState(toUpdate.Status.Snapshot.GetVolumeSnapshots())
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("Tag %s was deleted while it was being created, please retry", tagRef)
		}
		return nil, fmt.Errorf("while finalizing tag %s: %w", tagRef, err)
	}
	return stored, nil
}

// finalizedTagState is the state a tag whose creation completed starts in:
// TAG_STATE_CAPTURED while any of its volume snapshots was not yet ready to
// use, otherwise TAG_STATE_READY.
func finalizedTagState(volumeSnapshots []*ateapipb.ExternalVolumeSnapshot) ateapipb.TagState {
	for _, snap := range volumeSnapshots {
		if !snap.GetReadyToUse() {
			return ateapipb.TagState_TAG_STATE_CAPTURED
		}
	}
	return ateapipb.TagState_TAG_STATE_READY
}
