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
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TagActorSnapshot tags the external snapshot held by the suspended actor the
// tag's source_actor names.
// The tag is given its own copy of that snapshot, so suspending the actor again
// or deleting the actor does not garbage collect the tag's snapshot.
//
// The tag is built in 3 phases:
//  1. Reserve the tag in TAG_STATE_CREATING and record its storage location.
//  2. Copy the snapshot under the reserved tag's UID.
//  3. Finalize: write the completed snapshot object to the tag and move it to
//     TAG_STATE_READY.
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
func (w *ActorWorkflow) TagActorSnapshot(ctx context.Context, tag *ateapipb.Tag) (_ *ateapipb.Tag, err error) {
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
	return w.ensureTagFinalized(leaseCtx, reserved, snapshot, dst)
}

// DeleteTag releases the external snapshot the tag owns and then removes the
// row, in that order: the row is the only handle on that snapshot, so dropping
// it first would leak.
//
// The workflow is built in 4 phases:
//  1. Load the tag (which names the snapshot to collect).
//  2. Move it to TAG_STATE_DELETING, so readers see it is going away.
//  3. Release that snapshot, tolerating a previous attempt partly collected.
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

// ensureTagFinalized publishes the copy by setting status.snapshot and moves
// the tag from TAG_STATE_CREATING to TAG_STATE_READY. Until this lands the tag
// is unusable; deleting it collects any partial copy.
func (w *ActorWorkflow) ensureTagFinalized(ctx context.Context, tag *ateapipb.Tag, snapshot *ateapipb.ExternalSnapshot, dst resources.SnapshotURI) (_ *ateapipb.Tag, err error) {
	ctx, done := stepSpan(ctx, "FinalizeTag")
	defer func() { err = done(err) }()

	tagRef := resources.TagRefFromTag(tag)
	// The copy is byte-identical to the source, so it carries the same content.
	finalSnapshot := &ateapipb.ExternalSnapshot{
		SnapshotUri:  dst.String(),
		ContentScope: snapshot.GetContentScope(),
	}
	stored, err := w.store.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapipb.Tag) error {
		toUpdate.Status.Snapshot = finalSnapshot
		toUpdate.Status.State = ateapipb.TagState_TAG_STATE_READY
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
