//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSQLiteFirstBindingUnlinkRetainsCaptureAndHistoricalReplay(t *testing.T) {
	q := bmQANew(t)
	bmQACapture(t, q, "finished")
	before := bmQARead(t, q)
	in := UnlinkInput{BindingID: q.linked.Binding.ID, IfRevision: "1", RequestID: bmQAID(101), Confirmed: true}
	got, err := q.service().Unlink(q.ctx, in)
	revision := "2"
	want := MutationResult{ContractVersion: 1, SnapshotRevision: bump(before.meta.Revision), RequestID: in.RequestID, Changed: true, AffectedIDs: []string{in.BindingID}, EntityRevision: &revision}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("public SQLite Unlink", err, got)
	}
	q.requests = append(q.requests, in.RequestID)
	after := bmQARead(t, q)
	bmQAFence(t, before, after, false)
	tombstone := q.linked.Binding
	tombstone.Revision = "2"
	bmQABinding(t, after.bindings[in.BindingID], tombstone, true)
	if row := after.requests[in.RequestID]; row.Value.Operation != "bindings.unlink" || !reflect.DeepEqual(row.Value.MutationResult, &got) {
		t.Fatal("cold atomic Unlink receipt")
	}
	if err := os.Rename(q.scope, q.scope+"-moved"); err != nil {
		t.Fatal(err)
	}
	again, err := q.service().Unlink(q.ctx, in)
	if err != nil || !reflect.DeepEqual(again, got) {
		t.Fatal("cold exact Unlink replay", err)
	}
	replayed := bmQARead(t, q)
	bmQAFence(t, after, replayed, true)
	oldLink, err := q.service().Link(q.ctx, q.input, brQAOffline(t))
	if err != nil || !reflect.DeepEqual(oldLink, q.linked) {
		t.Fatal("historical Link replay before vanished-path/provider discovery", err)
	}
	last := bmQARead(t, q)
	bmQAFence(t, replayed, last, true)
	bmQABinding(t, last.bindings[in.BindingID], tombstone, true)
	conflict := in
	conflict.IfRevision = "2"
	r, err := q.service().Unlink(q.ctx, conflict)
	bmQARefusal(t, r, MutationResult{}, err, "request_conflict")
	fresh := conflict
	fresh.RequestID = bmQAID(102)
	r, err = q.service().Unlink(q.ctx, fresh)
	bmQARefusal(t, r, MutationResult{}, err, "not_found")
	repaired, err := q.service().RepairBinding(q.ctx, RepairBindingInput{BindingID: in.BindingID, Path: q.scope + "-moved", IfRevision: "2", RequestID: bmQAID(103), Confirmed: true})
	bmQARefusal(t, repaired, BindingResult{}, err, "not_found")
	list, err := q.service().ListBindings(q.ctx)
	brQAList(t, list, err, last.meta.Revision, []Binding{})
	shown, err := q.service().ShowBinding(q.ctx, ShowBindingInput{BindingID: in.BindingID})
	brQARefusal(t, shown, err, "not_found")
	bmQAUnchanged(t, q, last)
	brQAPrivate(t, q.f)
}

func TestSQLiteFirstBindingRepairMovedLocatorCASCollisionAndReplay(t *testing.T) {
	q := bmQANew(t)
	bmQACapture(t, q, "finished")
	before := bmQARead(t, q)
	moved := filepath.Join(q.home, "moved")
	if err := os.Rename(q.scope, moved); err != nil {
		t.Fatal(err)
	}
	in := RepairBindingInput{BindingID: q.linked.Binding.ID, Path: moved, IfRevision: "1", RequestID: bmQAID(201), Confirmed: true}
	got, err := q.service().RepairBinding(q.ctx, in)
	wantBinding := q.linked.Binding
	wantBinding.Revision, wantBinding.Locator = "2", moved
	want := BindingResult{ContractVersion: 1, SnapshotRevision: bump(before.meta.Revision), RequestID: in.RequestID, Changed: true, Binding: wantBinding}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("public moved-directory Repair", err, got)
	}
	q.requests = append(q.requests, in.RequestID)
	after := bmQARead(t, q)
	bmQAFence(t, before, after, false)
	bmQABinding(t, after.bindings[in.BindingID], wantBinding, false)
	if row := after.requests[in.RequestID]; row.Value.Operation != "bindings.repair" || !reflect.DeepEqual(row.Value.BindingResult, &got) {
		t.Fatal("cold atomic Repair receipt")
	}
	stale := in
	stale.RequestID = bmQAID(202)
	r, err := q.service().RepairBinding(q.ctx, stale)
	bmQARefusal(t, r, BindingResult{}, err, "revision_conflict")
	u, err := q.service().Unlink(q.ctx, UnlinkInput{BindingID: in.BindingID, IfRevision: "1", RequestID: bmQAID(203), Confirmed: true})
	bmQARefusal(t, u, MutationResult{}, err, "revision_conflict")
	noConfirm := in
	noConfirm.IfRevision, noConfirm.RequestID, noConfirm.Confirmed = "2", bmQAID(204), false
	r, err = q.service().RepairBinding(q.ctx, noConfirm)
	bmQARefusal(t, r, BindingResult{}, err, "confirmation_required")
	u, err = q.service().Unlink(q.ctx, UnlinkInput{BindingID: in.BindingID, IfRevision: "2", RequestID: bmQAID(205)})
	bmQARefusal(t, u, MutationResult{}, err, "confirmation_required")
	bmQAUnchanged(t, q, after)
	noop := in
	noop.IfRevision, noop.RequestID = "2", bmQAID(206)
	n, err := q.service().RepairBinding(q.ctx, noop)
	noopWant := want
	noopWant.SnapshotRevision, noopWant.RequestID, noopWant.Changed = bump(after.meta.Revision), noop.RequestID, false
	if err != nil || !reflect.DeepEqual(n, noopWant) {
		t.Fatal("new no-op Repair receipt/public counter", err, n)
	}
	q.requests = append(q.requests, noop.RequestID)
	nooped := bmQARead(t, q)
	bmQAFence(t, after, nooped, false)
	if !reflect.DeepEqual(nooped.bindings, after.bindings) || !reflect.DeepEqual(nooped.requests[noop.RequestID].Value.BindingResult, &n) {
		t.Fatal("no-op Repair changed entity or lost receipt")
	}
	otherInput := q.input
	otherInput.Path, otherInput.RequestID = brQADirectory(t, filepath.Join(q.home, "occupied")), bmQAID(207)
	other, err := q.service().Link(q.ctx, otherInput, qaLinkDeps(t, qaNewLinkProvider(t)))
	if err != nil || !other.Changed || other.Binding.ID == in.BindingID {
		t.Fatal("SETUP second real binding", err)
	}
	q.bindings, q.requests = append(q.bindings, other.Binding.ID), append(q.requests, otherInput.RequestID)
	occupied := bmQARead(t, q)
	collision := noop
	collision.Path, collision.RequestID = otherInput.Path, bmQAID(208)
	r, err = q.service().RepairBinding(q.ctx, collision)
	bmQARefusal(t, r, BindingResult{}, err, "revision_conflict")
	collision.RequestID = in.RequestID
	r, err = q.service().RepairBinding(q.ctx, collision)
	bmQARefusal(t, r, BindingResult{}, err, "request_conflict")
	bmQAUnchanged(t, q, occupied)
	if err := os.Rename(moved, moved+"-again"); err != nil {
		t.Fatal(err)
	}
	r, err = q.service().RepairBinding(q.ctx, in)
	if err != nil || !reflect.DeepEqual(r, got) {
		t.Fatal("historical Repair replay before missing destination discovery", err)
	}
	replayed := bmQARead(t, q)
	bmQAFence(t, occupied, replayed, true)
	unlink := UnlinkInput{BindingID: in.BindingID, IfRevision: "2", RequestID: bmQAID(209), Confirmed: true}
	u, err = q.service().Unlink(q.ctx, unlink)
	if err != nil || !u.Changed || u.EntityRevision == nil || *u.EntityRevision != "3" || u.SnapshotRevision != bump(replayed.meta.Revision) {
		t.Fatal("Unlink repaired binding after locator vanished", err)
	}
	q.requests = append(q.requests, unlink.RequestID)
	deleted := bmQARead(t, q)
	bmQAFence(t, replayed, deleted, false)
	wantBinding.Revision = "3"
	bmQABinding(t, deleted.bindings[in.BindingID], wantBinding, true)
	if !reflect.DeepEqual(deleted.bindings[other.Binding.ID], occupied.bindings[other.Binding.ID]) {
		t.Fatal("mutation changed colliding peer binding")
	}
	r, err = q.service().RepairBinding(q.ctx, in)
	if err != nil || !reflect.DeepEqual(r, got) {
		t.Fatal("historical Repair result after deletion", err)
	}
	last := bmQARead(t, q)
	bmQAFence(t, deleted, last, true)
	bmQABinding(t, last.bindings[in.BindingID], wantBinding, true)
	collision.RequestID, collision.IfRevision = bmQAID(210), "3"
	r, err = q.service().RepairBinding(q.ctx, collision)
	bmQARefusal(t, r, BindingResult{}, err, "not_found")
	list, err := q.service().ListBindings(q.ctx)
	brQAList(t, list, err, last.meta.Revision, []Binding{other.Binding})
	bmQAUnchanged(t, q, last)
	brQAPrivate(t, q.f)
}

func TestSQLiteFirstBindingMutationsRefuseWorkingWaitingAndStaleActors(t *testing.T) {
	for _, state := range []string{"working", "wait_user", "stale"} {
		t.Run(state, func(t *testing.T) {
			q := bmQANew(t)
			bmQACapture(t, q, state)
			before := bmQARead(t, q)
			shown, err := q.service().ShowBinding(q.ctx, ShowBindingInput{BindingID: q.linked.Binding.ID})
			want := q.linked.Binding
			want.AttachedActors = []ActorRef{before.actor.Ref}
			brQAList(t, shown, err, before.meta.Revision, []Binding{want})
			u, err := q.service().Unlink(q.ctx, UnlinkInput{BindingID: want.ID, IfRevision: "1", RequestID: bmQAID(301), Confirmed: true})
			bmQARefusal(t, u, MutationResult{}, err, "binding_in_use")
			r, err := q.service().RepairBinding(q.ctx, RepairBindingInput{BindingID: want.ID, Path: brQADirectory(t, filepath.Join(q.home, "destination")), IfRevision: "1", RequestID: bmQAID(302), Confirmed: true})
			bmQARefusal(t, r, BindingResult{}, err, "binding_in_use")
			bmQAUnchanged(t, q, before)
		})
	}
}
