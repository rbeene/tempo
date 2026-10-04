//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"sort"
)

const sqliteCaptureCandidatesSQL = "SELECT actor_key,generation FROM actors WHERE computer_id=? AND health='continuous' AND state IN ('working','wait_children','wait_user')"
const sqliteCaptureUnboundedSQL = "SELECT uncertainty_id FROM uncertainties WHERE actor_key=? AND state='unresolved' AND upper_bound_sec IS NULL"
const sqliteCaptureMembershipSQL = "SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties WHERE actor_key=? AND uncertainty_id=? ORDER BY ordinal ASC LIMIT 1"
const sqliteCaptureOpenUnboundedSQL = "SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND upper_bound_sec IS NULL AND (user_id<>? OR task_id<>? OR timezone<>?) LIMIT 1"
const sqliteCaptureOpenBoundedSQL = "SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND project_id=? AND state='unresolved' AND (upper_bound_sec,upper_bound_nsec)>=(?,?) AND (user_id<>? OR task_id<>? OR timezone<>?) LIMIT 1"
const sqliteCaptureBindingsSQL = "SELECT binding_id FROM bindings WHERE computer_id=? AND account_id=? AND project_id=? AND active=1"
const sqliteCaptureTurnsSQL = "SELECT turn_key FROM host_turns WHERE actor_key=? AND actor_generation=?"

func sqliteCaptureTerminal(a sqliteActorLocalRow) bool {
	return a.State == "finished" || a.State == "interrupted"
}
func sqliteCaptureIDs(s *sqliteio.Stmt, hash bool) (ids []string, err error) {
	ids = []string{}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			ids = nil
		}
	}()
	for {
		row, e := s.Step()
		if e != nil {
			return nil, e
		}
		if !row {
			return ids, nil
		}
		if s.ColumnCount() != 1 {
			return nil, failure("state_corrupt")
		}
		id, e := sqliteDependencyText(s, 0)
		if e != nil {
			return nil, e
		}
		if (!hash && !validUUID(id)) || (hash && len(id) != 64) {
			return nil, failure("state_corrupt")
		}
		ids = append(ids, id)
	}
}
func sqliteCaptureReadActor(tx *sqliteio.Tx, computer, ceiling string, key ActorKey) (*sqliteActorLocalRow, error) {
	a, found, err := sqliteReadActorLocal(tx, computer, key)
	if err != nil || !found {
		return nil, err
	}
	// This row was decoded in this same owned snapshot. Validate its complete
	// dependency closure without preparing the identical actor read again.
	if !sqliteDependencyScope(computer, ceiling) {
		return nil, failure("validation")
	}
	if err = sqliteValidateActorDependencies(tx, computer, a); err != nil {
		return nil, err
	}
	if a.SegmentID != nil {
		if err = sqliteValidateSelectedCaptureDependencies(tx, computer, ceiling, sqliteDependencySelection{SegmentIDs: []string{*a.SegmentID}}); err != nil {
			return nil, err
		}
	}
	return &a, nil
}
func sqliteCaptureClockRefs(tx *sqliteio.Tx, computer string) (refs []ActorRef, err error) {
	s, err := tx.Prepare(sqliteCaptureCandidatesSQL, sqliteio.Text(computer))
	if err != nil {
		return nil, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			refs = nil
		}
	}()
	refs = []ActorRef{}
	for {
		present, e := s.Step()
		if e != nil {
			return nil, e
		}
		if !present {
			break
		}
		if s.ColumnCount() != 2 {
			return nil, failure("state_corrupt")
		}
		ref, e := sqliteDependencyStoredActor(tx, s, computer)
		if e != nil {
			return nil, e
		}
		refs = append(refs, ref)
	}
	sort.Slice(refs, func(i, j int) bool { return actorKey(refs[i].Key) < actorKey(refs[j].Key) })
	return refs, nil
}
func sqliteCapturePendingWait(a sqliteCaptureClockActor) bool {
	if a.Actor.State == "wait_children" {
		return true
	}
	if a.Actor.State != "wait_user" || a.Actor.Ref.Key.Source != "claude" {
		return false
	}
	for _, turn := range a.WaitTurns {
		waiting := false
		blocked := false
		for _, tool := range turn.Pending {
			if !hostWaitTool(turn.Row.Source, tool.Name) {
				blocked = true
			}
			waiting = true
		}
		if waiting && !blocked {
			return true
		}
	}
	return false
}
func sqliteCaptureObserveActor(tx *sqliteio.Tx, computer, ceiling string, a sqliteActorLocalRow) (result sqliteCaptureClockActor, err error) {
	result.Actor = a
	result.WaitTurns = []sqliteCaptureWaitTurn{}
	if a.SegmentID != nil {
		row, found, e := sqliteReadSegmentLocal(tx, computer, *a.SegmentID)
		if e != nil {
			return result, e
		}
		if !found {
			return result, failure("state_corrupt")
		}
		result.Segment = &row
		epoch, found, e := sqliteReadEpoch(tx, computer, row.EpochID)
		if e != nil {
			return result, e
		}
		if !found {
			return result, failure("state_corrupt")
		}
		result.Epoch = &epoch
	}
	if a.State == "wait_user" && a.Ref.Key.Source == "claude" {
		gen, e := sqliteEncodeUint64(a.Ref.Generation)
		if e != nil {
			return result, e
		}
		stmt, e := tx.Prepare(sqliteCaptureTurnsSQL, sqliteio.Text(actorKey(a.Ref.Key)), sqliteio.Blob(gen[:]))
		if e != nil {
			return result, e
		}
		keys, e := sqliteCaptureIDs(stmt, true)
		if e != nil {
			return result, e
		}
		sort.Strings(keys)
		for _, key := range keys {
			row, found, e := sqliteReadHostTurn(tx, computer, key)
			if e != nil {
				return result, e
			}
			if !found || row.Actor == nil || *row.Actor != a.Ref {
				return result, failure("state_corrupt")
			}
			tools, e := sqlitePendingHostTools(tx, computer, key)
			if e != nil {
				return result, e
			}
			if e = sqliteValidateSelectedCaptureDependencies(tx, computer, ceiling, sqliteDependencySelection{HostTurnKeys: []string{key}}); e != nil {
				return result, e
			}
			result.WaitTurns = append(result.WaitTurns, sqliteCaptureWaitTurn{Row: row, Pending: tools})
		}
	}
	if sqliteCapturePendingWait(result) {
		row, found, e := sqliteLatestHostReceipt(tx, computer, ceiling, a.Ref)
		if e != nil {
			return result, e
		}
		if found {
			result.LatestWait = &row
		}
	}
	return result, nil
}
func sqliteCaptureObserveClock(tx *sqliteio.Tx, computer, ceiling string) ([]sqliteCaptureClockActor, error) {
	refs, err := sqliteCaptureClockRefs(tx, computer)
	if err != nil {
		return nil, err
	}
	result := make([]sqliteCaptureClockActor, 0, len(refs))
	for _, ref := range refs {
		a, e := sqliteCaptureReadActor(tx, computer, ceiling, ref.Key)
		if e != nil {
			return nil, e
		}
		if a == nil || a.Ref != ref || a.Health != "continuous" || (a.State != "working" && a.State != "wait_children" && a.State != "wait_user") {
			return nil, failure("state_corrupt")
		}
		observed, e := sqliteCaptureObserveActor(tx, computer, ceiling, *a)
		if e != nil {
			return nil, e
		}
		result = append(result, observed)
	}
	return result, nil
}
func sqliteCaptureTimerBindings(tx *sqliteio.Tx, computer string, a Attribution) ([]sqliteBindingRow, error) {
	s, err := tx.Prepare(sqliteCaptureBindingsSQL, sqliteio.Text(computer), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID))
	if err != nil {
		return nil, err
	}
	ids, err := sqliteCaptureIDs(s, false)
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	rows := make([]sqliteBindingRow, 0, len(ids))
	for _, id := range ids {
		row, found, e := sqliteReadBinding(tx, computer, id)
		if e != nil {
			return nil, e
		}
		if !found || (row.Record != nil && row.Record.Deleted) || row.Snapshot.Attribution.AccountID != a.AccountID || row.Snapshot.Attribution.ProjectID != a.ProjectID {
			return nil, failure("state_corrupt")
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// One operation's concrete staged writes and selected dependencies, never a state overlay.
type sqliteCaptureMutation struct {
	tx           *sqliteio.Tx
	meta         sqliteStoreMeta
	nextRevision string
	transition   sqliteEventTransition
}

func (m *sqliteCaptureMutation) add(n int64, err error) error {
	return sqliteFinalizationAdd(&m.transition.Delta, n, err)
}
func sqliteCaptureMergeFinal(a *sqliteFinalizationSelection, b sqliteFinalizationSelection) {
	a.ChangedSegmentIDs = append(a.ChangedSegmentIDs, b.ChangedSegmentIDs...)
	a.NewSeals = append(a.NewSeals, b.NewSeals...)
	live, required, removed, cleared := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, id := range a.FrontierIDs {
		live[id] = true
	}
	for _, id := range a.RequiredPendingIDs {
		required[id] = true
	}
	for _, id := range a.RemovedFrontierIDs {
		removed[id] = true
	}
	for _, id := range a.ClearedPendingIDs {
		cleared[id] = true
	}
	for _, id := range b.RemovedFrontierIDs {
		removed[id] = true
		delete(live, id)
		delete(required, id)
		delete(cleared, id)
	}
	for _, id := range b.FrontierIDs {
		live[id] = true
		delete(removed, id)
	}
	for _, id := range b.RequiredPendingIDs {
		required[id] = true
		delete(cleared, id)
	}
	for _, id := range b.ClearedPendingIDs {
		cleared[id] = true
		delete(required, id)
	}
	a.FrontierIDs = sqliteCaptureSet(live)
	a.RequiredPendingIDs = sqliteCaptureSet(required)
	a.RemovedFrontierIDs = sqliteCaptureSet(removed)
	a.ClearedPendingIDs = sqliteCaptureSet(cleared)
}
func sqliteCaptureSet(values map[string]bool) []string {
	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
func (m *sqliteCaptureMutation) frontier(r sqliteFrontierMutationResult, err error) error {
	if err = m.add(r.Delta, err); err != nil {
		return err
	}
	sqliteCaptureMergeFinal(&m.transition.Finalization, r.Selection)
	m.transition.Dependencies.ChangedSegmentIDs = append(m.transition.Dependencies.ChangedSegmentIDs, r.Selection.ChangedSegmentIDs...)
	return nil
}
func sqliteCaptureTimer(computer string, a Attribution) sqliteTimerKey {
	return sqliteTimerKey{ComputerID: computer, AccountID: a.AccountID, ProjectID: a.ProjectID}
}
func (m *sqliteCaptureMutation) invalidate(a Attribution, before, after *sqliteReservationWindow) error {
	r, err := sqliteInvalidateReservation(m.tx, sqliteCaptureTimer(m.meta.ComputerID, a), before, after)
	return m.frontier(r, err)
}
func (m *sqliteCaptureMutation) writeSegment(before *sqliteSegmentLocalRow, after sqliteSegmentLocalRow) error {
	n, err := sqliteWriteSegmentLocal(m.tx, m.meta.ComputerID, before, after)
	if err = m.add(n, err); err != nil {
		return err
	}
	r, err := sqliteRefreshSegmentFrontier(m.tx, m.meta.ComputerID, before, after)
	if err = m.frontier(r, err); err != nil {
		return err
	}
	m.transition.Dependencies.ChangedSegmentIDs = append(m.transition.Dependencies.ChangedSegmentIDs, after.ID)
	return nil
}
func (m *sqliteCaptureMutation) actorWindow(a *sqliteActorLocalRow) (*sqliteReservationWindow, error) {
	if a == nil || a.State != "working" || a.Health != "continuous" || a.SegmentID == nil {
		return nil, nil
	}
	row, found, err := sqliteReadSegmentLocal(m.tx, m.meta.ComputerID, *a.SegmentID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, failure("state_corrupt")
	}
	return &sqliteReservationWindow{Start: row.Start}, nil
}
func (m *sqliteCaptureMutation) writeActor(before *sqliteActorLocalRow, after sqliteActorLocalRow) error {
	oldWindow, err := m.actorWindow(before)
	if err != nil {
		return err
	}
	newWindow, err := m.actorWindow(&after)
	if err != nil {
		return err
	}
	n, err := sqliteWriteActorLocal(m.tx, m.meta.ComputerID, before, after)
	if err = m.add(n, err); err != nil {
		return err
	}
	if before != nil && before.Attribution != after.Attribution {
		if err = m.invalidate(before.Attribution, oldWindow, nil); err != nil {
			return err
		}
		oldWindow = nil
	}
	if err = m.invalidate(after.Attribution, oldWindow, newWindow); err != nil {
		return err
	}
	m.transition.Dependencies.ActorKeys = append(m.transition.Dependencies.ActorKeys, after.Ref.Key)
	return nil
}
func (m *sqliteCaptureMutation) writeUncertainty(before *Uncertainty, after Uncertainty) error {
	var oldWindow, newWindow *sqliteReservationWindow
	if before != nil && before.State == "unresolved" {
		oldWindow = &sqliteReservationWindow{Start: before.LowerBound, End: before.UpperBound}
	}
	if after.State == "unresolved" {
		newWindow = &sqliteReservationWindow{Start: after.LowerBound, End: after.UpperBound}
	}
	n, err := sqliteWriteUncertainty(m.tx, m.meta.ComputerID, before, after)
	if err = m.add(n, err); err != nil {
		return err
	}
	if before != nil && before.Attribution != after.Attribution {
		if err = m.invalidate(before.Attribution, oldWindow, nil); err != nil {
			return err
		}
		oldWindow = nil
	}
	if err = m.invalidate(after.Attribution, oldWindow, newWindow); err != nil {
		return err
	}
	m.transition.Dependencies.ChangedUncertaintyIDs = append(m.transition.Dependencies.ChangedUncertaintyIDs, after.ID)
	return nil
}
func (m *sqliteCaptureMutation) readActor(key ActorKey) (*sqliteActorLocalRow, error) {
	a, found, err := sqliteReadActorLocal(m.tx, m.meta.ComputerID, key)
	if err != nil || !found {
		return nil, err
	}
	return &a, nil
}
func (m *sqliteCaptureMutation) quarantine(a sqliteActorLocalRow, reason, kind string, sample ClockSample) (bool, error) {
	observed, err := sqliteCaptureObserveActor(m.tx, m.meta.ComputerID, m.nextRevision, a)
	if err != nil {
		return false, err
	}
	if sqliteCapturePendingWait(observed) {
		if a.Health != "continuous" || observed.LatestWait == nil {
			return false, nil
		}
		receipt := observed.LatestWait.Record.Result
		receipt.ID = newID()
		receipt.SnapshotRevision = m.nextRevision
		receipt.Kind = kind
		receipt.ToolID = ""
		receipt.ObservedAt = sample.WallUTC
		receipt.Disposition = "review_required"
		receipt.Ordering = "review_required"
		receipt.DiagnosticCode = "source_loss_while_waiting"
		ref := a.Ref
		receipt.Actor = &ref
		row := sqliteHostReceiptRow{Key: hostHash([]string{"wait_loss", receipt.ID}), Record: hostReceiptRecord{Fingerprint: hostHash(receipt), Result: receipt}}
		n, e := sqliteInsertHostReceipt(m.tx, m.meta.ComputerID, m.nextRevision, row)
		if e = m.add(n, e); e != nil {
			return false, e
		}
		after := a
		after.Health = "stale"
		after.Revision = bump(a.Revision)
		if err = m.writeActor(&a, after); err != nil {
			return false, err
		}
		m.transition.Dependencies.HostReceiptKeys = append(m.transition.Dependencies.HostReceiptKeys, row.Key)
		return true, nil
	}
	if a.State != "working" || a.Health != "continuous" || a.SegmentID == nil {
		return false, nil
	}
	if observed.Segment == nil {
		return false, failure("state_corrupt")
	}
	seg := *observed.Segment
	id := newID()
	u := Uncertainty{ID: id, Revision: "1", Actor: a.Ref, SegmentID: seg.ID, Attribution: a.Attribution, LowerBound: seg.Confirmed, Reason: reason, State: "unresolved"}
	if err = m.writeUncertainty(nil, u); err != nil {
		return false, err
	}
	n, e := sqliteWriteUncertaintyEvidence(m.tx, id, nil, uncertaintyEvidence{Detection: sample, LastConfirmed: seg.ConfirmedSample})
	if err = m.add(n, e); err != nil {
		return false, err
	}
	next := seg
	next.UncertaintyID = &id
	if err = m.writeSegment(&seg, next); err != nil {
		return false, err
	}
	ordinal, e := sqliteNextActorUncertaintyOrdinalLocal(m.tx, m.meta.ComputerID, a.Ref.Key)
	if e != nil {
		return false, e
	}
	n, e = sqliteAppendActorUncertaintyLocal(m.tx, m.meta.ComputerID, a.Ref.Key, ordinal, id)
	if err = m.add(n, e); err != nil {
		return false, err
	}
	after := a
	after.Health = "stale"
	after.Revision = bump(a.Revision)
	if err = m.writeActor(&a, after); err != nil {
		return false, err
	}
	m.transition.Dependencies.ActorUncertaintyEdges = append(m.transition.Dependencies.ActorUncertaintyEdges, sqliteActorUncertaintySelection{Key: a.Ref.Key, Ordinal: ordinal})
	return true, nil
}
func (m *sqliteCaptureMutation) quarantineClock(sample ClockSample) (bool, error) {
	// This is the complete initial candidate snapshot, before any safety write.
	// Stored provenance must satisfy the observed revision, not the new ceiling.
	observed, err := sqliteCaptureObserveClock(m.tx, m.meta.ComputerID, m.meta.Revision)
	if err != nil {
		return false, err
	}
	changed := false
	for _, row := range observed {
		why := discontinuity(row.Actor.LastEvidence, sample)
		if why == "" && row.Epoch != nil {
			why = discontinuity(row.Epoch.Value.Anchor, sample)
		}
		if why == "" {
			continue
		}
		did, e := m.quarantine(row.Actor, why, "ClockObservation", sample)
		if e != nil {
			return false, e
		}
		changed = changed || did
	}
	return changed, nil
}
func sqliteCaptureMembership(s *sqliteio.Stmt, key, id string) (ordinal int64, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			ordinal = 0
			found = false
		}
	}()
	row, err := s.Step()
	if err != nil || !row {
		return 0, false, err
	}
	if s.ColumnCount() != 3 {
		return 0, false, failure("state_corrupt")
	}
	owner, err := sqliteDependencyText(s, 0)
	if err != nil {
		return 0, false, err
	}
	ordinal, err = sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return 0, false, err
	}
	selected, err := sqliteDependencyText(s, 2)
	if err != nil {
		return 0, false, err
	}
	if owner != key || selected != id || ordinal < 0 {
		return 0, false, failure("state_corrupt")
	}
	row, err = s.Step()
	if err != nil {
		return 0, false, err
	}
	if row {
		return 0, false, failure("state_corrupt")
	}
	return ordinal, true, nil
}
func (m *sqliteCaptureMutation) cap(a sqliteActorLocalRow, sample ClockSample) (*Error, error) {
	bound := sample.WallUTC
	if a.SegmentID != nil {
		row, found, err := sqliteReadSegmentLocal(m.tx, m.meta.ComputerID, *a.SegmentID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, failure("state_corrupt")
		}
		ep, found, err := sqliteReadEpoch(m.tx, m.meta.ComputerID, row.EpochID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, failure("state_corrupt")
		}
		if t, ok := projectSample(&ep.Value, sample); ok {
			bound = t
		}
	}
	s, err := m.tx.Prepare(sqliteCaptureUnboundedSQL, sqliteio.Text(actorKey(a.Ref.Key)))
	if err != nil {
		return nil, err
	}
	ids, err := sqliteCaptureIDs(s, false)
	if err != nil {
		return nil, err
	}
	type capRow struct {
		ordinal int64
		row     Uncertainty
	}
	rows := []capRow{}
	for _, id := range ids {
		u, found, e := sqliteReadUncertaintyScalar(m.tx, m.meta.ComputerID, id)
		if e != nil {
			return nil, e
		}
		if !found || actorKey(u.Actor.Key) != actorKey(a.Ref.Key) || u.State != "unresolved" || u.UpperBound != nil {
			return nil, failure("state_corrupt")
		}
		if e = sqliteValidateSelectedCaptureDependencies(m.tx, m.meta.ComputerID, m.nextRevision, sqliteDependencySelection{UncertaintyIDs: []string{id}}); e != nil {
			return nil, e
		}
		stmt, e := m.tx.Prepare(sqliteCaptureMembershipSQL, sqliteio.Text(actorKey(a.Ref.Key)), sqliteio.Text(id))
		if e != nil {
			return nil, e
		}
		ordinal, found, e := sqliteCaptureMembership(stmt, actorKey(a.Ref.Key), id)
		if e != nil {
			return nil, e
		}
		if found {
			rows = append(rows, capRow{ordinal, u})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ordinal < rows[j].ordinal })
	for _, entry := range rows {
		u := entry.row
		if bound.Before(u.LowerBound) {
			return failure("clock_conflict"), nil
		}
		ev, found, e := sqliteReadUncertaintyEvidenceScalar(m.tx, u.ID)
		if e != nil {
			return nil, e
		}
		if !found {
			return nil, failure("state_corrupt")
		}
		after := u
		after.UpperBound = &bound
		after.Revision = bump(u.Revision)
		if e = m.writeUncertainty(&u, after); e != nil {
			return nil, e
		}
		next := ev
		copySample := sqliteCaptureCopySample(sample)
		next.BoundSample = &copySample
		n, e := sqliteWriteUncertaintyEvidence(m.tx, u.ID, &ev, next)
		if e = m.add(n, e); e != nil {
			return nil, e
		}
	}
	return nil, nil
}
func (m *sqliteCaptureMutation) open(a *sqliteActorLocalRow, sample ClockSample, key string) (*Error, error) {
	ep, found, err := sqliteLatestEpoch(m.tx, m.meta.ComputerID, a.Attribution)
	if err != nil {
		return nil, err
	}
	_, projected := projectSample(&ep.Value, sample)
	if !found || !projected {
		ordinal, e := sqliteNextEpochOrdinal(m.tx)
		if e != nil {
			return nil, e
		}
		ep = sqliteEpochRow{Ordinal: ordinal, Value: timelineEpoch{ID: newID(), ComputerID: m.meta.ComputerID, Attribution: a.Attribution, Anchor: sample}}
		n, e := sqliteInsertEpoch(m.tx, ep)
		if e = m.add(n, e); e != nil {
			return nil, e
		}
		m.transition.Dependencies.EpochIDs = append(m.transition.Dependencies.EpochIDs, ep.Value.ID)
	}
	start, ok := projectSample(&ep.Value, sample)
	if !ok {
		return nil, failure("state_corrupt")
	}
	old, found, err := sqliteLatestIntervalLocal(m.tx, sqliteCaptureTimer(m.meta.ComputerID, a.Attribution))
	if err != nil {
		return nil, err
	}
	if found {
		if _, _, _, err = sqliteReadFinalizationInterval(m.tx, m.meta.ComputerID, old.ID); err != nil {
			return nil, err
		}
		if start.Before(old.End) {
			return failure("clock_conflict"), nil
		}
	}
	at := a.Attribution
	base := []sqliteio.Value{sqliteio.Text(m.meta.ComputerID), sqliteio.Text(at.AccountID), sqliteio.Text(at.ProjectID)}
	for index, sql := range []string{sqliteCaptureOpenUnboundedSQL, sqliteCaptureOpenBoundedSQL} {
		binds := append([]sqliteio.Value{}, base...)
		if index == 1 {
			binds = append(binds, sqliteio.Integer(start.Unix()), sqliteio.Integer(int64(start.Nanosecond())))
		}
		binds = append(binds, sqliteio.Text(at.UserID), sqliteio.Text(at.TaskID), sqliteio.Text(at.Timezone))
		s, e := m.tx.Prepare(sql, binds...)
		if e != nil {
			return nil, e
		}
		ids, e := sqliteCaptureIDs(s, false)
		if e != nil {
			return nil, e
		}
		if len(ids) > 1 {
			return nil, failure("state_corrupt")
		}
		if len(ids) == 0 {
			continue
		}
		u, found, e := sqliteReadUncertaintyScalar(m.tx, m.meta.ComputerID, ids[0])
		if e != nil {
			return nil, e
		}
		if !found || u.State != "unresolved" || u.Attribution.AccountID != at.AccountID || u.Attribution.ProjectID != at.ProjectID || u.Attribution == at || (index == 0 && u.UpperBound != nil) || (index == 1 && (u.UpperBound == nil || start.After(*u.UpperBound))) {
			return nil, failure("state_corrupt")
		}
		if e = sqliteValidateSelectedCaptureDependencies(m.tx, m.meta.ComputerID, m.nextRevision, sqliteDependencySelection{UncertaintyIDs: ids}); e != nil {
			return nil, e
		}
		return attributionConflict(u.Attribution, at), nil
	}
	n, e := sqliteEnsureActorGeneration(m.tx, a.Ref)
	if e = m.add(n, e); e != nil {
		return nil, e
	}
	if a.Parent != nil {
		n, e = sqliteEnsureActorGeneration(m.tx, *a.Parent)
		if e = m.add(n, e); e != nil {
			return nil, e
		}
	}
	id := newID()
	row := sqliteSegmentLocalRow{ID: id, Actor: a.Ref, Binding: BindingSnapshot{ID: a.BindingID, Revision: a.BindingRevision, Attribution: a.Attribution}, EpochID: ep.Value.ID, StartSample: sample, ConfirmedSample: sample, Start: start, Confirmed: start}
	if err = m.writeSegment(nil, row); err != nil {
		return nil, err
	}
	n, e = sqliteAppendSegmentEventLocal(m.tx, m.meta.ComputerID, id, 0, key)
	if e = m.add(n, e); e != nil {
		return nil, e
	}
	a.SegmentID = &id
	m.transition.Dependencies.SegmentEventEdges = append(m.transition.Dependencies.SegmentEventEdges, sqliteSegmentEventSelection{SegmentID: id, Ordinal: 0})
	return nil, nil
}
func (m *sqliteCaptureMutation) confirm(a *sqliteActorLocalRow, sample ClockSample, key string, close bool) (*Error, error) {
	if a.SegmentID == nil {
		return nil, failure("state_corrupt")
	}
	row, found, err := sqliteReadSegmentLocal(m.tx, m.meta.ComputerID, *a.SegmentID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, failure("state_corrupt")
	}
	ep, found, err := sqliteReadEpoch(m.tx, m.meta.ComputerID, row.EpochID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, failure("state_corrupt")
	}
	end, ok := projectSample(&ep.Value, sample)
	if !ok || end.Before(row.Confirmed) {
		return failure("clock_conflict"), nil
	}
	after := row
	after.Confirmed = end
	after.ConfirmedSample = sample
	if close {
		after.End = &end
	}
	if err = m.writeSegment(&row, after); err != nil {
		return nil, err
	}
	ordinal, err := sqliteNextSegmentEventOrdinalLocal(m.tx, m.meta.ComputerID, row.ID)
	if err != nil {
		return nil, err
	}
	n, e := sqliteAppendSegmentEventLocal(m.tx, m.meta.ComputerID, row.ID, ordinal, key)
	if e = m.add(n, e); e != nil {
		return nil, e
	}
	a.LastEvidence = sample
	m.transition.Dependencies.SegmentEventEdges = append(m.transition.Dependencies.SegmentEventEdges, sqliteSegmentEventSelection{SegmentID: row.ID, Ordinal: ordinal})
	return nil, nil
}
