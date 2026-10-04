//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteRecoveryTarget struct {
	Uncertainty Uncertainty
	Segment     sqliteSegmentLocalRow
	Evidence    uncertaintyEvidence
	Epoch       sqliteEpochRow
	Actor       *sqliteActorLocalRow
}

// Read-only recovery owns one snapshot. Absent and proven pristine stores are
// empty observations; neither path creates a database or an authority file.
func sqliteRecoveryOpenRead(ctx context.Context, a sqliteCaptureAdmission) (c *sqliteio.Conn, tx *sqliteio.Tx, meta sqliteStoreMeta, found bool, err error) {
	var kind sqliteio.LinkInspection
	c, kind, err = sqliteio.InspectForLink(ctx, a.Directory, a.StateBasename, a.DatabaseBasename, a.AcquireDeadline)
	if err != nil {
		return
	}
	if kind == sqliteio.LinkAbsent || kind == sqliteio.LinkPristine {
		if c != nil {
			err = failure("state_corrupt")
		}
		return
	}
	if kind != sqliteio.LinkWAL || c == nil {
		err = failure("state_corrupt")
		return
	}
	tx, err = c.Begin(ctx, sqliteio.Read)
	if err == nil {
		meta, found, err = sqliteReadLinkSchema(tx, a.StateBasename, a.DatabaseBasename)
	}
	return
}

func (s *Service) reviewSQLite(ctx context.Context, in ReviewInput) (result ReviewList, err error) {
	a, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return result, err
	}
	c, tx, meta, found, err := sqliteRecoveryOpenRead(ctx, a)
	defer func() {
		cleanup := sqliteCaptureCleanup(c, tx)
		if err != nil || cleanup != nil {
			result = ReviewList{}
			err = sqliteCaptureError(errors.Join(err, cleanup))
		}
	}()
	if err != nil {
		return result, err
	}
	result = ReviewList{ContractVersion: 1, SnapshotRevision: "0", Uncertainties: []Uncertainty{}}
	if !found {
		return result, nil
	}
	result.SnapshotRevision = meta.Revision
	stmt, err := tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND state='unresolved' AND (?='' OR account_id=?) AND (?='' OR project_id=?) ORDER BY uncertainty_id", sqliteio.Text(meta.ComputerID), sqliteio.Text(in.AccountID), sqliteio.Text(in.AccountID), sqliteio.Text(in.ProjectID), sqliteio.Text(in.ProjectID))
	if err != nil {
		return result, err
	}
	ids, err := sqliteCaptureIDs(stmt, false)
	if err != nil {
		return result, err
	}
	for _, id := range ids {
		u, present, e := sqliteReadUncertaintyScalar(tx, meta.ComputerID, id)
		if e != nil {
			return result, e
		}
		if !present || u.State != "unresolved" || in.AccountID != "" && u.Attribution.AccountID != in.AccountID || in.ProjectID != "" && u.Attribution.ProjectID != in.ProjectID {
			return result, failure("state_corrupt")
		}
		if e = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, sqliteDependencySelection{UncertaintyIDs: []string{id}}); e != nil {
			return result, e
		}
		result.Uncertainties = append(result.Uncertainties, u)
	}
	return result, nil
}

func sqliteRecoveryReadTarget(tx *sqliteio.Tx, meta sqliteStoreMeta, id string) (result sqliteRecoveryTarget, err error) {
	u, found, err := sqliteReadUncertaintyScalar(tx, meta.ComputerID, id)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("uncertainty_not_found")
	}
	if u.State != "unresolved" {
		return result, failure("invalid_transition")
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, sqliteDependencySelection{UncertaintyIDs: []string{id}}); err != nil {
		return result, err
	}
	seg, found, err := sqliteReadSegmentLocal(tx, meta.ComputerID, u.SegmentID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	evidence, found, err := sqliteReadUncertaintyEvidenceScalar(tx, id)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	epoch, found, err := sqliteReadEpoch(tx, meta.ComputerID, seg.EpochID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	actor, err := sqliteCaptureReadActor(tx, meta.ComputerID, meta.Revision, u.Actor.Key)
	if err != nil {
		return result, err
	}
	return sqliteRecoveryTarget{Uncertainty: u, Segment: seg, Evidence: evidence, Epoch: epoch, Actor: actor}, nil
}

func sqliteRecoveryCeiling(target sqliteRecoveryTarget, sample ClockSample) time.Time {
	if at, ok := projectSample(&target.Epoch.Value, sample); ok {
		return at
	}
	return sample.WallUTC
}

// This check is pure and retains the legacy clock/bound/saturation precedence.
func sqliteRecoveryEnd(target sqliteRecoveryTarget, in RecoveryInput, sample *ClockSample) (time.Time, *TimeRange, error) {
	u, seg := target.Uncertainty, target.Segment
	end, upper := u.LowerBound, u.UpperBound
	if !in.DiscardTail {
		if sample == nil {
			return time.Time{}, nil, failure("clock_unavailable")
		}
		now := sqliteRecoveryCeiling(target, *sample)
		if now.Before(u.LowerBound) || target.Evidence.BoundSample != nil && (upper == nil || now.Before(*upper)) {
			return time.Time{}, nil, failure("clock_conflict")
		}
		if upper == nil || now.Before(*upper) {
			upper = &now
		}
		end = *in.End
		if end.Before(u.LowerBound) || end.After(*upper) || !seg.Start.Add(end.Sub(seg.Start)).Equal(end) {
			return time.Time{}, nil, failure("recovery_bounds")
		}
	}
	var suffix *TimeRange
	if upper != nil {
		suffix = &TimeRange{Start: end, End: *upper}
	}
	return end, suffix, nil
}

// These are concrete selected recovery facts, not a legacy state overlay.
// Preview's displayed union deliberately includes the selected attribution's
// retained supports; mutations do not load that display history.
type sqliteRecoveryContacts struct {
	Later     *sqliteSegmentLocalRow
	Other     []sqliteSegmentLocalRow
	Intervals []sqliteIntervalLocalRow
}

func sqliteRecoveryReadContacts(tx *sqliteio.Tx, meta sqliteStoreMeta, target sqliteRecoveryTarget, end time.Time) (result sqliteRecoveryContacts, err error) {
	seg, a := target.Segment, target.Segment.Binding.Attribution
	stmt, err := tx.Prepare("SELECT segment_id FROM segments WHERE actor_key=? AND (start_sec,start_nsec)>(?,?) ORDER BY start_sec,start_nsec,segment_id LIMIT 1", sqliteio.Text(actorKey(seg.Actor.Key)), sqliteio.Integer(seg.Start.Unix()), sqliteio.Integer(int64(seg.Start.Nanosecond())))
	if err != nil {
		return result, err
	}
	ids, err := sqliteCaptureIDs(stmt, false)
	if err != nil {
		return result, err
	}
	if len(ids) > 0 {
		row, found, e := sqliteReadSegmentLocal(tx, meta.ComputerID, ids[0])
		if e != nil {
			return result, e
		}
		if !found || row.Actor.Key != seg.Actor.Key || !row.Start.After(seg.Start) {
			return result, failure("state_corrupt")
		}
		if e = sqliteValidateSegmentDependencies(tx, meta.ComputerID, row); e != nil {
			return result, e
		}
		result.Later = &row
	}
	// An explicit recovery must account for zero-length and open contacts too.
	// The selected rows are typed and dependency-checked before their use.
	stmt, err = tx.Prepare("SELECT s.segment_id FROM segments AS s LEFT JOIN uncertainties AS u ON u.uncertainty_id=s.uncertainty_id WHERE s.computer_id=? AND s.account_id=? AND s.project_id=? AND s.segment_id<>? AND (s.user_id<>? OR s.task_id<>? OR s.timezone<>?) AND (s.start_sec,s.start_nsec)<=(?,?) AND ((s.end_sec IS NOT NULL AND (s.end_sec,s.end_nsec)>=(?,?)) OR (s.end_sec IS NULL AND (u.upper_bound_sec IS NULL OR (u.upper_bound_sec,u.upper_bound_nsec)>=(?,?)))) ORDER BY s.start_sec,s.start_nsec,s.segment_id", sqliteio.Text(meta.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Text(seg.ID), sqliteio.Text(a.UserID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Integer(end.Unix()), sqliteio.Integer(int64(end.Nanosecond())), sqliteio.Integer(seg.Start.Unix()), sqliteio.Integer(int64(seg.Start.Nanosecond())), sqliteio.Integer(seg.Start.Unix()), sqliteio.Integer(int64(seg.Start.Nanosecond())))
	if err != nil {
		return result, err
	}
	ids, err = sqliteCaptureIDs(stmt, false)
	if err != nil {
		return result, err
	}
	for _, id := range ids {
		row, found, e := sqliteReadSegmentLocal(tx, meta.ComputerID, id)
		if e != nil {
			return result, e
		}
		if !found || row.Binding.Attribution == a {
			return result, failure("state_corrupt")
		}
		if e = sqliteValidateSegmentDependencies(tx, meta.ComputerID, row); e != nil {
			return result, e
		}
		result.Other = append(result.Other, row)
	}
	timer := sqliteCaptureTimer(meta.ComputerID, a)
	prior, found, err := sqliteIntervalPredecessorLocal(tx, timer, seg.Start)
	if err != nil {
		return result, err
	}
	if found && !prior.End.Before(seg.Start) {
		if _, _, _, e := sqliteReadFinalizationInterval(tx, meta.ComputerID, prior.ID); e != nil {
			return result, e
		}
		result.Intervals = append(result.Intervals, prior)
	}
	stmt, err = tx.Prepare("SELECT interval_id FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)>=(?,?) AND (start_sec,start_nsec)<=(?,?) ORDER BY start_sec,start_nsec,interval_id", sqliteio.Text(meta.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.ProjectID), sqliteio.Integer(seg.Start.Unix()), sqliteio.Integer(int64(seg.Start.Nanosecond())), sqliteio.Integer(end.Unix()), sqliteio.Integer(int64(end.Nanosecond())))
	if err != nil {
		return result, err
	}
	ids, err = sqliteCaptureIDs(stmt, false)
	if err != nil {
		return result, err
	}
	for _, id := range ids {
		row, present, e := sqliteReadIntervalLocal(tx, meta.ComputerID, id)
		if e != nil {
			return result, e
		}
		if !present {
			return result, failure("state_corrupt")
		}
		if _, _, _, e = sqliteReadFinalizationInterval(tx, meta.ComputerID, id); e != nil {
			return result, e
		}
		result.Intervals = append(result.Intervals, row)
	}
	return result, nil
}

func sqliteRecoveryContactError(target sqliteRecoveryTarget, end time.Time, contacts sqliteRecoveryContacts) error {
	if contacts.Later != nil && end.After(contacts.Later.Start) {
		return failure("recovery_bounds")
	}
	if len(contacts.Other) > 0 {
		return attributionConflict(target.Segment.Binding.Attribution, contacts.Other[0].Binding.Attribution)
	}
	if len(contacts.Intervals) > 0 {
		return failure("clock_conflict")
	}
	return nil
}

func (s *Service) previewSQLite(ctx context.Context, in RecoveryInput) (result RecoveryPreview, err error) {
	if in.End != nil {
		owned := *in.End
		in.End = &owned
	}
	a, _, err := s.sqliteSyncConfigAdmission(ctx)
	if err != nil {
		return result, err
	}
	c, tx, meta, found, err := sqliteRecoveryOpenRead(ctx, a)
	defer func() {
		cleanup := sqliteCaptureCleanup(c, tx)
		if err != nil || cleanup != nil {
			result = RecoveryPreview{}
			err = sqliteCaptureError(errors.Join(err, cleanup))
		}
	}()
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("uncertainty_not_found")
	}
	target, err := sqliteRecoveryReadTarget(tx, meta, in.UncertaintyID)
	if err != nil {
		return result, err
	}
	proposed := target.Uncertainty.LowerBound
	if in.End != nil {
		proposed = *in.End
	}
	contacts, err := sqliteRecoveryReadContacts(tx, meta, target, proposed)
	if err != nil {
		return result, err
	}
	group := attributionKey(meta.ComputerID, target.Uncertainty.Attribution)
	stmt, err := tx.Prepare("SELECT segment_id FROM segments WHERE group_order=? ORDER BY start_sec,start_nsec,segment_id", sqliteio.Text(group))
	if err != nil {
		return result, err
	}
	ids, err := sqliteCaptureIDs(stmt, false)
	if err != nil {
		return result, err
	}
	before, after := []timeRange{}, []timeRange{}
	for _, id := range ids {
		row, present, e := sqliteReadSegmentLocal(tx, meta.ComputerID, id)
		if e != nil {
			return result, e
		}
		if !present || row.Binding.Attribution != target.Uncertainty.Attribution {
			return result, failure("state_corrupt")
		}
		if e = sqliteValidateSegmentDependencies(tx, meta.ComputerID, row); e != nil {
			return result, e
		}
		end := sqliteFinalizationEnd(row)
		before = append(before, timeRange{start: row.Start, end: end})
		if id == target.Segment.ID {
			end = proposed
		}
		after = append(after, timeRange{start: row.Start, end: end})
	}
	attr := target.Uncertainty.Attribution
	stmt, err = tx.Prepare("SELECT uncertainty_id FROM uncertainties WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND state='unresolved' AND uncertainty_id<>? ORDER BY uncertainty_id", sqliteio.Text(meta.ComputerID), sqliteio.Text(attr.AccountID), sqliteio.Text(attr.UserID), sqliteio.Text(attr.ProjectID), sqliteio.Text(attr.TaskID), sqliteio.Text(attr.Timezone), sqliteio.Text(in.UncertaintyID))
	if err != nil {
		return result, err
	}
	blocked, err := sqliteCaptureIDs(stmt, false)
	if err != nil {
		return result, err
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, meta.ComputerID, meta.Revision, sqliteDependencySelection{UncertaintyIDs: blocked}); err != nil {
		return result, err
	}
	// No arbitrary clock callback executes while a native snapshot is owned.
	if err = sqliteCaptureCleanup(c, tx); err != nil {
		c = nil
		tx = nil
		return result, err
	}
	c = nil
	tx = nil
	var sample *ClockSample
	if !in.DiscardTail {
		value, e := s.sample()
		if e != nil {
			return result, e
		}
		sample = &value
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	end, suffix, err := sqliteRecoveryEnd(target, in, sample)
	if err != nil {
		return result, err
	}
	if err = sqliteRecoveryContactError(target, end, contacts); err != nil {
		return result, err
	}
	result = RecoveryPreview{ContractVersion: 1, SnapshotRevision: meta.Revision, Uncertainty: target.Uncertainty, SegmentStart: target.Segment.Start, ConfirmedPrefix: TimeRange{Start: target.Segment.Start, End: target.Segment.Confirmed}, ProposedEnd: end, DiscardedSuffix: suffix, AffectedUnionBefore: []TimeRange{}, AffectedUnionAfter: []TimeRange{}, StillBlockedIDs: blocked}
	for _, r := range mergeRanges(before) {
		result.AffectedUnionBefore = append(result.AffectedUnionBefore, TimeRange{Start: r.start, End: r.end})
	}
	for _, r := range mergeRanges(after) {
		result.AffectedUnionAfter = append(result.AffectedUnionAfter, TimeRange{Start: r.start, End: r.end})
	}
	sort.Strings(result.StillBlockedIDs)
	return result, nil
}
