//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"errors"
	"math"
	"reflect"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// These checks consume owned materialized rows. They never encode or repair a
// row, and peer reads stay scalar: the capture composer owns graph traversal.
func sqliteValidateActorDependencies(tx *sqliteio.Tx, computer string, row sqliteActorLocalRow) error {
	if !validUUID(computer) {
		return failure("validation")
	}
	if !sqliteValidActorLocal(row) || row.Ref.Key.ComputerID != computer ||
		!sqliteDependencyRefMaterialized(row.Ref) || !sqliteDependencyAttributionMaterialized(row.Attribution) ||
		!sqliteDependencyClockMaterialized(row.LastEvidence) ||
		row.Parent != nil && !sqliteDependencyRefMaterialized(*row.Parent) {
		return failure("state_corrupt")
	}
	if err := sqliteDependencyGeneration(tx, row.Ref); err != nil {
		return err
	}
	if row.Parent != nil {
		if err := sqliteDependencyGeneration(tx, *row.Parent); err != nil {
			return err
		}
	}
	if row.SegmentID != nil {
		segment, found, err := sqliteReadSegmentLocal(tx, computer, *row.SegmentID)
		if err != nil {
			return err
		}
		if !found || segment.Actor != row.Ref || segment.End != nil {
			return failure("state_corrupt")
		}
	}
	return nil
}

func sqliteValidateSegmentDependencies(tx *sqliteio.Tx, computer string, row sqliteSegmentLocalRow) error {
	if !validUUID(computer) {
		return failure("validation")
	}
	if !sqliteValidSegmentLocal(row) || row.Actor.Key.ComputerID != computer ||
		!sqliteDependencyRefMaterialized(row.Actor) || !sqliteDependencyAttributionMaterialized(row.Binding.Attribution) ||
		!sqliteDependencyClockMaterialized(row.StartSample) || !sqliteDependencyClockMaterialized(row.ConfirmedSample) ||
		!sqliteDependencyTimeMaterialized(row.Start) || !sqliteDependencyTimeMaterialized(row.Confirmed) ||
		row.End != nil && !sqliteDependencyTimeMaterialized(*row.End) {
		return failure("state_corrupt")
	}
	if err := sqliteDependencyGeneration(tx, row.Actor); err != nil {
		return err
	}
	epoch, found, err := sqliteReadEpoch(tx, computer, row.EpochID)
	if err != nil {
		return err
	}
	if !found || epoch.Value.Attribution != row.Binding.Attribution {
		return failure("state_corrupt")
	}
	start, startOK := projectSample(&epoch.Value, row.StartSample)
	confirmed, confirmedOK := projectSample(&epoch.Value, row.ConfirmedSample)
	if !startOK || !confirmedOK || !start.Equal(row.Start) || !confirmed.Equal(row.Confirmed) {
		return failure("state_corrupt")
	}
	if row.UncertaintyID == nil {
		if row.End != nil && !row.End.Equal(row.Confirmed) {
			return failure("state_corrupt")
		}
		return nil
	}
	u, found, err := sqliteReadUncertaintyScalar(tx, computer, *row.UncertaintyID)
	if err != nil {
		return err
	}
	if !found || u.SegmentID != row.ID || u.Actor != row.Actor || u.Attribution != row.Binding.Attribution ||
		row.End != nil && !row.End.Equal(row.Confirmed) && u.State != "resolved" {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteValidateUncertaintyDependencies(tx *sqliteio.Tx, computer, revisionCeiling string, row Uncertainty) error {
	if !sqliteDependencyScope(computer, revisionCeiling) {
		return failure("validation")
	}
	if !sqliteValidUncertaintyScalar(row) || row.Actor.Key.ComputerID != computer ||
		!sqliteDependencyRefMaterialized(row.Actor) || !sqliteDependencyAttributionMaterialized(row.Attribution) ||
		!sqliteDependencyTimeMaterialized(row.LowerBound) ||
		row.UpperBound != nil && !sqliteDependencyTimeMaterialized(*row.UpperBound) ||
		row.ResolutionEnd != nil && !sqliteDependencyTimeMaterialized(*row.ResolutionEnd) {
		return failure("state_corrupt")
	}
	if err := sqliteDependencyGeneration(tx, row.Actor); err != nil {
		return err
	}
	segment, found, err := sqliteReadSegmentLocal(tx, computer, row.SegmentID)
	if err != nil {
		return err
	}
	if !found || segment.Actor != row.Actor || !row.LowerBound.Equal(segment.Confirmed) {
		return failure("state_corrupt")
	}
	evidence, found, err := sqliteReadUncertaintyEvidenceScalar(tx, row.ID)
	if err != nil {
		return err
	}
	if !found || !reflect.DeepEqual(evidence.LastConfirmed, segment.ConfirmedSample) ||
		(evidence.BoundSample == nil) != (row.UpperBound == nil) {
		return failure("state_corrupt")
	}
	decision, found, err := sqliteReadRecoveryDecisionScalar(tx, row.ID)
	if err != nil {
		return err
	}
	if row.State == "unresolved" {
		if found {
			return failure("state_corrupt")
		}
		return nil
	}
	if !found || decision.UncertaintyID != row.ID || row.ResolutionEnd == nil ||
		!decision.ResolutionEnd.Equal(*row.ResolutionEnd) || decision.Discarded != row.Discarded {
		return failure("state_corrupt")
	}
	previous, ok := counter(decision.PreviousRevision)
	revision, revisionOK := counter(row.Revision)
	if !ok || !revisionOK || previous == 0 || previous == math.MaxUint64 || revision != previous+1 ||
		segment.End == nil || !segment.End.Equal(decision.ResolutionEnd) || segment.End.Before(segment.Confirmed) ||
		decision.Discarded && !segment.End.Equal(segment.Confirmed) ||
		row.UpperBound != nil && segment.End.After(*row.UpperBound) {
		return failure("state_corrupt")
	}
	if (row.UpperBound == nil) != (decision.DiscardedSuffix == nil) {
		return failure("state_corrupt")
	}
	if decision.DiscardedSuffix != nil && (!decision.DiscardedSuffix.Start.Equal(*segment.End) ||
		!decision.DiscardedSuffix.End.Equal(*row.UpperBound)) {
		return failure("state_corrupt")
	}
	if !decision.Discarded {
		if decision.ObservedSample == nil || row.UpperBound == nil {
			return failure("state_corrupt")
		}
		if _, _, available := sampleValues(*decision.ObservedSample); !available {
			return failure("state_corrupt")
		}
		epoch, present, readErr := sqliteReadEpoch(tx, computer, segment.EpochID)
		if readErr != nil {
			return readErr
		}
		if !present {
			return failure("state_corrupt")
		}
		ceiling, projected := projectSample(&epoch.Value, *decision.ObservedSample)
		if !projected {
			ceiling = decision.ObservedSample.WallUTC
		}
		if ceiling.Before(*segment.End) || ceiling.Before(*row.UpperBound) {
			return failure("state_corrupt")
		}
	}
	proof, found, err := sqliteReadResolveRequestProof(tx, decision.RequestID, revisionCeiling)
	if err != nil {
		return err
	}
	if !found {
		return failure("state_corrupt")
	}
	member := false
	for _, id := range proof.Result.AffectedIDs {
		if id == row.ID {
			member = true
		}
	}
	if !member {
		return failure("state_corrupt")
	}
	return sqliteValidateResolveRequestDependencies(tx, computer, revisionCeiling, proof)
}

func sqliteValidateResolveRequestDependencies(tx *sqliteio.Tx, computer, revisionCeiling string, row sqliteResolveRequestRow) error {
	if !sqliteDependencyScope(computer, revisionCeiling) {
		return failure("validation")
	}
	ceiling, _ := counter(revisionCeiling)
	revision, _ := counter(row.Result.SnapshotRevision)
	if !sqliteValidResolveRequest(row) || revision > ceiling {
		return failure("state_corrupt")
	}
	for _, id := range row.Result.AffectedIDs {
		uncertainty, err := sqliteDependencyAffectedID(tx, computer, id, true)
		if err != nil {
			return err
		}
		actor, err := sqliteDependencyAffectedID(tx, computer, id, false)
		if err != nil {
			return err
		}
		if !uncertainty && !actor {
			return failure("state_corrupt")
		}
	}
	return nil
}

func sqliteValidateActorUncertaintyEdge(tx *sqliteio.Tx, computer string, key ActorKey, ordinal int64) error {
	_, err := sqliteActorUncertaintyDependency(tx, computer, key, ordinal)
	return err
}

func sqliteActorUncertaintyDependency(tx *sqliteio.Tx, computer string, key ActorKey, ordinal int64) (string, error) {
	if !validUUID(computer) || !validKey(key) || ordinal < 0 {
		return "", failure("validation")
	}
	owner, found, err := sqliteReadActorLocal(tx, computer, key)
	if err != nil {
		return "", err
	}
	if !found {
		return "", failure("state_corrupt")
	}
	id, err := sqliteDependencyEdge(tx, actorKey(key), ordinal, true)
	if err != nil {
		return "", err
	}
	u, found, err := sqliteReadUncertaintyScalar(tx, computer, id)
	if err != nil {
		return "", err
	}
	if !found || u.Actor.Key != owner.Ref.Key {
		return "", failure("state_corrupt")
	}
	return id, nil
}

// The two edge tables have the same fixed three-column shape. Their reference
// policies differ: segment event references remain opaque retained text.
func sqliteDependencyEdge(tx *sqliteio.Tx, owner string, ordinal int64, uncertainty bool) (result string, err error) {
	query := "SELECT segment_id,ordinal,event_reference FROM segment_events WHERE segment_id=? AND ordinal=?"
	if uncertainty {
		query = "SELECT actor_key,ordinal,uncertainty_id FROM actor_uncertainties WHERE actor_key=? AND ordinal=?"
	}
	s, err := tx.Prepare(query, sqliteio.Text(owner), sqliteio.Integer(ordinal))
	if err != nil {
		return "", err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = ""
		}
	}()
	present, err := s.Step()
	if err != nil {
		return "", err
	}
	if !present {
		return "", failure("state_corrupt")
	}
	storedOrdinal, reference, err := sqliteReadLocalChild(s, owner, uncertainty)
	if err != nil {
		return "", err
	}
	if storedOrdinal != ordinal {
		return "", failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", err
	} else if present {
		return "", failure("state_corrupt")
	}
	return reference, nil
}

// Both alternatives are checked even when the first exists: a malformed or
// foreign row in the other family cannot be hidden by a successful fallback.
func sqliteDependencyAffectedID(tx *sqliteio.Tx, computer, id string, uncertainty bool) (found bool, err error) {
	query := "SELECT id,computer_id FROM actors WHERE id=?"
	if uncertainty {
		query = "SELECT uncertainty_id,computer_id FROM uncertainties WHERE uncertainty_id=?"
	}
	s, err := tx.Prepare(query, sqliteio.Text(id))
	if err != nil {
		return false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return false, err
	}
	if s.ColumnCount() != 2 {
		return false, failure("state_corrupt")
	}
	storedID, err := sqliteDependencyText(s, 0)
	if err != nil {
		return false, err
	}
	storedComputer, err := sqliteDependencyText(s, 1)
	if err != nil {
		return false, err
	}
	if storedID != id || storedComputer != computer {
		return false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return false, err
	} else if present {
		return false, failure("state_corrupt")
	}
	return true, nil
}

func sqliteDependencyText(s *sqliteio.Stmt, column int) (string, error) {
	kind, err := s.Kind(column)
	if err != nil {
		return "", errors.Join(failure("state_corrupt"), err)
	}
	if kind != sqliteio.TextKind {
		return "", failure("state_corrupt")
	}
	value, err := s.Text(column)
	if err != nil {
		return "", err
	}
	if !utf8.ValidString(value) {
		return "", failure("state_corrupt")
	}
	return value, nil
}

func sqliteDependencyGeneration(tx *sqliteio.Tx, ref ActorRef) error {
	stored, err := sqliteReadHostReceiptGeneration(tx, actorKey(ref.Key), ref.Generation)
	if err != nil {
		return err
	}
	if stored != ref {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteDependencyScope(computer, revisionCeiling string) bool {
	n, ok := counter(revisionCeiling)
	return validUUID(computer) && ok && n > 0
}

func sqliteDependencyRefMaterialized(ref ActorRef) bool {
	return utf8.ValidString(ref.Key.ComputerID) && utf8.ValidString(ref.Key.Source) &&
		utf8.ValidString(ref.Key.SessionID) && utf8.ValidString(ref.Key.AgentID) && utf8.ValidString(ref.Generation)
}

func sqliteDependencyAttributionMaterialized(a Attribution) bool {
	return utf8.ValidString(a.AccountID) && utf8.ValidString(a.UserID) && utf8.ValidString(a.ProjectID) &&
		utf8.ValidString(a.TaskID) && utf8.ValidString(a.Timezone)
}

func sqliteDependencyTimeMaterialized(value time.Time) bool {
	_, err := value.MarshalJSON()
	return err == nil
}

func sqliteDependencyClockMaterialized(sample ClockSample) bool {
	return utf8.ValidString(sample.Capability) && sqliteDependencyTimeMaterialized(sample.WallUTC) &&
		(sample.Epoch == nil || utf8.ValidString(*sample.Epoch)) &&
		(sample.ElapsedNS == nil || utf8.ValidString(*sample.ElapsedNS)) &&
		(sample.AwakeNS == nil || utf8.ValidString(*sample.AwakeNS))
}
