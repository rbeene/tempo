//go:build (darwin || linux) && (amd64 || arm64)

package activity

import "github.com/rbeene/tempo/internal/activity/sqliteio"

type sqliteSealSelection struct{ ComponentID, IntervalID, OutboxID string }

type sqliteFinalizationSelection struct {
	ChangedSegmentIDs                                                      []string
	FrontierIDs, RequiredPendingIDs, RemovedFrontierIDs, ClearedPendingIDs []string
	NewSeals                                                               []sqliteSealSelection
}

func sqliteEmptyFinalizationSelection() sqliteFinalizationSelection {
	return sqliteFinalizationSelection{ChangedSegmentIDs: []string{}, FrontierIDs: []string{}, RequiredPendingIDs: []string{}, RemovedFrontierIDs: []string{}, ClearedPendingIDs: []string{}, NewSeals: []sqliteSealSelection{}}
}

// Own a fixed one-identity selection through DONE/Close before any row lookup.
func sqliteFinalizationSelectedID(s *sqliteio.Stmt, component bool) (id string, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			id = ""
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", false, err
	}
	if s.ColumnCount() != 1 {
		return "", false, failure("state_corrupt")
	}
	id, err = sqliteDependencyText(s, 0)
	if err != nil {
		return "", false, err
	}
	if component && !sqliteValidComponentID(id) || !component && !validUUID(id) {
		return "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	return id, true, nil
}

func sqliteFinalizationIntervalOwner(tx *sqliteio.Tx, segmentID string) (id string, found bool, err error) {
	s, err := tx.Prepare("SELECT interval_id,ordinal,segment_id FROM interval_segments WHERE segment_id=?", sqliteio.Text(segmentID))
	if err != nil {
		return "", false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			id = ""
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", false, err
	}
	if s.ColumnCount() != 3 {
		return "", false, failure("state_corrupt")
	}
	id, err = sqliteDependencyText(s, 0)
	if err != nil {
		return "", false, err
	}
	ordinal, err := sqliteLocalRangeInteger(s, 1)
	if err != nil {
		return "", false, err
	}
	selected, err := sqliteDependencyText(s, 2)
	if err != nil {
		return "", false, err
	}
	if !validUUID(id) || ordinal < 0 || selected != segmentID {
		return "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	return id, true, nil
}

func sqliteFinalizationSealOwner(tx *sqliteio.Tx, componentID string) (id string, found bool, err error) {
	s, err := tx.Prepare("SELECT interval_id,component_id FROM interval_components WHERE component_id=?", sqliteio.Text(componentID))
	if err != nil {
		return "", false, err
	}
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			id = ""
			found = false
		}
	}()
	present, err := s.Step()
	if err != nil || !present {
		return "", false, err
	}
	if s.ColumnCount() != 2 {
		return "", false, failure("state_corrupt")
	}
	id, err = sqliteDependencyText(s, 0)
	if err != nil {
		return "", false, err
	}
	selected, err := sqliteDependencyText(s, 1)
	if err != nil {
		return "", false, err
	}
	if !validUUID(id) || selected != componentID {
		return "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	return id, true, nil
}

// Under the preserved disjoint-family invariant, the nearest OTHER row is a
// complete conflict witness. Only this PK is excluded, including within a Tx.
func sqliteFinalizationFrontierMaximal(tx *sqliteio.Tx, row sqliteFrontierLocalRow) error {
	a := row.Attribution
	s, err := tx.Prepare("SELECT component_id FROM union_frontier WHERE computer_id=? AND account_id=? AND user_id=? AND project_id=? AND task_id=? AND timezone=? AND (start_sec,start_nsec)<=(?,?) AND component_id<>? ORDER BY start_sec DESC,start_nsec DESC,end_sec DESC,end_nsec DESC LIMIT 1",
		sqliteio.Text(row.ComputerID), sqliteio.Text(a.AccountID), sqliteio.Text(a.UserID), sqliteio.Text(a.ProjectID), sqliteio.Text(a.TaskID), sqliteio.Text(a.Timezone), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())), sqliteio.Text(row.ID))
	if err != nil {
		return err
	}
	id, found, err := sqliteFinalizationSelectedID(s, true)
	if err != nil || !found {
		return err
	}
	other, found, err := sqliteReadFrontierLocal(tx, row.ComputerID, id)
	if err != nil {
		return err
	}
	if !found || id == row.ID || other.Attribution != a || sqliteCompareRangeTime(other.Start, row.End) > 0 || sqliteCompareRangeTime(other.End, row.Start) >= 0 {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteFinalizationIntervalDisjoint(tx *sqliteio.Tx, row sqliteIntervalLocalRow) error {
	s, err := tx.Prepare("SELECT interval_id FROM intervals WHERE computer_id=? AND account_id=? AND project_id=? AND (start_sec,start_nsec)<(?,?) AND interval_id<>? ORDER BY start_sec DESC,start_nsec DESC,end_sec DESC,end_nsec DESC LIMIT 1",
		sqliteio.Text(row.ComputerID), sqliteio.Text(row.Attribution.AccountID), sqliteio.Text(row.Attribution.ProjectID), sqliteio.Integer(row.End.Unix()), sqliteio.Integer(int64(row.End.Nanosecond())), sqliteio.Text(row.ID))
	if err != nil {
		return err
	}
	id, found, err := sqliteFinalizationSelectedID(s, false)
	if err != nil || !found {
		return err
	}
	other, found, err := sqliteReadIntervalLocal(tx, row.ComputerID, id)
	if err != nil {
		return err
	}
	if !found || id == row.ID || other.Attribution.AccountID != row.Attribution.AccountID || other.Attribution.ProjectID != row.Attribution.ProjectID ||
		sqliteCompareRangeTime(other.Start, row.End) >= 0 || sqliteCompareRangeTime(other.End, row.Start) > 0 {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteFinalizationNoMembers(tx *sqliteio.Tx, id string) (err error) {
	s, err := tx.Prepare("SELECT component_id,segment_id FROM component_segments WHERE component_id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	defer func() { err = sqliteCloseMetaStatement(s, err) }()
	any := false
	for {
		present, readErr := s.Step()
		if readErr != nil {
			return readErr
		}
		if !present {
			break
		}
		owner, _, readErr := sqliteDecodeComponentMemberLocal(s)
		if readErr != nil {
			return readErr
		}
		if owner != id {
			return failure("state_corrupt")
		}
		any = true
	}
	if any {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteFinalizationRemoved(tx *sqliteio.Tx, computer, id string) error {
	if _, found, err := sqliteReadFrontierLocal(tx, computer, id); err != nil {
		return err
	} else if found {
		return failure("state_corrupt")
	}
	if _, found, err := sqliteReadPendingLocal(tx, computer, id); err != nil {
		return err
	} else if found {
		return failure("state_corrupt")
	}
	return sqliteFinalizationNoMembers(tx, id)
}

func sqliteFinalizationNoPlan(tx *sqliteio.Tx, id string) error {
	s, err := tx.Prepare("SELECT interval_id FROM sync_plans WHERE interval_id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	selected, found, err := sqliteFinalizationSelectedID(s, false)
	if err != nil {
		return err
	}
	if found || selected != "" {
		return failure("state_corrupt")
	}
	return nil
}

// Retained intervals use current locally valid roots; only NewSeals impose the
// initial queued shape. Their later full sync graph remains a separate gate.
func sqliteReadFinalizationInterval(tx *sqliteio.Tx, computer, id string) ([]sqliteSegmentLocalRow, string, sqliteOutboxLocalRow, error) {
	row, found, err := sqliteReadIntervalLocal(tx, computer, id)
	if err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	if !found {
		return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	ids, err := sqliteIntervalSegmentIDsLocal(tx, computer, id)
	if err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	supports := make([]sqliteSegmentLocalRow, 0, len(ids))
	for index, segmentID := range ids {
		if index > 0 && ids[index-1] >= segmentID {
			return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
		}
		segment, present, readErr := sqliteReadSegmentLocal(tx, computer, segmentID)
		if readErr != nil {
			return nil, "", sqliteOutboxLocalRow{}, readErr
		}
		if !present || !segment.Finalized || segment.Binding.Attribution != row.Attribution {
			return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
		}
		if readErr = sqliteValidateSegmentDependencies(tx, computer, segment); readErr != nil {
			return nil, "", sqliteOutboxLocalRow{}, readErr
		}
		owner, hasOwner, readErr := sqliteFinalizationIntervalOwner(tx, segmentID)
		if readErr != nil {
			return nil, "", sqliteOutboxLocalRow{}, readErr
		}
		if !hasOwner || owner != id {
			return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
		}
		if _, present, readErr = sqliteComponentForSegmentLocal(tx, computer, segmentID); readErr != nil {
			return nil, "", sqliteOutboxLocalRow{}, readErr
		} else if present {
			return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
		}
		supports = append(supports, segment)
	}
	builds, err := sqliteBuildFrontiers(computer, supports)
	if err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	if len(builds) != 1 || builds[0].Row.Attribution != row.Attribution || !sqliteFinalizationSameTime(builds[0].Row.Start, row.Start) || !sqliteFinalizationSameTime(builds[0].Row.End, row.End) {
		return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	component, present, err := sqliteReadIntervalComponent(tx, computer, id)
	if err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	if !present || component != builds[0].Row.ID {
		return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	owner, present, err := sqliteFinalizationSealOwner(tx, component)
	if err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	if !present || owner != id {
		return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	if err = sqliteFinalizationRemoved(tx, computer, component); err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	root, present, err := sqliteReadOutboxLocal(tx, computer, id)
	if err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	if !present {
		return nil, "", sqliteOutboxLocalRow{}, failure("state_corrupt")
	}
	if err = sqliteFinalizationIntervalDisjoint(tx, row); err != nil {
		return nil, "", sqliteOutboxLocalRow{}, err
	}
	return supports, component, root, nil
}

func sqliteValidateSelectedFinalization(tx *sqliteio.Tx, computer, revisionCeiling string, selected sqliteFinalizationSelection) error {
	if !sqliteDependencyScope(computer, revisionCeiling) {
		return failure("validation")
	}
	live := make(map[string]bool)
	required := make(map[string]bool)
	cleared := make(map[string]bool)
	removed := make(map[string]bool)
	for _, list := range [][]string{selected.FrontierIDs, selected.RequiredPendingIDs, selected.ClearedPendingIDs, selected.RemovedFrontierIDs} {
		for _, id := range list {
			if !sqliteValidComponentID(id) {
				return failure("validation")
			}
		}
	}
	for _, id := range selected.FrontierIDs {
		live[id] = true
	}
	for _, id := range selected.RequiredPendingIDs {
		required[id] = true
		live[id] = true
	}
	for _, id := range selected.ClearedPendingIDs {
		cleared[id] = true
		live[id] = true
	}
	for _, id := range selected.RemovedFrontierIDs {
		removed[id] = true
	}
	seals := make(map[string]sqliteSealSelection)
	sealComponents := make(map[string]string)
	intervals := make(map[string]bool)
	dependencies := make(map[string]bool)
	for _, seal := range selected.NewSeals {
		if !validUUID(seal.IntervalID) || !validUUID(seal.OutboxID) || !sqliteValidComponentID(seal.ComponentID) || seal.IntervalID == seal.OutboxID {
			return failure("validation")
		}
		if previous, exists := seals[seal.IntervalID]; exists && previous != seal {
			return failure("state_corrupt")
		}
		if previous, exists := sealComponents[seal.ComponentID]; exists && previous != seal.IntervalID {
			return failure("state_corrupt")
		}
		seals[seal.IntervalID] = seal
		sealComponents[seal.ComponentID] = seal.IntervalID
		intervals[seal.IntervalID] = true
		removed[seal.ComponentID] = true
	}
	for id := range live {
		if removed[id] || required[id] && cleared[id] {
			return failure("state_corrupt")
		}
	}
	for _, id := range selected.ChangedSegmentIDs {
		if !validUUID(id) {
			return failure("validation")
		}
		if dependencies[id] {
			continue
		}
		dependencies[id] = true
		segment, found, err := sqliteReadSegmentLocal(tx, computer, id)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
		component, hasLive, err := sqliteComponentForSegmentLocal(tx, computer, id)
		if err != nil {
			return err
		}
		interval, hasInterval, err := sqliteFinalizationIntervalOwner(tx, id)
		if err != nil {
			return err
		}
		if segment.Finalized {
			if hasLive || !hasInterval {
				return failure("state_corrupt")
			}
			intervals[interval] = true
		} else if sqliteFinalizationEligible(segment) {
			if !hasLive || hasInterval {
				return failure("state_corrupt")
			}
			live[component] = true
		} else if hasLive || hasInterval {
			return failure("state_corrupt")
		}
	}
	for id := range live {
		if removed[id] {
			return failure("state_corrupt")
		}
		component, err := sqliteReadFinalizationComponent(tx, computer, id, nil)
		if err != nil {
			return err
		}
		if required[id] && component.Pending == nil || cleared[id] && component.Pending != nil {
			return failure("state_corrupt")
		}
		if err = sqliteFinalizationFrontierMaximal(tx, component.Row); err != nil {
			return err
		}
		for _, segment := range component.Segments {
			dependencies[segment.ID] = true
		}
	}
	for id := range intervals {
		supports, component, root, err := sqliteReadFinalizationInterval(tx, computer, id)
		if err != nil {
			return err
		}
		for _, segment := range supports {
			dependencies[segment.ID] = true
		}
		if seal, exists := seals[id]; exists {
			if component != seal.ComponentID || root.ID != seal.OutboxID || root.Revision != "1" || root.State != "queued" || root.EntryID != nil || root.FailureCategory != nil || root.RetryRequestID != nil || root.RunRequestID != nil || root.PlanPresent {
				return failure("state_corrupt")
			}
			if err = sqliteFinalizationNoPlan(tx, id); err != nil {
				return err
			}
		}
	}
	for id := range removed {
		if err := sqliteFinalizationRemoved(tx, computer, id); err != nil {
			return err
		}
	}
	changed := make([]string, 0, len(dependencies))
	for id := range dependencies {
		changed = append(changed, id)
	}
	return sqliteValidateSelectedCaptureDependencies(tx, computer, revisionCeiling, sqliteDependencySelection{ChangedSegmentIDs: changed})
}
