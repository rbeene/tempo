//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type sqliteFrontierMutationResult struct {
	Delta     int64
	Selection sqliteFinalizationSelection
}

// These are owned, affected supports, never a partial legacy state.
type sqliteFrontierBuild struct {
	Row      sqliteFrontierLocalRow
	Segments []sqliteSegmentLocalRow
}

type sqliteFinalizationComponent struct {
	sqliteFrontierBuild
	Pending *sqlitePendingLocalRow
}

func sqliteFinalizationAdd(total *int64, delta int64, err error) error {
	if err != nil {
		return err
	}
	if delta > 0 && *total > math.MaxInt64-delta || delta < 0 && *total < math.MinInt64-delta {
		return failure("state_corrupt")
	}
	*total += delta
	return nil
}

func sqliteFinalizationEnd(row sqliteSegmentLocalRow) time.Time {
	if row.End != nil {
		return *row.End
	}
	return row.Confirmed
}

func sqliteFinalizationEligible(row sqliteSegmentLocalRow) bool {
	return !row.Finalized && (row.End != nil || row.UncertaintyID != nil) && sqliteCompareRangeTime(sqliteFinalizationEnd(row), row.Start) > 0
}

func sqliteFrontierComponentID(computer string, a Attribution, minimum string) string {
	h := sha256.New()
	_, _ = h.Write([]byte("tempo-frontier-component-v1\x00"))
	for _, value := range []string{computer, a.AccountID, a.UserID, a.ProjectID, a.TaskID, a.Timezone, minimum} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	return "fc1:" + hex.EncodeToString(h.Sum(nil))
}

// Match mergeRanges' retention rule. UUID resolves only truly equal ranges.
// Finalized supports are allowed here for the immutable interval proof.
func sqliteBuildFrontiers(computer string, supports []sqliteSegmentLocalRow) ([]sqliteFrontierBuild, error) {
	rows := append([]sqliteSegmentLocalRow(nil), supports...)
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		if !validUUID(row.ID) || row.Actor.Key.ComputerID != computer || !validAttribution(row.Binding.Attribution) || seen[row.ID] ||
			row.End == nil && row.UncertaintyID == nil || sqliteCompareRangeTime(sqliteFinalizationEnd(row), row.Start) <= 0 {
			return nil, failure("state_corrupt")
		}
		seen[row.ID] = true
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		ga, gb := attributionKey(computer, a.Binding.Attribution), attributionKey(computer, b.Binding.Attribution)
		if ga != gb {
			return ga < gb
		}
		if order := sqliteCompareRangeTime(a.Start, b.Start); order != 0 {
			return order < 0
		}
		if order := sqliteCompareRangeTime(sqliteFinalizationEnd(a), sqliteFinalizationEnd(b)); order != 0 {
			return order < 0
		}
		return a.ID < b.ID
	})
	result := make([]sqliteFrontierBuild, 0)
	for _, row := range rows {
		end := sqliteFinalizationEnd(row)
		if len(result) == 0 || result[len(result)-1].Row.Attribution != row.Binding.Attribution || sqliteCompareRangeTime(row.Start, result[len(result)-1].Row.End) > 0 {
			result = append(result, sqliteFrontierBuild{Row: sqliteFrontierLocalRow{ComputerID: computer, Attribution: row.Binding.Attribution, Start: row.Start, End: end}, Segments: []sqliteSegmentLocalRow{row}})
		} else {
			last := &result[len(result)-1]
			if sqliteCompareRangeTime(end, last.Row.End) > 0 {
				last.Row.End = end
			}
			last.Segments = append(last.Segments, row)
		}
	}
	for i := range result {
		sort.Slice(result[i].Segments, func(a, b int) bool { return result[i].Segments[a].ID < result[i].Segments[b].ID })
		result[i].Row.ID = sqliteFrontierComponentID(computer, result[i].Row.Attribution, result[i].Segments[0].ID)
	}
	return result, nil
}

func sqliteFinalizationSameTime(a, b time.Time) bool {
	x, xe := a.MarshalJSON()
	y, ye := b.MarshalJSON()
	return xe == nil && ye == nil && string(x) == string(y) && sqliteCompareRangeTime(a, b) == 0
}

func sqliteFinalizationSameFrontier(a, b sqliteFrontierLocalRow) bool {
	return a.ID == b.ID && a.ComputerID == b.ComputerID && a.Attribution == b.Attribution && sqliteFinalizationSameTime(a.Start, b.Start) && sqliteFinalizationSameTime(a.End, b.End)
}

func sqliteFinalizationPending(row sqliteFrontierLocalRow) sqlitePendingLocalRow {
	return sqlitePendingLocalRow{ID: row.ID, ComputerID: row.ComputerID, Attribution: row.Attribution, Start: row.Start, End: row.End}
}

func sqliteFinalizationPendingMatches(p sqlitePendingLocalRow, row sqliteFrontierLocalRow) bool {
	return p.ID == row.ID && p.ComputerID == row.ComputerID && p.Attribution == row.Attribution && sqliteFinalizationSameTime(p.Start, row.Start) && sqliteFinalizationSameTime(p.End, row.End)
}

// Read and prove an entire old/live owner before any derived removal. Only the
// just-CASed segment may use its exact prior scalar; all peers are current SQL.
func sqliteReadFinalizationComponent(tx *sqliteio.Tx, computer, id string, prior *sqliteSegmentLocalRow) (sqliteFinalizationComponent, error) {
	row, found, err := sqliteReadFrontierLocal(tx, computer, id)
	if err != nil {
		return sqliteFinalizationComponent{}, err
	}
	if !found {
		return sqliteFinalizationComponent{}, failure("state_corrupt")
	}
	ids, err := sqliteComponentSegmentIDsLocal(tx, computer, id)
	if err != nil {
		return sqliteFinalizationComponent{}, err
	}
	supports := make([]sqliteSegmentLocalRow, 0, len(ids))
	substituted := prior == nil
	for _, segmentID := range ids {
		var segment sqliteSegmentLocalRow
		if prior != nil && segmentID == prior.ID {
			segment = *prior
			substituted = true
		} else {
			segment, found, err = sqliteReadSegmentLocal(tx, computer, segmentID)
			if err != nil {
				return sqliteFinalizationComponent{}, err
			}
			if !found {
				return sqliteFinalizationComponent{}, failure("state_corrupt")
			}
		}
		if !sqliteFinalizationEligible(segment) || segment.Binding.Attribution != row.Attribution {
			return sqliteFinalizationComponent{}, failure("state_corrupt")
		}
		if err = sqliteValidateSegmentDependencies(tx, computer, segment); err != nil {
			return sqliteFinalizationComponent{}, err
		}
		live, present, readErr := sqliteComponentForSegmentLocal(tx, computer, segmentID)
		if readErr != nil {
			return sqliteFinalizationComponent{}, readErr
		}
		if !present || live != id {
			return sqliteFinalizationComponent{}, failure("state_corrupt")
		}
		owner, present, readErr := sqliteFinalizationIntervalOwner(tx, segmentID)
		if readErr != nil {
			return sqliteFinalizationComponent{}, readErr
		}
		if present || owner != "" {
			return sqliteFinalizationComponent{}, failure("state_corrupt")
		}
		supports = append(supports, segment)
	}
	builds, err := sqliteBuildFrontiers(computer, supports)
	if err != nil {
		return sqliteFinalizationComponent{}, err
	}
	if !substituted || len(builds) != 1 || !sqliteFinalizationSameFrontier(row, builds[0].Row) {
		return sqliteFinalizationComponent{}, failure("state_corrupt")
	}
	if _, present, readErr := sqliteFinalizationSealOwner(tx, id); readErr != nil {
		return sqliteFinalizationComponent{}, readErr
	} else if present {
		return sqliteFinalizationComponent{}, failure("state_corrupt")
	}
	p, present, err := sqliteReadPendingLocal(tx, computer, id)
	if err != nil {
		return sqliteFinalizationComponent{}, err
	}
	result := sqliteFinalizationComponent{sqliteFrontierBuild: builds[0]}
	if present {
		if !sqliteFinalizationPendingMatches(p, row) {
			return sqliteFinalizationComponent{}, failure("state_corrupt")
		}
		result.Pending = &p
	}
	return result, nil
}

func sqliteRemoveFinalizationComponent(tx *sqliteio.Tx, computer string, row sqliteFinalizationComponent) (int64, error) {
	var delta int64
	for _, segment := range row.Segments {
		n, err := sqliteDeleteComponentSegmentLocal(tx, computer, row.Row.ID, segment.ID)
		if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
			return 0, err
		}
	}
	if row.Pending != nil {
		n, err := sqliteDeletePendingLocal(tx, computer, *row.Pending)
		if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
			return 0, err
		}
	}
	n, err := sqliteDeleteFrontierLocal(tx, computer, row.Row)
	if err = sqliteFinalizationAdd(&delta, n, err); err != nil {
		return 0, err
	}
	return delta, nil
}

func sqliteRefreshSegmentFrontier(tx *sqliteio.Tx, computer string, before *sqliteSegmentLocalRow, after sqliteSegmentLocalRow) (result sqliteFrontierMutationResult, err error) {
	defer func() {
		if err != nil {
			result = sqliteFrontierMutationResult{}
		}
	}()
	if !validUUID(computer) || after.Actor.Key.ComputerID != computer || before != nil && (before.ID != after.ID || before.Actor.Key.ComputerID != computer) {
		return result, failure("validation")
	}
	encoded, err := sqliteEncodeSegmentLocal(after)
	if err != nil {
		return result, err
	}
	if before != nil {
		if _, err = sqliteEncodeSegmentLocal(*before); err != nil {
			return result, err
		}
	}
	actual, found, err := sqliteReadSegmentLocal(tx, computer, after.ID)
	if err != nil {
		return result, err
	}
	if !found {
		return result, failure("state_corrupt")
	}
	stored, err := sqliteEncodeSegmentLocal(actual)
	if err != nil {
		return result, err
	}
	if !reflect.DeepEqual(encoded.values, stored.values) {
		return result, failure("state_corrupt")
	}
	after = actual
	if err = sqliteValidateSegmentDependencies(tx, computer, after); err != nil {
		return result, err
	}
	result.Selection = sqliteEmptyFinalizationSelection()
	affected := map[string]bool{after.ID: true}
	removed := make(map[string]bool)
	live := make(map[string]bool)
	owner, found, err := sqliteComponentForSegmentLocal(tx, computer, after.ID)
	if err != nil {
		return result, err
	}
	needsOld := before != nil && sqliteFinalizationEligible(*before)
	if found != needsOld {
		return result, failure("state_corrupt")
	}
	supports := make([]sqliteSegmentLocalRow, 0)
	if found {
		old, readErr := sqliteReadFinalizationComponent(tx, computer, owner, before)
		if readErr != nil {
			return result, readErr
		}
		if err = sqliteFinalizationFrontierMaximal(tx, old.Row); err != nil {
			return result, err
		}
		for _, segment := range old.Segments {
			affected[segment.ID] = true
			if segment.ID != after.ID {
				supports = append(supports, segment)
			}
		}
		n, removeErr := sqliteRemoveFinalizationComponent(tx, computer, old)
		if err = sqliteFinalizationAdd(&result.Delta, n, removeErr); err != nil {
			return result, err
		}
		removed[owner] = true
	}
	if sqliteFinalizationEligible(after) {
		supports = append(supports, after)
	}
	candidates, err := sqliteBuildFrontiers(computer, supports)
	if err != nil {
		return result, err
	}
	for _, candidate := range candidates {
		for {
			neighbor, present, readErr := sqliteFrontierPredecessorLocal(tx, computer, candidate.Row.Attribution, candidate.Row.Start)
			if readErr != nil {
				return result, readErr
			}
			if !present || sqliteCompareRangeTime(neighbor.End, candidate.Row.Start) < 0 {
				neighbor, present, readErr = sqliteFrontierSuccessorLocal(tx, computer, candidate.Row.Attribution, candidate.Row.Start)
				if readErr != nil {
					return result, readErr
				}
				if !present || sqliteCompareRangeTime(neighbor.Start, candidate.Row.End) > 0 {
					break
				}
			}
			old, readErr := sqliteReadFinalizationComponent(tx, computer, neighbor.ID, nil)
			if readErr != nil {
				return result, readErr
			}
			if err = sqliteFinalizationFrontierMaximal(tx, old.Row); err != nil {
				return result, err
			}
			n, removeErr := sqliteRemoveFinalizationComponent(tx, computer, old)
			if err = sqliteFinalizationAdd(&result.Delta, n, removeErr); err != nil {
				return result, err
			}
			removed[old.Row.ID] = true
			delete(live, old.Row.ID)
			for _, segment := range old.Segments {
				affected[segment.ID] = true
			}
			joined, joinErr := sqliteBuildFrontiers(computer, append(candidate.Segments, old.Segments...))
			if joinErr != nil {
				return result, joinErr
			}
			if len(joined) != 1 {
				return result, failure("state_corrupt")
			}
			candidate = joined[0]
		}
		n, writeErr := sqliteWriteFrontierLocal(tx, computer, nil, candidate.Row)
		if err = sqliteFinalizationAdd(&result.Delta, n, writeErr); err != nil {
			return result, err
		}
		for _, segment := range candidate.Segments {
			affected[segment.ID] = true
			n, writeErr = sqliteInsertComponentSegmentLocal(tx, computer, candidate.Row.ID, segment.ID)
			if err = sqliteFinalizationAdd(&result.Delta, n, writeErr); err != nil {
				return result, err
			}
		}
		n, writeErr = sqliteWritePendingLocal(tx, computer, nil, sqliteFinalizationPending(candidate.Row))
		if err = sqliteFinalizationAdd(&result.Delta, n, writeErr); err != nil {
			return result, err
		}
		live[candidate.Row.ID] = true
	}
	for id := range affected {
		result.Selection.ChangedSegmentIDs = append(result.Selection.ChangedSegmentIDs, id)
	}
	for id := range live {
		result.Selection.FrontierIDs = append(result.Selection.FrontierIDs, id)
		result.Selection.RequiredPendingIDs = append(result.Selection.RequiredPendingIDs, id)
	}
	for id := range removed {
		if !live[id] {
			result.Selection.RemovedFrontierIDs = append(result.Selection.RemovedFrontierIDs, id)
		}
	}
	sort.Strings(result.Selection.ChangedSegmentIDs)
	sort.Strings(result.Selection.FrontierIDs)
	sort.Strings(result.Selection.RequiredPendingIDs)
	sort.Strings(result.Selection.RemovedFrontierIDs)
	return result, nil
}

func sqliteInvalidateReservation(tx *sqliteio.Tx, timer sqliteTimerKey, before, after *sqliteReservationWindow) (result sqliteFrontierMutationResult, err error) {
	defer func() {
		if err != nil {
			result = sqliteFrontierMutationResult{}
		}
	}()
	if !sqliteValidTimerKey(timer) {
		return result, failure("validation")
	}
	for _, window := range []*sqliteReservationWindow{before, after} {
		if window != nil && window.End != nil && sqliteCompareRangeTime(*window.End, window.Start) < 0 {
			return result, failure("validation")
		}
	}
	result.Selection = sqliteEmptyFinalizationSelection()
	seen := make(map[string]bool)
	for _, window := range []*sqliteReservationWindow{before, after} {
		if window == nil {
			continue
		}
		rows, readErr := sqliteFrontierReservationRowsLocal(tx, timer, *window)
		if readErr != nil {
			return result, readErr
		}
		for _, row := range rows {
			if seen[row.ID] {
				continue
			}
			seen[row.ID] = true
			pending, found, readErr := sqliteReadPendingLocal(tx, timer.ComputerID, row.ID)
			if readErr != nil {
				return result, readErr
			}
			if found {
				if !sqliteFinalizationPendingMatches(pending, row) {
					return result, failure("state_corrupt")
				}
			} else {
				n, writeErr := sqliteWritePendingLocal(tx, timer.ComputerID, nil, sqliteFinalizationPending(row))
				if err = sqliteFinalizationAdd(&result.Delta, n, writeErr); err != nil {
					return result, err
				}
			}
			result.Selection.FrontierIDs = append(result.Selection.FrontierIDs, row.ID)
			result.Selection.RequiredPendingIDs = append(result.Selection.RequiredPendingIDs, row.ID)
		}
	}
	sort.Strings(result.Selection.FrontierIDs)
	sort.Strings(result.Selection.RequiredPendingIDs)
	return result, nil
}
