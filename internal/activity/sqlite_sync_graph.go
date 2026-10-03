//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/json"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// These readers certify owned selected graphs. They never take transaction,
// metadata, provider or current-binding ownership from their caller.
func sqliteReadRetainedSyncInterval(tx *sqliteio.Tx, computer, intervalID, revisionCeiling string) (Interval, bool, error) {
	if !sqliteDependencyScope(computer, revisionCeiling) || !validUUID(intervalID) {
		return Interval{}, false, failure("validation")
	}
	row, found, err := sqliteReadIntervalLocal(tx, computer, intervalID)
	if err != nil || !found {
		return Interval{}, false, err
	}
	supports, _, _, err := sqliteReadFinalizationInterval(tx, computer, intervalID)
	if err != nil {
		return Interval{}, false, err
	}
	ids := make([]string, 0, len(supports))
	for _, support := range supports {
		ids = append(ids, support.ID)
	}
	if err = sqliteValidateSelectedCaptureDependencies(tx, computer, revisionCeiling, sqliteDependencySelection{SegmentIDs: ids}); err != nil {
		return Interval{}, false, err
	}
	return Interval{ID: row.ID, ComputerID: row.ComputerID, Attribution: row.Attribution, Start: row.Start, End: row.End, DurationNS: row.DurationNS, SegmentIDs: ids}, true, nil
}

func sqliteReadSyncItem(tx *sqliteio.Tx, computer, rootID, revisionCeiling string) (OutboxItem, bool, error) {
	if !sqliteDependencyScope(computer, revisionCeiling) || !validUUID(rootID) {
		return OutboxItem{}, false, failure("validation")
	}
	requests := make(map[string]mutationRequest)
	item, found, err := sqliteSyncGraphItem(tx, computer, rootID, revisionCeiling, requests)
	if err != nil || !found {
		return OutboxItem{}, false, err
	}
	if err = sqliteSyncGraphPendingSingleton(tx, computer, revisionCeiling, requests); err != nil {
		return OutboxItem{}, false, err
	}
	return item, true, nil
}

func sqliteValidateSelectedSync(tx *sqliteio.Tx, computer, revisionCeiling string, rootIDs, requestIDs []string) error {
	if !sqliteDependencyScope(computer, revisionCeiling) {
		return failure("validation")
	}
	roots := make(map[string]bool)
	explicit := make(map[string]bool)
	for _, list := range [][]string{rootIDs, requestIDs} {
		for _, id := range list {
			if !validUUID(id) {
				return failure("validation")
			}
		}
	}
	for _, id := range rootIDs {
		roots[id] = true
	}
	for _, id := range requestIDs {
		explicit[id] = true
	}
	requests := make(map[string]mutationRequest)
	for _, id := range sqliteSyncGraphSortedIDs(explicit) {
		r, err := sqliteSyncGraphRequest(tx, computer, id, revisionCeiling, requests)
		if err != nil {
			return err
		}
		for _, root := range sqliteSyncGraphRequestRoots(r) {
			roots[root] = true
		}
		// Changed receipt seeds always expand all four reverse families. A
		// terminal result's UUID-only AffectedIDs are not forward root edges.
		for role := 0; role < 4; role++ {
			owners, err := sqliteSyncGraphRequestOwners(tx, id, role)
			if err != nil {
				return err
			}
			for _, owner := range owners {
				root, err := sqliteSyncGraphResolveOwner(tx, computer, id, role, owner)
				if err != nil {
					return err
				}
				roots[root] = true
			}
		}
	}
	for _, id := range sqliteSyncGraphSortedIDs(roots) {
		_, found, err := sqliteSyncGraphItem(tx, computer, id, revisionCeiling, requests)
		if err != nil {
			return err
		}
		if !found {
			return failure("state_corrupt")
		}
	}
	return sqliteSyncGraphPendingSingleton(tx, computer, revisionCeiling, requests)
}

func sqliteSyncGraphSortedIDs(ids map[string]bool) []string {
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

// The cache holds only complete checked request units. Endpoint reads here are
// local roots, never another complete item or a changed-request expansion.
func sqliteSyncGraphRequest(tx *sqliteio.Tx, computer, id, ceiling string, cache map[string]mutationRequest) (mutationRequest, error) {
	if r, found := cache[id]; found {
		return r, nil
	}
	row, found, err := sqliteReadMutationRequestLocal(tx, computer, id, ceiling)
	if err != nil {
		return mutationRequest{}, err
	}
	if !found || !syncOperation(row.Value.Operation) {
		return mutationRequest{}, failure("state_corrupt")
	}
	r := row.Value
	for _, rootID := range sqliteSyncGraphRequestRoots(r) {
		_, found, err := sqliteReadOutboxByIDLocal(tx, computer, rootID)
		if err != nil {
			return mutationRequest{}, err
		}
		if !found {
			return mutationRequest{}, failure("state_corrupt")
		}
	}
	if r.SyncConfigurationResult != nil {
		old := r.SyncConfigurationResult.Configuration
		current, found, err := sqliteReadSyncConfiguration(tx, old.AccountID, old.UserID)
		if err != nil {
			return mutationRequest{}, err
		}
		a, _ := counter(current.Revision)
		b, _ := counter(old.Revision)
		if !found || a < b {
			return mutationRequest{}, failure("state_corrupt")
		}
	}
	cache[id] = r
	return r, nil
}

func sqliteSyncGraphRequestRoots(r mutationRequest) []string {
	result := make([]string, 0)
	if r.PendingSync != nil {
		result = append(result, r.PendingSync.RootIDs...)
	}
	if r.SyncRun != nil {
		result = append(result, r.SyncRun.AttemptedIDs...)
		result = append(result, r.SyncRun.ResolvedIDs...)
		result = append(result, r.SyncRun.BlockedIDs...)
	}
	return result
}

func sqliteSyncGraphContains(ids []string, id string) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func sqliteSyncGraphResolveReceipt(r mutationRequest, root string) bool {
	return r.Operation == "sync.resolve" && r.MutationResult != nil &&
		len(r.MutationResult.AffectedIDs) == 1 && r.MutationResult.AffectedIDs[0] == root
}

func sqliteSyncGraphItem(tx *sqliteio.Tx, computer, rootID, ceiling string, requests map[string]mutationRequest) (OutboxItem, bool, error) {
	root, found, err := sqliteReadOutboxByIDLocal(tx, computer, rootID)
	if err != nil || !found {
		return OutboxItem{}, false, err
	}
	in, found, err := sqliteReadRetainedSyncInterval(tx, computer, root.IntervalID, ceiling)
	if err != nil {
		return OutboxItem{}, false, err
	}
	if !found {
		return OutboxItem{}, false, failure("state_corrupt")
	}
	o := OutboxItem{ID: root.ID, Revision: root.Revision, Interval: in, State: root.State, Correlation: root.Correlation, EntryID: root.EntryID, FailureCategory: root.FailureCategory, RetryRequestID: root.RetryRequestID, RunRequestID: root.RunRequestID}
	header, hasPlan, err := sqliteReadSyncPlanLocal(tx, computer, root.IntervalID)
	if err != nil {
		return OutboxItem{}, false, err
	}
	if root.PlanPresent != hasPlan {
		return OutboxItem{}, false, failure("state_corrupt")
	}
	if hasPlan {
		parts, err := sqliteSyncPartsLocal(tx, computer, root.IntervalID)
		if err != nil {
			return OutboxItem{}, false, err
		}
		if len(parts) == 0 || len(parts) > 100 {
			return OutboxItem{}, false, failure("state_corrupt")
		}
		o.Plan = &SyncPlan{Configuration: header.Configuration, CompanySource: header.CompanySource, Parts: make([]SyncPart, 0, len(parts))}
		for _, row := range parts {
			attempts, err := sqliteSyncAttemptsLocal(tx, computer, root.IntervalID, row.Ordinal)
			if err != nil {
				return OutboxItem{}, false, err
			}
			part := SyncPart{ID: row.ID, SpentDate: row.SpentDate, DurationNS: row.DurationNS, Start: row.Start, End: row.End,
				PlannedHours: row.PlannedHours, PlannedDurationNS: row.PlannedDurationNS, PlannedResidualNS: row.PlannedResidualNS,
				StartedTime: row.StartedTime, EndedTime: row.EndedTime, Correlation: row.Correlation, Notes: row.Notes, State: row.State,
				EntryID: row.EntryID, FailureCategory: row.FailureCategory, ReturnedHours: row.ReturnedHours, RoundedHours: row.RoundedHours,
				ConfirmedDurationNS: row.ConfirmedDurationNS, ProviderDeltaNS: row.ProviderDeltaNS, TotalResidualNS: row.TotalResidualNS,
				Attachment: row.Attachment, Attempts: make([]SyncAttempt, 0, len(attempts))}
			for _, attempt := range attempts {
				part.Attempts = append(part.Attempts, attempt.Value)
			}
			o.Plan.Parts = append(o.Plan.Parts, part)
		}
	} else {
		s, err := tx.Prepare("SELECT interval_id,ordinal FROM sync_parts WHERE interval_id=? LIMIT 1", sqliteio.Text(root.IntervalID))
		if err != nil {
			return OutboxItem{}, false, err
		}
		if err = sqliteSyncPlanEmptyChildren(s, root.IntervalID, false); err != nil {
			return OutboxItem{}, false, err
		}
		s, err = tx.Prepare("SELECT interval_id,part_ordinal,ordinal FROM sync_attempts WHERE interval_id=? LIMIT 1", sqliteio.Text(root.IntervalID))
		if err != nil {
			return OutboxItem{}, false, err
		}
		if err = sqliteSyncPlanEmptyChildren(s, root.IntervalID, true); err != nil {
			return OutboxItem{}, false, err
		}
	}
	if err = sqliteSyncGraphItemDependencies(tx, computer, ceiling, o, requests); err != nil {
		return OutboxItem{}, false, err
	}
	if err = sqliteSyncGraphPendingMemberships(tx, computer, ceiling, o.ID, requests); err != nil {
		return OutboxItem{}, false, err
	}
	return o, true, nil
}

// Direct predicates over a complete item, not a fabricated legacy State.
func sqliteSyncGraphItemDependencies(tx *sqliteio.Tx, computer, ceiling string, o OutboxItem, requests map[string]mutationRequest) error {
	if o.RetryRequestID != nil {
		r, err := sqliteSyncGraphRequest(tx, computer, *o.RetryRequestID, ceiling, requests)
		if err != nil {
			return err
		}
		if !sqliteSyncGraphResolveReceipt(r, o.ID) || o.State != "queued" && o.State != "submitting" {
			return failure("state_corrupt")
		}
	}
	if o.RunRequestID != nil {
		r, err := sqliteSyncGraphRequest(tx, computer, *o.RunRequestID, ceiling, requests)
		if err != nil {
			return err
		}
		if o.State != "submitting" || o.Plan == nil || r.PendingSync == nil || r.PendingSync.Run == nil || !sqliteSyncGraphContains(r.PendingSync.RootIDs, o.ID) {
			return failure("state_corrupt")
		}
	} else if o.State == "submitting" {
		return failure("state_corrupt")
	}
	if o.Plan == nil {
		if o.EntryID != nil || o.State == "synced" || o.State == "rejected" || o.State == "unknown" {
			return failure("state_corrupt")
		}
		return nil
	}
	plan := o.Plan
	if !validSyncConfiguration(plan.Configuration) || plan.Configuration.AccountID != o.Interval.Attribution.AccountID || plan.Configuration.UserID != o.Interval.Attribution.UserID {
		return failure("state_corrupt")
	}
	loc, err := time.LoadLocation(o.Interval.Attribution.Timezone)
	if err != nil {
		return failure("state_corrupt")
	}
	ids := map[string]bool{o.ID: true}
	cursor := o.Interval.Start
	attempted := false
	for _, p := range plan.Parts {
		if ids[p.ID] || !sqliteSyncGraphPartFacts(o, p, cursor, loc) {
			return failure("state_corrupt")
		}
		ids[p.ID] = true
		if err = sqliteSyncGraphPartIDUnused(tx, p.ID); err != nil {
			return err
		}
		cursor = p.End
		if p.State == "queued" {
			if len(p.Attempts) != 0 || p.EntryID != nil || p.FailureCategory != nil {
				return failure("state_corrupt")
			}
		} else if len(p.Attempts) == 0 && p.Attachment == nil {
			return failure("state_corrupt")
		}
		if p.State == "submitting" && o.RunRequestID == nil {
			return failure("state_corrupt")
		}
		for i, a := range p.Attempts {
			if ids[a.ID] || a.Number != strconv.Itoa(i+1) || a.State == "queued" {
				return failure("state_corrupt")
			}
			ids[a.ID] = true
			if err = sqliteSyncGraphAttemptIDUnused(tx, a.ID); err != nil {
				return err
			}
			r, err := sqliteSyncGraphRequest(tx, computer, a.RequestID, ceiling, requests)
			if err != nil {
				return err
			}
			if r.Operation != "sync.now" {
				return failure("state_corrupt")
			}
			var roots []string
			if r.PendingSync != nil && r.PendingSync.Run != nil {
				roots = r.PendingSync.RootIDs
			} else if r.SyncRun != nil {
				roots = r.SyncRun.AttemptedIDs
			} else {
				return failure("state_corrupt")
			}
			if !sqliteSyncGraphContains(roots, o.ID) || i < len(p.Attempts)-1 && a.State != "rejected" {
				return failure("state_corrupt")
			}
			if i == len(p.Attempts)-1 && (a.State != p.State || !reflect.DeepEqual(a.EntryID, p.EntryID) || !reflect.DeepEqual(a.FailureCategory, p.FailureCategory)) {
				return failure("state_corrupt")
			}
		}
		if p.Attachment != nil {
			r, err := sqliteSyncGraphRequest(tx, computer, p.Attachment.RequestID, ceiling, requests)
			if err != nil {
				return err
			}
			if p.EntryID == nil || *p.EntryID != p.Attachment.EntryID || !sqliteSyncGraphResolveReceipt(r, o.ID) {
				return failure("state_corrupt")
			}
		}
		if len(p.Attempts) > 0 || p.Attachment != nil {
			attempted = true
		}
	}
	if !cursor.Equal(o.Interval.End) {
		return failure("state_corrupt")
	}
	if !attempted {
		if o.State != "queued" && o.State != "needs_attention" || o.EntryID != nil {
			return failure("state_corrupt")
		}
	} else if o.State != "submitting" && o.RetryRequestID == nil {
		expected := o
		syncFinishItem(&expected)
		override := o.State == "needs_attention" && o.FailureCategory != nil && (*o.FailureCategory == "correlation_collision" || *o.FailureCategory == "response_mismatch") && expected.State != "synced"
		if !override && (expected.State != o.State || !reflect.DeepEqual(expected.EntryID, o.EntryID) || !reflect.DeepEqual(expected.FailureCategory, o.FailureCategory)) {
			return failure("state_corrupt")
		}
	}
	return nil
}

func sqliteSyncGraphPartFacts(o OutboxItem, p SyncPart, cursor time.Time, loc *time.Location) bool {
	if p.Correlation != "tempo:v1:"+p.ID || p.Notes != syncMarker(o, o.Plan.Configuration, p) || p.Attempts == nil ||
		!p.Start.Equal(cursor) || p.Start.Location() != time.UTC || p.End.Location() != time.UTC || !p.End.After(p.Start) || p.End.After(o.Interval.End) || p.SpentDate != p.Start.In(loc).Format("2006-01-02") {
		return false
	}
	boundary := syncNextDay(p.Start, loc)
	if boundary.IsZero() || p.End.After(boundary) || p.End.Before(o.Interval.End) && !p.End.Equal(boundary) {
		return false
	}
	n, ok := syncInt(p.DurationNS)
	if !ok || n <= 0 || n != int64(p.End.Sub(p.Start)) {
		return false
	}
	planned, ok := syncInt(p.PlannedDurationNS)
	if !ok || planned <= 0 {
		return false
	}
	residual, ok := syncInt(p.PlannedResidualNS)
	if !ok || new(big.Int).Sub(big.NewInt(n), big.NewInt(planned)).String() != strconv.FormatInt(residual, 10) {
		return false
	}
	_, hours := syncDecimal(json.Number(p.PlannedHours))
	if hours == nil || !syncRoundNS(hours).IsInt64() || syncRoundNS(hours).Int64() != planned {
		return false
	}
	if o.Plan.Configuration.DurationPolicy == "nearest-hundredth-hour" {
		q := n / 36000000000
		if n%36000000000 >= 18000000000 {
			q++
		}
		if planned != q*36000000000 || hours.Cmp(new(big.Rat).SetFrac(big.NewInt(q), big.NewInt(100))) != 0 {
			return false
		}
	} else if planned != n {
		return false
	}
	if o.Plan.Configuration.Mode == "duration" && (p.StartedTime != nil || p.EndedTime != nil) {
		return false
	}
	if o.Plan.Configuration.Mode == "timestamp" {
		if !syncTimestampSafe(p.Start, p.End, loc) || p.StartedTime == nil || p.EndedTime == nil {
			return false
		}
		layout := "15:04"
		if *o.Plan.Configuration.Clock == "12h" {
			layout = "3:04pm"
		}
		if *p.StartedTime != p.Start.In(loc).Format(layout) || *p.EndedTime != p.End.In(loc).Format(layout) {
			return false
		}
	}
	return validSyncAmounts(p)
}

// The only owner projections are the fixed 2/3/4-column indexed root, part
// and attempt tuples below. Decode and close the entire cursor before looking
// up any dependent row, including on a late DONE or finalize error.
type sqliteSyncGraphOwner struct {
	IntervalID, ID string
	Part, Attempt  int64
}

func sqliteSyncGraphOwners(s *sqliteio.Stmt, columns int) (result []sqliteSyncGraphOwner, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteSyncGraphOwner, 0)
	for {
		present, err := s.Step()
		if err != nil {
			return nil, err
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != columns {
			return nil, failure("state_corrupt")
		}
		row := sqliteSyncGraphOwner{}
		row.IntervalID, err = sqliteDependencyText(s, 0)
		if err != nil {
			return nil, err
		}
		row.ID, err = sqliteDependencyText(s, columns-1)
		if err != nil {
			return nil, err
		}
		if !validUUID(row.IntervalID) || !validUUID(row.ID) {
			return nil, failure("state_corrupt")
		}
		if columns >= 3 {
			row.Part, err = sqliteLocalRangeInteger(s, 1)
			if err != nil {
				return nil, err
			}
			if row.Part < 0 || row.Part >= 100 {
				return nil, failure("state_corrupt")
			}
		}
		if columns == 4 {
			row.Attempt, err = sqliteLocalRangeInteger(s, 2)
			if err != nil {
				return nil, err
			}
			if row.Attempt < 0 {
				return nil, failure("state_corrupt")
			}
		}
		result = append(result, row)
	}
}

func sqliteSyncGraphNoID(s *sqliteio.Stmt, columns int) error {
	rows, err := sqliteSyncGraphOwners(s, columns)
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteSyncGraphPartIDUnused(tx *sqliteio.Tx, id string) error {
	s, err := tx.Prepare("SELECT interval_id,id FROM outbox WHERE id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	if err = sqliteSyncGraphNoID(s, 2); err != nil {
		return err
	}
	s, err = tx.Prepare("SELECT interval_id,part_ordinal,ordinal,id FROM sync_attempts WHERE id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	return sqliteSyncGraphNoID(s, 4)
}

func sqliteSyncGraphAttemptIDUnused(tx *sqliteio.Tx, id string) error {
	s, err := tx.Prepare("SELECT interval_id,id FROM outbox WHERE id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	if err = sqliteSyncGraphNoID(s, 2); err != nil {
		return err
	}
	s, err = tx.Prepare("SELECT interval_id,ordinal,id FROM sync_parts WHERE id=?", sqliteio.Text(id))
	if err != nil {
		return err
	}
	return sqliteSyncGraphNoID(s, 3)
}

func sqliteSyncGraphRequestOwners(tx *sqliteio.Tx, request string, role int) ([]sqliteSyncGraphOwner, error) {
	var sql string
	columns := 2
	switch role {
	case 0:
		sql = "SELECT interval_id,id FROM outbox WHERE run_request_id=? ORDER BY id"
	case 1:
		sql = "SELECT interval_id,id FROM outbox WHERE retry_request_id=? ORDER BY id"
	case 2:
		sql = "SELECT interval_id,ordinal,id FROM sync_parts WHERE attachment_request_id=? ORDER BY interval_id,ordinal"
		columns = 3
	case 3:
		sql = "SELECT interval_id,part_ordinal,ordinal,id FROM sync_attempts WHERE request_id=? ORDER BY interval_id,part_ordinal,ordinal"
		columns = 4
	default:
		return nil, failure("state_corrupt")
	}
	s, err := tx.Prepare(sql, sqliteio.Text(request))
	if err != nil {
		return nil, err
	}
	return sqliteSyncGraphOwners(s, columns)
}

func sqliteSyncGraphResolveOwner(tx *sqliteio.Tx, computer, request string, role int, owner sqliteSyncGraphOwner) (string, error) {
	root, found, err := sqliteReadOutboxLocal(tx, computer, owner.IntervalID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", failure("state_corrupt")
	}
	switch role {
	case 0:
		if root.ID != owner.ID || root.RunRequestID == nil || *root.RunRequestID != request {
			return "", failure("state_corrupt")
		}
	case 1:
		if root.ID != owner.ID || root.RetryRequestID == nil || *root.RetryRequestID != request {
			return "", failure("state_corrupt")
		}
	case 2:
		part, found, err := sqliteReadSyncPartLocal(tx, computer, owner.IntervalID, owner.Part)
		if err != nil {
			return "", err
		}
		if !found || part.ID != owner.ID || part.Attachment == nil || part.Attachment.RequestID != request {
			return "", failure("state_corrupt")
		}
	case 3:
		attempts, err := sqliteSyncAttemptsLocal(tx, computer, owner.IntervalID, owner.Part)
		if err != nil {
			return "", err
		}
		if owner.Attempt >= int64(len(attempts)) {
			return "", failure("state_corrupt")
		}
		a := attempts[owner.Attempt]
		if a.Value.ID != owner.ID || a.Value.RequestID != request {
			return "", failure("state_corrupt")
		}
	default:
		return "", failure("state_corrupt")
	}
	return root.ID, nil
}

type sqliteSyncGraphPendingOwner struct {
	RequestID string
	Ordinal   int64
}

func sqliteSyncGraphPendingRows(s *sqliteio.Stmt, root string) (result []sqliteSyncGraphPendingOwner, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			result = nil
		}
	}()
	result = make([]sqliteSyncGraphPendingOwner, 0)
	for {
		present, err := s.Step()
		if err != nil {
			return nil, err
		}
		if !present {
			return result, nil
		}
		if s.ColumnCount() != 3 {
			return nil, failure("state_corrupt")
		}
		request, err := sqliteDependencyText(s, 0)
		if err != nil {
			return nil, err
		}
		ordinal, err := sqliteLocalRangeInteger(s, 1)
		if err != nil {
			return nil, err
		}
		id, err := sqliteDependencyText(s, 2)
		if err != nil {
			return nil, err
		}
		if !validUUID(request) || ordinal < 0 || id != root {
			return nil, failure("state_corrupt")
		}
		result = append(result, sqliteSyncGraphPendingOwner{request, ordinal})
	}
}

func sqliteSyncGraphPendingMemberships(tx *sqliteio.Tx, computer, ceiling, root string, requests map[string]mutationRequest) error {
	s, err := tx.Prepare("SELECT request_id,ordinal,outbox_id FROM pending_sync_roots WHERE outbox_id=? ORDER BY request_id,ordinal", sqliteio.Text(root))
	if err != nil {
		return err
	}
	owners, err := sqliteSyncGraphPendingRows(s, root)
	if err != nil {
		return err
	}
	for _, owner := range owners {
		r, err := sqliteSyncGraphRequest(tx, computer, owner.RequestID, ceiling, requests)
		if err != nil {
			return err
		}
		if r.PendingSync == nil || owner.Ordinal >= int64(len(r.PendingSync.RootIDs)) || r.PendingSync.RootIDs[owner.Ordinal] != root {
			return failure("state_corrupt")
		}
	}
	return nil
}

func sqliteSyncGraphPendingID(s *sqliteio.Stmt) (id string, found bool, err error) {
	defer func() {
		err = sqliteCloseMetaStatement(s, err)
		if err != nil {
			id, found = "", false
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
	if !validUUID(id) {
		return "", false, failure("state_corrupt")
	}
	if present, err = s.Step(); err != nil {
		return "", false, err
	} else if present {
		return "", false, failure("state_corrupt")
	}
	return id, true, nil
}

func sqliteSyncGraphPendingSingleton(tx *sqliteio.Tx, computer, ceiling string, requests map[string]mutationRequest) error {
	s, err := tx.Prepare("SELECT request_id FROM pending_sync WHERE singleton=1")
	if err != nil {
		return err
	}
	id, found, err := sqliteSyncGraphPendingID(s)
	if err != nil || !found {
		return err
	}
	r, err := sqliteSyncGraphRequest(tx, computer, id, ceiling, requests)
	if err != nil {
		return err
	}
	if r.PendingSync == nil {
		return failure("state_corrupt")
	}
	return nil
}
