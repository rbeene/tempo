package activity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
)

const syncHourNS int64 = 3600000000000

// Preflight checks use the immutable attribution, never the current default account.
func syncPreflight(ctx context.Context, p harvest.Provider, a Attribution, c SyncConfiguration) (string, error) {
	u, e := syncIdentity(ctx, p, a.AccountID)
	if e != nil {
		return "", e
	}
	if bindingID(u) != a.UserID {
		return "", failure("identity_conflict")
	}
	if c.Mode == "timestamp" {
		zone, _ := u["timezone"].(string)
		if !syncZoneMatches(zone, a.Timezone) {
			return "", failure("timezone")
		}
	}
	catalog, e := DiscoverAssignments(ctx, a.AccountID, p)
	if e != nil {
		return "", syncSafeError(e)
	}
	if catalog.UserID != a.UserID {
		return "", failure("identity_conflict")
	}
	assigned := false
	for _, project := range catalog.Projects {
		if project.ID == a.ProjectID {
			for _, task := range project.Tasks {
				if task.ID == a.TaskID {
					assigned = true
				}
			}
		}
	}
	if !assigned {
		return "", failure("assignment_unavailable")
	}
	company, e := p.Get(ctx, "/company")
	if e != nil {
		var he *harvest.Error
		if errors.As(e, &he) && he.Status == 403 && he.Code == "forbidden" {
			return "user_declared_fallback", nil
		}
		return "", syncSafeError(e)
	}
	active, ok := company["is_active"].(bool)
	if !ok || !active {
		return "", failure("company_unavailable")
	}
	timestamps, ok := company["wants_timestamp_timers"].(bool)
	if !ok {
		return "", failure("response")
	}
	if timestamps != (c.Mode == "timestamp") {
		return "", failure("mode_conflict")
	}
	if c.Mode == "timestamp" {
		clock, ok := company["clock"].(string)
		if !ok || c.Clock == nil || clock != *c.Clock {
			return "", failure("mode_conflict")
		}
	}
	return "company_verified", nil
}

func syncBuildPlan(o OutboxItem, c SyncConfiguration, source string) (*SyncPlan, error) {
	loc, e := time.LoadLocation(o.Interval.Attribution.Timezone)
	if e != nil {
		return nil, failure("timezone")
	}
	plan := &SyncPlan{Configuration: c, CompanySource: source, Parts: []SyncPart{}}
	if c.Mode == "timestamp" && !syncTimestampSafe(o.Interval.Start, o.Interval.End, loc) {
		return nil, failure("representation")
	}
	for cursor := o.Interval.Start; cursor.Before(o.Interval.End); {
		if len(plan.Parts) >= 100 {
			return nil, failure("representation")
		}
		end := syncNextDay(cursor, loc)
		if end.IsZero() || !end.After(cursor) {
			return nil, failure("representation")
		}
		if end.After(o.Interval.End) {
			end = o.Interval.End
		}
		n := int64(end.Sub(cursor))
		planned := n
		var hours string
		if c.DurationPolicy == "nearest-hundredth-hour" {
			q := n / 36000000000
			if n%36000000000 >= 18000000000 {
				q++
			}
			if q == 0 {
				return nil, failure("representation")
			}
			planned = q * 36000000000
			hours = new(big.Rat).SetFrac(big.NewInt(q), big.NewInt(100)).FloatString(2)
		} else {
			hours = new(big.Rat).SetFrac(big.NewInt(n), big.NewInt(syncHourNS)).FloatString(18)
		}
		hours = strings.TrimRight(strings.TrimRight(hours, "0"), ".")
		part := SyncPart{ID: newID(), SpentDate: cursor.In(loc).Format("2006-01-02"), DurationNS: strconv.FormatInt(n, 10), Start: cursor.UTC(), End: end.UTC(), PlannedHours: hours, PlannedDurationNS: strconv.FormatInt(planned, 10), PlannedResidualNS: strconv.FormatInt(n-planned, 10), State: "queued", Attempts: []SyncAttempt{}}
		if c.Mode == "timestamp" {
			layout := "15:04"
			if c.Clock != nil && *c.Clock == "12h" {
				layout = "3:04pm"
			}
			part.StartedTime = syncString(cursor.In(loc).Format(layout))
			part.EndedTime = syncString(end.In(loc).Format(layout))
		}
		part.Correlation = "tempo:v1:" + part.ID
		part.Notes = syncMarker(o, c, part)
		plan.Parts = append(plan.Parts, part)
		cursor = end
	}
	return plan, nil
}

// Walk actual zone periods rather than adding 24 hours or assuming every local
// calendar date exists. At each transition the new local date is checked first.
func syncNextDay(start time.Time, loc *time.Location) time.Time {
	date := start.In(loc).Format("2006-01-02")
	cursor := start
	for step := 0; step < 16; step++ {
		local := cursor.In(loc)
		if local.Format("2006-01-02") != date {
			return cursor.UTC()
		}
		y, m, d := local.Date()
		_, offset := local.Zone()
		// Midnight projected in this period's fixed offset; a prior transition may
		// change where that midnight actually falls, so it wins below.
		midnight := time.Date(y, m, d+1, 0, 0, 0, 0, time.UTC).Add(-time.Duration(offset) * time.Second)
		_, transition := local.ZoneBounds()
		next := midnight
		if !transition.IsZero() && transition.Before(next) {
			next = transition
		}
		if !next.After(cursor) {
			return time.Time{}
		}
		cursor = next
	}
	return time.Time{}
}

// syncMarkerInput is the versioned canonical correlation payload. It contains
// only immutable attribution, capture and representation, never response data.
type syncMarkerInput struct {
	Version           int         `json:"version"`
	RootID            string      `json:"root_id"`
	IntervalID        string      `json:"interval_id"`
	ComputerID        string      `json:"computer_id"`
	Attribution       Attribution `json:"attribution"`
	PartID            string      `json:"part_id"`
	Date              string      `json:"date"`
	Start             time.Time   `json:"start"`
	End               time.Time   `json:"end"`
	ExactNS           string      `json:"exact_ns"`
	PlannedHours      string      `json:"planned_hours"`
	PlannedNS         string      `json:"planned_ns"`
	PlannedResidualNS string      `json:"planned_residual_ns"`
	Mode              string      `json:"mode"`
	Policy            string      `json:"policy"`
	PolicyVersion     string      `json:"policy_version"`
	Clock             *string     `json:"clock"`
	StartedTime       *string     `json:"started_time"`
	EndedTime         *string     `json:"ended_time"`
}

func syncMarker(o OutboxItem, c SyncConfiguration, p SyncPart) string {
	raw, _ := json.Marshal(syncMarkerInput{1, o.ID, o.Interval.ID, o.Interval.ComputerID, o.Interval.Attribution, p.ID, p.SpentDate, p.Start, p.End, p.DurationNS, p.PlannedHours, p.PlannedDurationNS, p.PlannedResidualNS, c.Mode, c.DurationPolicy, c.PolicyVersion, c.Clock, p.StartedTime, p.EndedTime})
	sum := sha256.Sum256(raw)
	return "Tempo activity [tempo:v1:" + p.ID + ":" + hex.EncodeToString(sum[:]) + "]"
}
func syncPayload(o OutboxItem, part SyncPart) harvest.Object {
	a := o.Interval.Attribution
	payload := harvest.Object{"user_id": json.Number(a.UserID), "project_id": json.Number(a.ProjectID), "task_id": json.Number(a.TaskID), "spent_date": part.SpentDate, "notes": part.Notes, "external_reference": harvest.Object{"id": part.Correlation, "group_id": o.ID, "account_id": o.Interval.ComputerID}}
	if part.StartedTime != nil && part.EndedTime != nil {
		payload["started_time"] = *part.StartedTime
		payload["ended_time"] = *part.EndedTime
	} else {
		payload["hours"] = json.Number(part.PlannedHours)
	}
	return payload
}
func syncObject(value any) harvest.Object {
	switch o := value.(type) {
	case map[string]any:
		return o
	}
	return nil
}
func syncDecimal(value any) (string, *big.Rat) {
	n, ok := value.(json.Number)
	if !ok {
		return "", nil
	}
	s := string(n)
	if len(s) > 128 || !json.Valid([]byte(s)) || strings.ContainsAny(s, "/eE") {
		return "", nil
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok || r.Sign() < 0 {
		return "", nil
	}
	return s, r
}
func syncRoundNS(hours *big.Rat) *big.Int {
	r := new(big.Rat).Mul(hours, new(big.Rat).SetInt64(syncHourNS))
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(r.Num(), r.Denom(), rem)
	if new(big.Int).Lsh(rem, 1).Cmp(r.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	return q
}

// A response may acknowledge an entry without agreeing with the submitted amount.
// Preserve that remote ID and amount; never convert a mismatch into a retry.
func syncApplyResponse(o OutboxItem, part *SyncPart, row harvest.Object) {
	a := o.Interval.Attribution
	ref := syncObject(row["external_reference"])
	id := bindingID(row)
	if !identity.Valid(id) || bindingID(syncObject(row["user"])) != a.UserID || bindingID(syncObject(row["project"])) != a.ProjectID || bindingID(syncObject(row["task"])) != a.TaskID || row["is_running"] != false || row["spent_date"] != part.SpentDate || row["notes"] != part.Notes || ref["id"] != part.Correlation || ref["group_id"] != o.ID || ref["account_id"] != o.Interval.ComputerID {
		part.State = "unknown"
		part.FailureCategory = syncString("response_mismatch")
		return
	}
	if part.StartedTime != nil && (row["started_time"] != *part.StartedTime || part.EndedTime == nil || row["ended_time"] != *part.EndedTime) {
		part.State = "unknown"
		part.FailureCategory = syncString("response_mismatch")
		return
	}
	returned, rat := syncDecimal(row["hours"])
	if rat == nil || !syncRoundNS(rat).IsInt64() {
		part.State = "unknown"
		part.FailureCategory = syncString("response")
		return
	}
	part.EntryID = syncString(id)
	part.ReturnedHours = &returned
	if rounded, r := syncDecimal(row["rounded_hours"]); r != nil && syncRoundNS(r).IsInt64() {
		part.RoundedHours = &rounded
	}
	confirmed := syncRoundNS(rat)
	part.ConfirmedDurationNS = syncString(confirmed.String())
	planned, _ := new(big.Int).SetString(part.PlannedDurationNS, 10)
	exact, _ := new(big.Int).SetString(part.DurationNS, 10)
	part.ProviderDeltaNS = syncString(new(big.Int).Sub(planned, confirmed).String())
	part.TotalResidualNS = syncString(new(big.Int).Sub(exact, confirmed).String())
	intended, _ := new(big.Rat).SetString(part.PlannedHours)
	part.State = "synced"
	part.FailureCategory = nil
	if intended.Cmp(rat) != 0 {
		part.State = "needs_attention"
		part.FailureCategory = syncString("duration_mismatch")
	}
}
