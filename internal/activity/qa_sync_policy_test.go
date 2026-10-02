package activity

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/harvest"
)

func TestQASyncCompanyFallbackIsOnlyExplicit403(t *testing.T) {
	for _, tc := range []struct {
		name      string
		company   harvest.Object
		err       error
		wantPosts int
	}{
		{"verified", harvest.Object{"is_active": true, "wants_timestamp_timers": false, "clock": "24h"}, nil, 1},
		{"forbidden", nil, &harvest.Error{Code: "forbidden", Status: 403}, 1},
		{"unauthorized", nil, &harvest.Error{Code: "unauthorized", Status: 401}, 0},
		{"network", nil, &harvest.Error{Code: "network"}, 0},
		{"raw-network", nil, errors.New("private provider failure"), 0},
		{"inactive", harvest.Object{"is_active": false, "wants_timestamp_timers": false, "clock": "24h"}, nil, 0},
		{"missing-mode", harvest.Object{"is_active": true}, nil, 0},
		{"wrong-type-mode", harvest.Object{"is_active": true, "wants_timestamp_timers": "false", "clock": "24h"}, nil, 0},
		{"conflicting-mode", harvest.Object{"is_active": true, "wants_timestamp_timers": true, "clock": "24h"}, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
			p := qaNewSyncProvider(t)
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			p.company, p.companyErr = tc.company, tc.err
			_, _ = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(60)}, qaSyncDeps(t, p))
			item := qaSyncOnlyItem(t, s)
			if !slices.Contains(p.calls, "/company") {
				t.Fatalf("company preflight not reached: %v", p.calls)
			}
			if len(p.posts) != tc.wantPosts {
				t.Fatalf("company fallback posts=%d want=%d item=%+v", len(p.posts), tc.wantPosts, item)
			}
			if tc.wantPosts == 1 && item.State != "synced" {
				t.Fatalf("valid configured mode not synced: %+v", item)
			}
			if tc.wantPosts == 0 && item.State == "synced" {
				t.Fatalf("failed preflight labeled success: %+v", item)
			}
		})
	}
}
func TestQASyncNearestHundredthBoundaryAndZeroBlock(t *testing.T) {
	for _, tc := range []struct {
		name            string
		duration        time.Duration
		hours, residual string
		posts           int
	}{
		{"just-below-half", 17999 * time.Millisecond, "", "", 0},
		{"half-up", 18 * time.Second, "0.01", "-18000000000", 1},
		{"exact-unit", 36 * time.Second, "0.01", "0", 1},
		{"second-half", 54 * time.Second, "0.02", "-18000000000", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, original := qaSyncFixture(t, tc.duration)
			p := qaNewSyncProvider(t)
			p.returnedHours = tc.hours
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(61)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if len(p.posts) != tc.posts || item.Interval.DurationNS != original.DurationNS {
				t.Fatalf("quantization posts=%d item=%+v", len(p.posts), item)
			}
			if tc.posts == 0 {
				if item.State != "needs_attention" {
					t.Fatalf("zero quantization state=%s", item.State)
				}
				return
			}
			part := item.Plan.Parts[0]
			if item.State != "synced" || part.PlannedHours != tc.hours || part.PlannedResidualNS != tc.residual {
				t.Fatalf("half-up arithmetic=%+v", part)
			}
		})
	}
}
func TestQASyncStrictRationalEqualityPrecedesNanosecondRounding(t *testing.T) {
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	// Difference is 0.000036 ns: rounding to integer nanoseconds hides it.
	p.returnedHours = "0.04000000000000001"
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(62)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || item.State != "needs_attention" || item.EntryID == nil || *item.EntryID != "901" {
		t.Fatalf("rational mismatch falsely synced: %+v", item)
	}
	qaSyncString(t, item.Plan.Parts[0].ProviderDeltaNS, "0")
	_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(63)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	if len(p.posts) != 1 {
		t.Fatal("known mismatched entry rebilled")
	}
}
func TestQASyncExactPolicyDoesNotAdoptRoundedReturnedHours(t *testing.T) {
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(1), Confirmed: true}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	qaSyncEnable(t, s)
	_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(64)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || item.State != "needs_attention" || item.Plan == nil {
		t.Fatalf("exact policy silently became nearest: %+v", item)
	}
	part := item.Plan.Parts[0]
	if part.PlannedDurationNS != "137482000000" || part.PlannedResidualNS != "0" || part.PlannedHours == "0.04" {
		t.Fatalf("exact representation=%+v", part)
	}
	qaSyncString(t, part.ProviderDeltaNS, "-6518000000")
}
func TestQASyncInjectedWriteStatusAllowlist(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{{400, "rejected"}, {401, "rejected"}, {403, "rejected"}, {404, "rejected"}, {422, "rejected"}, {429, "rejected"}, {408, "unknown"}, {302, "unknown"}, {500, "unknown"}} {
		t.Run("status-"+strconv.Itoa(tc.status), func(t *testing.T) {
			s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
			p := qaNewSyncProvider(t)
			p.createErr = &harvest.Error{Code: "api_error", Status: tc.status, Uncertain: false}
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(65)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if len(p.posts) != 1 || item.Plan == nil || item.Plan.Parts[0].State != tc.want {
				t.Fatalf("status%d classification=%+v posts%d", tc.status, item, len(p.posts))
			}
			_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(66)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			if len(p.posts) != 1 {
				t.Fatalf("status%d auto retry", tc.status)
			}
		})
	}
}

func TestQASyncIncompleteMultipartTotalsNeverInventZeroReturnedTime(t *testing.T) {
	s := qaSyncHistorical(t, qaSyncTime(t, "2026-10-02T23:59:40Z"), qaSyncTime(t, "2026-10-03T00:00:20Z"), "UTC")
	p := qaNewSyncProvider(t)
	p.returnedHours = ""
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	s.store.fail = func(stage string) error {
		if stage == "sync_after_part_saved" {
			_, err := s.SyncPause(context.Background(), qaSyncID(171))
			return err
		}
		return nil
	}
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(170)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	s.store.fail = nil
	status, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(p.posts) != 1 || status.Totals.ExactDurationNS != "40000000000" || status.Totals.ConfirmedDurationNS != nil || status.Totals.TotalResidualNS != nil {
		t.Fatalf("incomplete total invented returned duration: %+v", status.Totals)
	}
	qaSyncString(t, status.Totals.PlannedDurationNS, "72000000000")
	qaSyncString(t, status.Totals.PlannedResidualNS, "-32000000000")
	seen := map[string]bool{}
	for _, g := range status.Accounting {
		if g.Scope == "project" {
			seen["project"] = true
			if !reflect.DeepEqual(g.Totals, status.Totals) {
				t.Fatalf("project lost conservation: %+v", g)
			}
			continue
		}
		if g.Scope != "day" || g.Date == nil {
			t.Fatalf("invalid accounting scope=%+v", g)
		}
		seen[*g.Date] = true
		if g.Totals.ExactDurationNS != "20000000000" {
			t.Fatalf("day exact=%+v", g)
		}
		qaSyncString(t, g.Totals.PlannedDurationNS, "36000000000")
		qaSyncString(t, g.Totals.PlannedResidualNS, "-16000000000")
		if *g.Date == "2026-10-02" {
			qaSyncString(t, g.Totals.ConfirmedDurationNS, "36000000000")
			qaSyncString(t, g.Totals.TotalResidualNS, "-16000000000")
		} else if g.Totals.ConfirmedDurationNS != nil || g.Totals.TotalResidualNS != nil {
			t.Fatalf("unposted day represented as zero: %+v", g)
		}
	}
	if len(seen) != 3 || !seen["project"] || !seen["2026-10-02"] || !seen["2026-10-03"] {
		t.Fatalf("missing accounting scopes%v", seen)
	}
}

func TestQASyncOtherEndpoint403NeverUsesCompanyFallback(t *testing.T) {
	for _, endpoint := range []string{"user", "assignments"} {
		t.Run(endpoint, func(t *testing.T) {
			s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
			p := qaNewSyncProvider(t)
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			p.calls = nil
			p.companyErr = &harvest.Error{Code: "forbidden", Status: 403}
			if endpoint == "user" {
				p.userErr = p.companyErr
			} else {
				p.assignmentErr = p.companyErr
			}
			_, _ = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(210)}, qaSyncDeps(t, p))
			item := qaSyncOnlyItem(t, s)
			wantPath := "/users/me"
			if endpoint == "assignments" {
				wantPath = "/users/me/project_assignments"
			}
			if !slices.Contains(p.calls, wantPath) || slices.Contains(p.calls, "/company") || len(p.posts) != 0 || item.State == "synced" {
				t.Fatalf("nonCompany403 bypassed auth: calls%v posts%d item%+v", p.calls, len(p.posts), item)
			}
		})
	}
}
func TestQASyncMalformedReturnedDecimalIsUnknown(t *testing.T) {
	for _, value := range []string{"NaN", "-0.04", "1/25", "0.04garbage", "0.04e0"} {
		t.Run(value, func(t *testing.T) {
			s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
			p := qaNewSyncProvider(t)
			p.returnedHours = value
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(211)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if len(p.posts) != 1 || item.State != "unknown" || item.Plan.Parts[0].ConfirmedDurationNS != nil {
				t.Fatalf("malformed decimal adopted %+v", item)
			}
		})
	}
}

func TestQASyncDayAccountingIncludesCapturedNeverPlannedPortion(t *testing.T) {
	s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(240)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	first := qaSyncOnlyItem(t, s)
	if first.State != "synced" {
		t.Fatal("fixture not synced")
	}
	qaSyncAppendCapturedInterval(t, s)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	status, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || len(p.posts) != 1 {
		t.Fatal("offline accounting changed evidence or effects")
	}
	if status.Totals.ExactDurationNS != "173482000000" || status.Totals.PlannedDurationNS != nil || status.Totals.ConfirmedDurationNS != nil {
		t.Fatalf("global incomplete totals=%+v", status.Totals)
	}
	seenDay := false
	for _, group := range status.Accounting {
		if group.Scope == "project" && !reflect.DeepEqual(group.Totals, status.Totals) {
			t.Fatalf("project does not conserve global total: %+v", group)
		}
		if group.Scope == "day" && group.Date != nil && *group.Date == "2026-10-02" {
			seenDay = true
			if group.Totals.ExactDurationNS != "173482000000" || group.Totals.PlannedDurationNS != nil || group.Totals.ConfirmedDurationNS != nil || group.Totals.PlannedResidualNS != nil || group.Totals.TotalResidualNS != nil {
				t.Fatalf("day omitted neverplanned capture or falsely complete: %+v", group)
			}
		}
	}
	if !seenDay {
		t.Fatal("day accounting absent")
	}
	for _, item := range status.Items {
		if item.ID != first.ID && item.Plan != nil {
			t.Fatal("offline status manufactured plan")
		}
		if item.ID == first.ID && !reflect.DeepEqual(item.Plan, first.Plan) {
			t.Fatal("incomplete aggregate erased known part accounting")
		}
	}
}
