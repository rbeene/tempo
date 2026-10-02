package activity

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// A preserved historical continuous interval fixture: start from a genuine
// linked/captured record, replace its entire supporting coordinate evidence in
// one private transaction, and demand validState before testing sync. This
// avoids hundreds of thousands of heartbeat disk writes for partition limits.
func qaSyncHistorical(t *testing.T, start, end time.Time, zone string) *Service {
	t.Helper()
	s, _, _ := qaSyncFixture(t, time.Minute)
	sample := func(at time.Time) ClockSample {
		epoch, n := "qa-history", strconv.FormatInt(int64(at.Sub(start)), 10)
		return ClockSample{Capability: "available", WallUTC: at.UTC(), Epoch: &epoch, ElapsedNS: &n, AwakeNS: &n}
	}
	first, last := sample(start), sample(end)
	err := s.store.update(context.Background(), func(st *state) (bool, error) {
		for id, b := range st.Bindings {
			b.Attribution.Timezone = zone
			st.Bindings[id] = b
		}
		for id, b := range st.BindingRecords {
			b.Snapshot.Attribution.Timezone = zone
			st.BindingRecords[id] = b
		}
		for _, a := range st.Actors {
			a.Attribution.Timezone = zone
			a.LastEvidence = last
		}
		for _, ep := range st.Epochs {
			ep.Attribution.Timezone = zone
			ep.Anchor = first
		}
		for _, seg := range st.Segments {
			seg.Binding.Attribution.Timezone = zone
			seg.StartSample = first
			seg.ConfirmedSample = last
			seg.Start = start.UTC()
			seg.Confirmed = end.UTC()
			e := end.UTC()
			seg.End = &e
		}
		for i, in := range st.Intervals {
			in.Start = start.UTC()
			in.End = end.UTC()
			in.DurationNS = strconv.FormatInt(int64(end.Sub(start)), 10)
			in.Attribution.Timezone = zone
			st.Intervals[i] = in
			o := st.Outbox[in.ID]
			o.Interval = in
			st.Outbox[in.ID] = o
		}
		if !validState(st) {
			t.Fatal("invalid historical interval fixture")
		}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	s.clock = ClockFunc(func() (ClockSample, error) { return last, nil })
	if _, _, err = s.store.read(context.Background()); err != nil {
		t.Fatalf("historical fixture failed persistence validation: %v", err)
	}
	return s
}
func qaSyncTime(t *testing.T, s string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatal(err)
	}
	return at
}
func TestQASyncCalendarUsesLocalDatesAcrossDSTAndSkippedDate(t *testing.T) {
	for _, tc := range []struct {
		name, zone, start, end string
		dates                  []string
		hours                  []string
	}{
		{"new-york-spring", "America/New_York", "2026-03-08T00:00:00-05:00", "2026-03-10T00:00:00-04:00", []string{"2026-03-08", "2026-03-09"}, []string{"23", "24"}},
		{"new-york-fall", "America/New_York", "2026-11-01T00:00:00-04:00", "2026-11-03T00:00:00-05:00", []string{"2026-11-01", "2026-11-02"}, []string{"25", "24"}},
		{"lord-howe-half-hour", "Australia/Lord_Howe", "2026-10-04T00:00:00+10:30", "2026-10-06T00:00:00+11:00", []string{"2026-10-04", "2026-10-05"}, []string{"23.5", "24"}},
		{"apia-skipped-date", "Pacific/Apia", "2011-12-29T00:00:00-10:00", "2012-01-01T00:00:00+14:00", []string{"2011-12-29", "2011-12-31"}, []string{"24", "24"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, end := qaSyncTime(t, tc.start), qaSyncTime(t, tc.end)
			s := qaSyncHistorical(t, start, end, tc.zone)
			p := qaNewSyncProvider(t)
			p.timezone = tc.zone
			p.returnedHours = ""
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(110)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if item.State != "synced" || item.Plan == nil || len(item.Plan.Parts) != len(tc.dates) || len(p.posts) != len(tc.dates) {
				t.Fatalf("calendar split=%+v posts%d", item, len(p.posts))
			}
			var total int64
			for i, part := range item.Plan.Parts {
				if part.SpentDate != tc.dates[i] || part.PlannedHours != tc.hours[i] || !part.End.After(part.Start) {
					t.Fatalf("part%d=%+v", i, part)
				}
				n, err := strconv.ParseInt(part.DurationNS, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				total += n
			}
			if total != int64(end.Sub(start)) {
				t.Fatalf("split lost exact duration%d", total)
			}
		})
	}
}
func TestQASyncQuantizesEachDateAndBlocksWholeRootForZeroPart(t *testing.T) {
	for _, tc := range []struct {
		name, start, end string
		posts            int
		residual         string
	}{
		{"twenty-seconds-each", "2026-10-02T23:59:40Z", "2026-10-03T00:00:20Z", 2, "-16000000000"},
		{"one-zero-part", "2026-10-02T23:59:50Z", "2026-10-03T00:00:30Z", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := qaSyncHistorical(t, qaSyncTime(t, tc.start), qaSyncTime(t, tc.end), "UTC")
			p := qaNewSyncProvider(t)
			p.returnedHours = ""
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(111)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if len(p.posts) != tc.posts {
				t.Fatalf("whole-root zero preflight posts%d item%+v", len(p.posts), item)
			}
			if tc.posts == 0 {
				if item.State != "needs_attention" {
					t.Fatal(item.State)
				}
				return
			}
			if len(item.Plan.Parts) != 2 {
				t.Fatal("did not split before quantization")
			}
			for _, part := range item.Plan.Parts {
				if part.PlannedHours != "0.01" || part.PlannedResidualNS != tc.residual {
					t.Fatalf("rounding occurred before split: %+v", part)
				}
			}
		})
	}
}
func TestQASyncPartitionLimitPreflightsBeforeAnyPost(t *testing.T) {
	start := qaSyncTime(t, "2026-01-01T00:00:00Z")
	s := qaSyncHistorical(t, start, start.AddDate(0, 0, 101), "UTC")
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(112)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 0 || item.State != "needs_attention" {
		t.Fatalf("oversized root partly submitted: %+v posts%d", item, len(p.posts))
	}
}

func TestQASyncTimestampPreflightRejectsAmbiguousOrInexactWholeRoot(t *testing.T) {
	for _, tc := range []struct{ name, zone, start, end string }{
		{"fractional-minute", "UTC", "2026-10-02T09:00:00Z", "2026-10-02T09:02:17.482Z"},
		{"dst-transition", "America/New_York", "2026-03-08T01:59:00-05:00", "2026-03-08T03:01:00-04:00"},
		{"fold-same-offset-endpoints", "America/New_York", "2026-11-01T01:10:00-04:00", "2026-11-01T01:20:00-04:00"},
		{"midnight-ending", "UTC", "2026-10-02T23:59:00Z", "2026-10-03T00:00:00Z"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := qaSyncHistorical(t, qaSyncTime(t, tc.start), qaSyncTime(t, tc.end), tc.zone)
			p := qaNewSyncProvider(t)
			p.timezone = tc.zone
			p.company["wants_timestamp_timers"] = true
			_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "timestamp", DurationPolicy: "exact", Clock: "24h", IfRevision: "0", RequestID: qaSyncID(1), Confirmed: true}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			qaSyncEnable(t, s)
			_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(113)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			item := qaSyncOnlyItem(t, s)
			if len(p.posts) != 0 || item.State != "needs_attention" {
				t.Fatalf("unsafe timestamp submitted: %+v posts%d", item, len(p.posts))
			}
		})
	}
}
func TestQASyncTimestampUsesDeclaredClockAndStoppedEndpoints(t *testing.T) {
	s := qaSyncHistorical(t, qaSyncTime(t, "2026-10-02T09:00:00Z"), qaSyncTime(t, "2026-10-02T09:02:00Z"), "UTC")
	p := qaNewSyncProvider(t)
	p.company["wants_timestamp_timers"] = true
	_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "timestamp", DurationPolicy: "exact", Clock: "24h", IfRevision: "0", RequestID: qaSyncID(1), Confirmed: true}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	qaSyncEnable(t, s)
	p.beforeCreate = func(body map[string]any) {
		if body["started_time"] != "09:00" || body["ended_time"] != "09:02" {
			t.Fatalf("timestamp request endpoints=%v", body)
		}
		if _, ok := body["hours"]; ok {
			t.Fatal("timestamp request mixed duration and clock payload")
		}
		st, _, err := s.store.read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range st.Outbox {
			p.returnedHours = o.Plan.Parts[0].PlannedHours
		}
	}
	_, err = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(114)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || item.State != "synced" {
		t.Fatalf("valid timestamps not synced: %+v posts%d", item, len(p.posts))
	}
}
