package activity

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestQASyncConfigureRejectsInvalidConsentBeforeProvider(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		alter      func(*SyncConfigureInput)
	}{
		{"missing-account", "input_required", func(i *SyncConfigureInput) { i.AccountID = "" }},
		{"missing-mode", "input_required", func(i *SyncConfigureInput) { i.Mode = "" }},
		{"missing-policy", "input_required", func(i *SyncConfigureInput) { i.DurationPolicy = "" }},
		{"missing-cas", "input_required", func(i *SyncConfigureInput) { i.IfRevision = "" }},
		{"unconfirmed", "confirmation_required", func(i *SyncConfigureInput) { i.Confirmed = false }},
		{"bad-mode", "validation", func(i *SyncConfigureInput) { i.Mode = "guessed" }},
		{"bad-policy", "validation", func(i *SyncConfigureInput) { i.DurationPolicy = "round" }},
		{"duration-clock", "validation", func(i *SyncConfigureInput) { i.Clock = "24h" }},
		{"timestamp-nearest", "validation", func(i *SyncConfigureInput) { i.Mode = "timestamp"; i.Clock = "24h" }},
		{"timestamp-no-clock", "input_required", func(i *SyncConfigureInput) { i.Mode = "timestamp"; i.DurationPolicy = "exact" }},
		{"invalid-revision", "validation", func(i *SyncConfigureInput) { i.IfRevision = "01" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _ := qaSyncFixture(t, time.Minute)
			before, _, err := s.store.read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			in := SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: "0", RequestID: qaSyncID(70), Confirmed: true}
			tc.alter(&in)
			_, err = s.SyncConfigure(context.Background(), in, qaSyncNoProvider(t))
			qaCode(t, err, tc.code)
			after, _, err := s.store.read(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("invalid consent mutated state")
			}
		})
	}
}
func TestQASyncConfigureRechecksCASAfterRemoteReads(t *testing.T) {
	s, _, _ := qaSyncFixture(t, time.Minute)
	outer := qaNewSyncProvider(t)
	inner := qaNewSyncProvider(t)
	raced := 0
	outer.beforeUser = func() {
		raced++
		_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "exact", IfRevision: "0", RequestID: qaSyncID(72), Confirmed: true}, qaSyncDeps(t, inner))
		if err != nil {
			t.Fatalf("concurrent configuration blocked by remote read: %v", err)
		}
	}
	_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "nearest-hundredth-hour", IfRevision: "0", RequestID: qaSyncID(71), Confirmed: true}, qaSyncDeps(t, outer))
	qaCode(t, err, "revision_conflict")
	st, err := s.SyncStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if raced != 1 || len(st.Configurations) != 1 || st.Configurations[0].DurationPolicy != "exact" || st.Configurations[0].Revision != "1" {
		t.Fatalf("stale preflight overwrote winner: %+v reads%d", st.Configurations, raced)
	}
}

func TestQASyncConfigurationChangeDuringPreflightPreventsFirstClaim(t *testing.T) {
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	changed := 0
	p.beforeUser = func() {
		if changed != 0 {
			return
		}
		changed++
		_, err := s.SyncConfigure(context.Background(), SyncConfigureInput{AccountID: "1", Mode: "duration", DurationPolicy: "exact", IfRevision: "1", RequestID: qaSyncID(74), Confirmed: true}, qaSyncDeps(t, qaNewSyncProvider(t)))
		if err != nil {
			t.Fatalf("configuration race setup: %v", err)
		}
	}
	_, _ = s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(73)}, qaSyncDeps(t, p))
	item := qaSyncOnlyItem(t, s)
	if changed != 1 || len(p.posts) != 0 || item.State == "synced" || item.State == "submitting" {
		t.Fatalf("changed consent used by stale preflight: callbacks%d posts%d item%+v", changed, len(p.posts), item)
	}
	if item.Plan != nil {
		for _, part := range item.Plan.Parts {
			if len(part.Attempts) != 0 {
				t.Fatal("configuration conflict claimed a part")
			}
		}
	}
}
