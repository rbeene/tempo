package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

func TestQASyncPersistedPlanCorruptionFailsClosedWithoutRewriting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		alter func(map[string]any)
	}{
		{"exact-duration", func(p map[string]any) { p["duration_ns"] = "1" }},
		{"planned-residual", func(p map[string]any) { p["planned_residual_ns"] = "0" }},
		{"start-after-end", func(p map[string]any) { p["start"] = "2027-01-01T00:00:00Z" }},
		{"marker-digest", func(p map[string]any) {
			p["notes"] = "Tempo activity [tempo:v1:00000000-0000-4000-8000-000000000000:" + strings.Repeat("0", 64) + "]"
		}},
		{"synced-with-unknown-attempt", func(p map[string]any) { p["attempts"].([]any)[0].(map[string]any)["state"] = "unknown" }},
		{"duplicate-attempt-id", func(p map[string]any) { a := p["attempts"].([]any); p["attempts"] = append(a, a[0]) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, path, _ := qaSyncFixture(t, 137482*time.Millisecond)
			p := qaNewSyncProvider(t)
			qaSyncConfigure(t, s, p)
			qaSyncEnable(t, s)
			_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(130)}, qaSyncDeps(t, p))
			if err != nil {
				t.Fatal(err)
			}
			if item := qaSyncOnlyItem(t, s); item.State != "synced" {
				t.Fatalf("fixture not synced: %+v", item)
			}
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			dec := json.NewDecoder(bytes.NewReader(b))
			dec.UseNumber()
			var raw map[string]any
			if err = dec.Decode(&raw); err != nil {
				t.Fatal(err)
			}
			for _, o := range raw["outbox"].(map[string]any) {
				part := o.(map[string]any)["plan"].(map[string]any)["parts"].([]any)[0].(map[string]any)
				tc.alter(part)
			}
			corrupt, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = New(Options{Path: path}).SyncStatus(context.Background())
			qaCode(t, err, "state_corrupt")
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, corrupt) || len(p.posts) != 1 {
				t.Fatal("corruption read rewrote evidence or posted")
			}
		})
	}
}
func TestQASyncReturnedHoursOverflowCannotBecomeConfirmedDuration(t *testing.T) {
	s, _, _ := qaSyncFixture(t, 137482*time.Millisecond)
	p := qaNewSyncProvider(t)
	p.returnedHours = strings.Repeat("9", 128)
	qaSyncConfigure(t, s, p)
	qaSyncEnable(t, s)
	_, err := s.SyncNow(context.Background(), SyncRunInput{RequestID: qaSyncID(131)}, qaSyncDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	item := qaSyncOnlyItem(t, s)
	if len(p.posts) != 1 || item.State != "unknown" || item.Plan.Parts[0].ConfirmedDurationNS != nil {
		t.Fatalf("unrepresentable response adopted: %+v", item)
	}
}
