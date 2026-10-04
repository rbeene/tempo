package hookstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEligibilityDiagnosticsEligibleReadOnlyRoleBytes(t *testing.T) {
	c := qaPolicyContext(t)
	large := filepath.Join(c.Path, "configuration")
	content := bytes.Repeat([]byte("x"), 262145)
	if err := os.WriteFile(large, content, 0600); err != nil {
		t.Fatal(err)
	}
	c.Artifacts = append(c.Artifacts,
		Artifact{Role: "skill", Path: filepath.Join(c.Path, "absent-skill")},
		Artifact{Role: "configuration", Path: large},
		Artifact{Role: "configuration", Path: filepath.Join(c.Path, "absent-configuration")},
		Artifact{Role: "repository", Path: c.Path})
	s := New(Options{Path: filepath.Join(t.TempDir(), "metadata", "hooks.json")})
	qaPolicyConfirm(t, s, c, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	before, err := os.ReadFile(s.options.Path)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := s.Eligibility(context.Background(), "codex", c.Path)
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithTimeout(context.Background(), 900*time.Millisecond)
	defer cancel()
	ctx, observer := WithEligibilityDiagnostics(parent)
	gotDeadline, gotOK := ctx.Deadline()
	wantDeadline, wantOK := parent.Deadline()
	if gotOK != wantOK || !gotDeadline.Equal(wantDeadline) {
		t.Error("observer changed caller deadline")
	}
	observed, err := s.Eligibility(ctx, "codex", c.Path)
	if err != nil || !reflect.DeepEqual(plain, observed) {
		t.Error("observation changed eligible profile", err)
	}
	after, readErr := os.ReadFile(s.options.Path)
	if readErr != nil || !bytes.Equal(before, after) {
		t.Error("observation wrote policy state")
	}
	value := observer.Snapshot()
	if value.D {
		t.Error("bounded observation dropped data")
	}
	for _, phase := range []int{0, 1, 2, 4, 6} {
		if value.P[phase] < 0 || value.P[phase] > value.P[6] {
			t.Error("reached policy phase missing", phase)
		}
	}
	if value.P[3] != -1 || value.P[5] != -1 {
		t.Error("unreached policy phase fabricated")
	}
	for role, name := range []string{"runtime", "executable", "definitions"} {
		r := value.R[role]
		if r[0] != 1 || r[1] != 1 || r[2] != int64(len("SYNTHETIC_SECRET_CONTENT_"+name)) || r[3] < 1 {
			t.Error("actual small artifact accounting", role)
		}
	}
	if value.R[3][0] != 1 || value.R[3][1] != 0 || value.R[3][2] != 0 {
		t.Error("absent skill was hashed")
	}
	if r := value.R[4]; r[0] != 2 || r[1] != 1 || r[2] != int64(len(content)) || r[3] < 3 {
		t.Error("real multi-read configuration accounting")
	}
	if r := value.R[5]; r[0] != 1 || r[1] != 0 || r[2] != 0 {
		t.Error("repository identity was treated as file content")
	}
	if (value.C[0] == -1) != (value.C[1] == -1) || value.C[0] < -1 || value.C[1] < -1 {
		t.Error("invalid CPU availability")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{c.Path, s.options.Path, "SYNTHETIC_SECRET_CONTENT"} {
		if strings.Contains(string(encoded), secret) {
			t.Error("observation leaked nonnumeric data")
		}
	}
	value.R[0][2] = -99
	if observer.Snapshot().R[0][2] == -99 {
		t.Error("snapshot aliases observer")
	}
}

func TestEligibilityDiagnosticsCanceledAndRepeatedObservation(t *testing.T) {
	s := New(Options{Path: filepath.Join(t.TempDir(), "absent", "hooks.json")})
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	ctx, observer := WithEligibilityDiagnostics(parent)
	_, err := s.Eligibility(ctx, "codex", t.TempDir())
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != "state_busy" || ctx.Err() != context.Canceled {
		t.Error("caller cancellation changed")
	}
	value := observer.Snapshot()
	if value.D || value.P[6] < 0 || value.R != [6][9]int64{} {
		t.Error("canceled observation fabricated artifact work")
	}
	for i := 0; i < 6; i++ {
		if value.P[i] != -1 {
			t.Error("canceled observation fabricated reached phase")
		}
	}
	if _, err := os.Stat(filepath.Dir(s.options.Path)); !errors.Is(err, os.ErrNotExist) {
		t.Error("canceled policy observation created state")
	}
	_, _ = s.Eligibility(ctx, "codex", t.TempDir())
	if !observer.Snapshot().D {
		t.Error("repeated use silently overwrote a bounded attempt")
	}
}

func TestEligibilityDiagnosticsCPUProjectionAndDisabledContext(t *testing.T) {
	if beginEligibilityDiagnostic(context.Background()) != nil {
		t.Error("disabled context enabled policy observation")
	}
	cases := []struct {
		before, after [2]int64
		valid         bool
		want          [2]int64
	}{
		{[2]int64{10, 20}, [2]int64{15, 27}, true, [2]int64{5, 7}},
		{[2]int64{10, 20}, [2]int64{15, 27}, false, [2]int64{-1, -1}},
		{[2]int64{10, 20}, [2]int64{9, 27}, true, [2]int64{-1, -1}},
		{[2]int64{-1, 20}, [2]int64{15, 27}, true, [2]int64{-1, -1}},
		{[2]int64{}, [2]int64{120000001, 0}, true, [2]int64{-1, -1}},
	}
	for _, item := range cases {
		if got := eligibilityCPUDelta(item.before, item.after, item.valid); got != item.want {
			t.Error("CPU projection changed availability/delta")
		}
	}
}
