package hookstate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// This is the independently inspected pinned Codex0.159.3 executable size.
// The fixture is an inert sparse zero file, not the downloaded runtime.
const qaPinnedRuntimeBytes int64 = 287086056
const qaPinnedZeroDigest = "683d8abfd5109fee9926a58c45812e29b15f3505aaaf7e8b4a6adefe89a150b4"

func qaPolicySparse(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(size); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
}
func qaPolicyRuntimeDigest(t *testing.T, p Profile) string {
	t.Helper()
	for _, a := range p.Context.Artifacts {
		if a.Role == "runtime" {
			return a.SHA256
		}
	}
	t.Fatal("missing runtime artifact")
	return ""
}
func TestQAPolicyPinnedRuntimeSizeSupportsDeclaredLifecycleAndTailDrift(t *testing.T) {
	c := qaPolicyContext(t)
	runtime := c.Artifacts[0].Path
	qaPolicySparse(t, runtime, qaPinnedRuntimeBytes)
	path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
	s := New(Options{Path: path})
	ctx := context.Background()
	started := time.Now()
	preview, err := s.Preview(ctx, c)
	previewElapsed := time.Since(started)
	if err != nil {
		t.Fatalf("pinned287086056byte runtime rejected by Preview: %v", err)
	}
	if qaPolicyRuntimeDigest(t, preview) != qaPinnedZeroDigest {
		t.Fatalf("Preview did not hash complete inert runtime: %s", qaPolicyRuntimeDigest(t, preview))
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Preview initialized metadata: %v", err)
	}
	started = time.Now()
	confirmed, err := s.Confirm(ctx, ConfirmInput{Context: preview.Context, Fingerprint: preview.Fingerprint, DeclarationVersion: DeclarationVersion, RequestID: "13000000-0000-4000-8000-000000000901", Confirmed: true})
	confirmElapsed := time.Since(started)
	if err != nil || !confirmed.CaptureEligible || confirmed.Basis != "operator_declared" || qaPolicyRuntimeDigest(t, confirmed) != qaPinnedZeroDigest {
		t.Fatalf("large-runtime Confirm failed: %+v %v", confirmed, err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	eligible, err := s.Eligibility(ctx, "codex", c.Path)
	eligibilityElapsed := time.Since(started)
	if err != nil || !eligible.CaptureEligible || eligible.Revision != confirmed.Revision || qaPolicyRuntimeDigest(t, eligible) != qaPinnedZeroDigest {
		t.Fatalf("large-runtime Eligibility failed: %+v %v", eligible, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatalf("healthy eligibility rewrote profile: %v", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = s.Eligibility(cancelled, "codex", c.Path)
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != "state_busy" {
		t.Fatalf("cancelled large-runtime eligibility: %v", err)
	}
	after, err = os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatalf("cancellation mutated retained profile: %v", err)
	}
	f, err := os.OpenFile(runtime, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{1}, qaPinnedRuntimeBytes-1)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("tail drift fixture: %v %v", err, closeErr)
	}
	drift, err := s.Eligibility(ctx, "codex", c.Path)
	if err != nil || drift.CaptureEligible || drift.State != "invalidated" {
		t.Fatalf("same-size tail change escaped full hashing: %+v %v", drift, err)
	}
	if qaPolicyRuntimeDigest(t, drift) != qaPinnedZeroDigest {
		t.Fatal("invalidation replaced original declaration evidence")
	}
	reopened := New(Options{Path: path})
	retained, err := reopened.Eligibility(ctx, "codex", c.Path)
	if err != nil || retained.CaptureEligible || retained.State != "invalidated" {
		t.Fatalf("invalidation not durable: %+v %v", retained, err)
	}
	t.Logf("inert runtime bytes=%d Preview=%s Confirm=%s Eligibility=%s (observed only; hosted real-runtime900ms acceptance is separate)", qaPinnedRuntimeBytes, previewElapsed, confirmElapsed, eligibilityElapsed)
}
func TestQAPolicyArtifactSizeBoundsRemainFinite(t *testing.T) {
	for _, mode := range []string{"runtime_over_320MiB", "executable_over_256MiB", "definitions_over_8MiB", "aggregate_over_512MiB", "cancelled_preview"} {
		t.Run(mode, func(t *testing.T) {
			c := qaPolicyContext(t)
			path := filepath.Join(t.TempDir(), "metadata", "hooks.json")
			s := New(Options{Path: path})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := "validation"
			switch mode {
			case "runtime_over_320MiB":
				qaPolicySparse(t, c.Artifacts[0].Path, (320<<20)+1)
			case "executable_over_256MiB":
				qaPolicySparse(t, c.Artifacts[1].Path, (256<<20)+1)
			case "definitions_over_8MiB":
				qaPolicySparse(t, c.Artifacts[2].Path, (8<<20)+1)
			case "aggregate_over_512MiB":
				qaPolicySparse(t, c.Artifacts[0].Path, 256<<20)
				qaPolicySparse(t, c.Artifacts[1].Path, 256<<20)
				qaPolicySparse(t, c.Artifacts[2].Path, 1)
			case "cancelled_preview":
				qaPolicySparse(t, c.Artifacts[0].Path, qaPinnedRuntimeBytes)
				cancel()
				want = "state_busy"
			}
			_, err := s.Preview(ctx, c)
			var pe *Error
			if !errors.As(err, &pe) || pe.Code != want {
				t.Fatalf("bound %s accepted or wrong failure: %v", mode, err)
			}
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected Preview wrote metadata: %v", err)
			}
		})
	}
}
