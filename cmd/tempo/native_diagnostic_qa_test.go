//go:build tempo_native_diagnostic && (darwin || linux) && (amd64 || arm64)

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/cli"
	"golang.org/x/sys/unix"
)

type nativeDiagnosticShortWriter struct{ err error }

func (w nativeDiagnosticShortWriter) Write([]byte) (int, error) { return 1, w.err }

func TestNativeDiagnosticActualCLIHookFailureRetainsHostOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	w := &nativeDiagnosticWriter{forward: &stderr}
	// Invalid finite JSON refuses before service construction, credentials or
	// state admission. It exercises the real CLI writer seam and safe boundary.
	code := cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, strings.NewReader("{"), &stdout, w, cli.Dependencies{})
	r := w.record(code)
	if code != 0 || stdout.String() != "{}\n" || stderr.String() != "tempo hook: validation; durability=not_committed\n" || r.Status != "safe_error" || r.Code != "validation" || r.Durability != "not_committed" {
		t.Fatal("real hook semantics changed")
	}
}

func TestNativeDiagnosticWriterExactForwardingAndSafeGrammar(t *testing.T) {
	for _, tc := range []struct{ input, status, code, durability string }{
		{"", "empty", "", ""},
		{"tempo hook: state_busy; durability=not_committed\n", "safe_error", "state_busy", "not_committed"},
		{"tempo hook: local_write_unknown; durability=unknown\n", "safe_error", "local_write_unknown", "unknown"},
		{"tempo hook: review_required; durability=committed\n", "safe_error", "review_required", "committed"},
		{"tempo hook: PRIVATE; durability=not_committed\n", "unavailable", "", ""},
		{"tempo hook: state_busy; durability=PRIVATE\n", "unavailable", "", ""},
		{"tempo hook: state_busy; durability=not_committed", "unavailable", "", ""},
		{"tempo hook: state_busy; durability=not_committed\nPRIVATE\n", "unavailable", "", ""},
		{strings.Repeat("PRIVATE", 100), "unavailable", "", ""},
	} {
		var forwarded bytes.Buffer
		w := &nativeDiagnosticWriter{forward: &forwarded}
		for i := 0; i < len(tc.input); i += 7 {
			end := i + 7
			if end > len(tc.input) {
				end = len(tc.input)
			}
			chunk := tc.input[i:end]
			if n, err := io.WriteString(w, chunk); n != len(chunk) || err != nil {
				t.Fatal("forwarding changed")
			}
		}
		r := w.record(0)
		if forwarded.String() != tc.input || r.Status != tc.status || r.Code != tc.code || r.Durability != tc.durability || !r.DiagnosticOnly || r.AcceptanceEligible {
			t.Fatal("safe projection changed")
		}
		if len(w.bytes) > nativeDiagnosticLimit {
			t.Fatal("capture memory bound exceeded")
		}
	}
	want := errors.New("synthetic writer failure")
	w := &nativeDiagnosticWriter{forward: nativeDiagnosticShortWriter{want}}
	if n, err := w.Write([]byte("arbitrary")); n != 1 || err != want {
		t.Fatal("original writer outcome changed")
	}
}

func nativeDiagnosticFixture(t *testing.T) (string, string, string) {
	t.Helper()
	p, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("fixture path")
	}
	if os.Chmod(p, 0700) != nil {
		t.Fatal("fixture mode")
	}
	var s unix.Stat_t
	if unix.Lstat(p, &s) != nil {
		t.Fatal("fixture identity")
	}
	return p, strconv.FormatUint(uint64(s.Dev), 10), strconv.FormatUint(uint64(s.Ino), 10)
}

func TestNativeDiagnosticSinkOwnIdentityExclusiveFilesAndFiniteCapacity(t *testing.T) {
	p, dev, ino := nativeDiagnosticFixture(t)
	w := &nativeDiagnosticWriter{forward: io.Discard}
	_, _ = io.WriteString(w, "tempo hook: state_busy; durability=not_committed\n")
	r := w.record(0)
	for i := 0; i < nativeDiagnosticSlots; i++ {
		if err := nativeDiagnosticRecord(p, dev, ino, r); err != nil {
			t.Fatal("owned record refused")
		}
	}
	entries, err := os.ReadDir(p)
	if err != nil || len(entries) != nativeDiagnosticSlots {
		t.Fatal("record capacity differs")
	}
	before := map[string][]byte{}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > nativeDiagnosticLimit {
			t.Fatal("unsafe record")
		}
		data, err := os.ReadFile(filepath.Join(p, e.Name()))
		if err != nil || bytes.Contains(data, []byte("tempo hook:")) {
			t.Fatal("raw stderr retained")
		}
		before[e.Name()] = data
	}
	if nativeDiagnosticRecord(p, dev, ino, r) == nil {
		t.Fatal("full namespace accepted another record")
	}
	for name, data := range before {
		after, err := os.ReadFile(filepath.Join(p, name))
		if err != nil || !bytes.Equal(after, data) {
			t.Fatal("existing record overwritten")
		}
	}
}

func TestNativeDiagnosticSinkRejectsWrongIdentityModeAndSymlink(t *testing.T) {
	p, dev, ino := nativeDiagnosticFixture(t)
	r := (&nativeDiagnosticWriter{forward: io.Discard}).record(0)
	if nativeDiagnosticRecord(p, dev, "0", r) == nil {
		t.Fatal("wrong identity accepted")
	}
	if os.Chmod(p, 0755) != nil {
		t.Fatal("fixture mode")
	}
	if nativeDiagnosticRecord(p, dev, ino, r) == nil {
		t.Fatal("public sink accepted")
	}
	if os.Chmod(p, 0700) != nil {
		t.Fatal("fixture mode")
	}
	link := filepath.Join(t.TempDir(), "alias")
	if os.Symlink(p, link) != nil {
		t.Fatal("fixture alias")
	}
	if nativeDiagnosticRecord(link, dev, ino, r) == nil {
		t.Fatal("symlink sink accepted")
	}
	entries, err := os.ReadDir(p)
	if err != nil || len(entries) != 0 {
		t.Fatal("refused sink was changed")
	}
}
