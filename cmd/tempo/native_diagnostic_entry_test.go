//go:build tempo_native_diagnostic && (darwin || linux) && (amd64 || arm64)

package main

// This entry exists only in an explicitly tagged test executable. It is never
// compiled by go build and cannot establish shipping-binary acceptance.
import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/terminal"
	"golang.org/x/sys/unix"
)

const nativeDiagnosticOptIn = "TEMPO_NATIVE_DIAGNOSTIC"
const nativeDiagnosticDir = "TEMPO_NATIVE_DIAGNOSTIC_DIR"
const nativeDiagnosticDev = "TEMPO_NATIVE_DIAGNOSTIC_DEV"
const nativeDiagnosticIno = "TEMPO_NATIVE_DIAGNOSTIC_INO"
const nativeDiagnosticLimit = 256
const nativeDiagnosticSlots = 128

func TestMain(m *testing.M) {
	// TestMain runs before m.Run parses flags. Exact opt-in is required so normal
	// tagged unit tests retain the ordinary testing entry and flag handling.
	if os.Getenv(nativeDiagnosticOptIn) != "1" {
		// Production auth helpers inherit only PATH. Preserve that exact private
		// mode without opt-in or recording; HelperMain validates its own pipes.
		if len(os.Args) == 2 && os.Args[1] == "--tempo-auth-helper" {
			os.Exit(nativeDiagnosticRun(os.Stderr))
		}
		os.Exit(m.Run())
	}
	writer := &nativeDiagnosticWriter{forward: os.Stderr}
	code := nativeDiagnosticRun(writer)
	// Sink failure cannot change the real CLI result or invent an error code.
	// Missing records never prove non-delivery or complete observation.
	_ = nativeDiagnosticRecord(os.Getenv(nativeDiagnosticDir), os.Getenv(nativeDiagnosticDev), os.Getenv(nativeDiagnosticIno), writer.record(code))
	os.Exit(code)
}

// Exact shipping run body; only the CLI errOut argument is substituted. All
// deferred signal and context cleanup completes before TestMain writes a record.
func nativeDiagnosticRun(writer io.Writer) int {
	if handled, code := auth.HelperMain(os.Args[1:]); handled {
		return code
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	done := make(chan struct{})
	stopped := make(chan struct{})
	defer func() { close(done); <-stopped }()
	go func() {
		defer close(stopped)
		select {
		case sig := <-signals:
			code := 130
			if sig == syscall.SIGTERM {
				code = 143
			}
			cancel(&terminal.ExitError{Code: code})
		case <-done:
		}
	}()
	if !cli.WorkerRunInvocation(os.Args[1:]) && !cli.InteractiveInvocation(os.Args[1:], os.Stdin, os.Stdout) {
		var end context.CancelFunc
		ctx, end = context.WithTimeout(ctx, 2*time.Minute)
		defer end()
	}
	code := cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, writer, cli.Dependencies{})

	return code
}

type nativeDiagnosticWriter struct {
	forward  io.Writer
	bytes    []byte
	overflow bool
}

func (w *nativeDiagnosticWriter) Write(p []byte) (int, error) {
	// Return exactly the original writer's outcome; capture adds no I/O here.
	n, err := w.forward.Write(p)
	if !w.overflow {
		if len(p) > nativeDiagnosticLimit-len(w.bytes) {
			w.bytes = nil
			w.overflow = true
		} else {
			w.bytes = append(w.bytes, p...)
		}
	}
	return n, err
}

type nativeDiagnosticRow struct {
	Schema             int    `json:"schema_version"`
	DiagnosticOnly     bool   `json:"diagnostic_only"`
	AcceptanceEligible bool   `json:"acceptance_eligible"`
	Status             string `json:"status"`
	Code               string `json:"code"`
	Durability         string `json:"durability"`
	Exit               int    `json:"exit_code"`
}

func nativeDiagnosticSafeCode(code string) bool {
	switch code {
	case "validation", "unsupported_contract", "state_corrupt", "state_busy", "local_write_unknown", "clock_unavailable", "clock_conflict", "binding_unavailable", "event_conflict", "event_gap", "ordering_unavailable", "profile_required", "profile_invalidated", "profile_revoked", "untracked", "review_required", "source_lost", "restart_unknown", "incomplete_wait", "source_loss_while_waiting", "internal":
		return true
	}
	return false
}

func (w *nativeDiagnosticWriter) record(exit int) nativeDiagnosticRow {
	r := nativeDiagnosticRow{Schema: 1, DiagnosticOnly: true, Status: "unavailable", Exit: exit}
	if w.overflow {
		return r
	}
	if len(w.bytes) == 0 {
		r.Status = "empty"
		return r
	}
	line := string(w.bytes)
	if !strings.HasPrefix(line, "tempo hook: ") || !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
		return r
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(line, "tempo hook: "), "\n"), "; durability=")
	if len(parts) != 2 || !nativeDiagnosticSafeCode(parts[0]) {
		return r
	}
	if parts[1] != "committed" && parts[1] != "not_committed" && parts[1] != "unknown" {
		return r
	}
	r.Status, r.Code, r.Durability = "safe_error", parts[0], parts[1]
	return r
}

// A finite random-start namespace bounds disk use at 128 records, each <=256
// bytes. O_EXCL never replaces an existing entry; no raw stderr is written.
func nativeDiagnosticRecord(path, devText, inoText string, row nativeDiagnosticRow) (err error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	dev, e := strconv.ParseUint(devText, 10, 64)
	if e != nil {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	ino, e := strconv.ParseUint(inoText, 10, 64)
	if e != nil {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	data, e := json.Marshal(row)
	if e != nil || len(data)+1 > nativeDiagnosticLimit {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	data = append(data, '\n')
	fd, e := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	defer func() {
		if unix.Close(fd) != nil {
			err = fmt.Errorf("diagnostic_sink_unavailable")
		}
	}()
	var info unix.Stat_t
	if unix.Fstat(fd, &info) != nil || info.Mode&unix.S_IFMT != unix.S_IFDIR || info.Mode&0777 != 0700 || info.Uid != uint32(os.Getuid()) || uint64(info.Dev) != dev || uint64(info.Ino) != ino {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	var random [1]byte
	if _, e = rand.Read(random[:]); e != nil {
		return fmt.Errorf("diagnostic_sink_unavailable")
	}
	for offset := 0; offset < nativeDiagnosticSlots; offset++ {
		name := fmt.Sprintf("r%03d.json", (int(random[0])+offset)%nativeDiagnosticSlots)
		child, openErr := unix.Openat(fd, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if openErr == unix.EEXIST {
			continue
		}
		if openErr != nil {
			return fmt.Errorf("diagnostic_sink_unavailable")
		}
		var file unix.Stat_t
		valid := unix.Fstat(child, &file) == nil && file.Mode&unix.S_IFMT == unix.S_IFREG && file.Mode&0777 == 0600 && file.Uid == uint32(os.Getuid()) && file.Nlink == 1
		written, writeErr := 0, error(nil)
		if valid {
			written, writeErr = unix.Write(child, data)
		}
		closeErr := unix.Close(child)
		if !valid || writeErr != nil || written != len(data) || closeErr != nil {
			return fmt.Errorf("diagnostic_sink_unavailable")
		}
		return nil
	}
	return fmt.Errorf("diagnostic_sink_full")
}
