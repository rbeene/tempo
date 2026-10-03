//go:build (darwin || linux) && (amd64 || arm64)

package activity

// DIAGNOSTIC ONLY: nil/state_busy are recorded, not product acceptance. No
// changed timeout, fault injection, nested fixture I/O, goroutine, or helper.
// Existing hooks expose leases and native boundaries, not pinDirectory calls.
import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type cptQAMark struct {
	Name string `json:"name"`
	NS   int64  `json:"ns"`
}

type cptQABucket struct {
	Lease          int            `json:"lease"`
	StartNS        int64          `json:"start_ns"`
	EndNS          int64          `json:"end_ns"`
	Counts         map[string]int `json:"counts"`
	CheckGapNS     int64          `json:"step_before_to_native_ns"`
	CheckGapMaxNS  int64          `json:"step_before_to_native_max_ns"`
	NativeNS       int64          `json:"native_step_ns"`
	NativeMaxNS    int64          `json:"native_step_max_ns"`
	before, native time.Time
}

type cptQAReport struct {
	DiagnosticOnly bool           `json:"diagnostic_only"`
	Label          string         `json:"label"`
	ElapsedNS      int64          `json:"elapsed_ns"`
	BudgetNS       int64          `json:"admission_budget_ns"`
	PathComponents int            `json:"path_components"`
	Code           string         `json:"public_code"`
	NativePhase    string         `json:"native_phase,omitempty"`
	NativeCategory string         `json:"native_category,omitempty"`
	Marks          []cptQAMark    `json:"marks"`
	Buckets        []*cptQABucket `json:"lease_buckets"`
	FSCounts       map[string]int `json:"fs_counts"`
}

type cptQATrace struct {
	mu      sync.Mutex
	start   time.Time
	report  cptQAReport
	current *cptQABucket
}

func (p *cptQATrace) markLocked(name string, now time.Time) {
	p.report.Marks = append(p.report.Marks, cptQAMark{name, now.Sub(p.start).Nanoseconds()})
}

func (p *cptQATrace) mark(name string) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.markLocked(name, now)
}

func (p *cptQATrace) sql(e ncQASQLEvent) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.current
	if b == nil {
		b = p.report.Buckets[0] // Events outside an observed root lease.
	}
	b.Counts[e.Operation+"/"+e.Phase]++
	switch {
	case e.Operation == "statement" && e.Phase == "step-before":
		b.before = now
	case e.Operation == "statement" && e.Phase == "step-before-native":
		if !b.before.IsZero() {
			d := now.Sub(b.before).Nanoseconds()
			b.CheckGapNS += d
			b.CheckGapMaxNS = max(b.CheckGapMaxNS, d)
			b.before = time.Time{}
		}
		b.native = now
	case e.Operation == "statement" && e.Phase == "step-after":
		if !b.native.IsZero() {
			d := now.Sub(b.native).Nanoseconds()
			b.NativeNS += d
			b.NativeMaxNS = max(b.NativeMaxNS, d)
			b.native = time.Time{}
		}
	case e.Operation == "begin" || e.Operation == "commit" || e.Operation == "rollback" || e.Operation == "close":
		p.markLocked("sql/"+e.Operation+"/"+e.Phase, now)
	}
}

func (p *cptQATrace) fs(e ncQAFSEvent) {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	// Namespace, FD, paths, SQL text, identities and result payloads are omitted.
	p.report.FSCounts[e.Role+"/"+e.Op+"/"+e.Phase]++
	if e.Role != "root" || e.Op != "lease" {
		return
	}
	p.markLocked("fs/root/lease/"+e.Phase, now)
	if e.Phase == "acquired" {
		b := &cptQABucket{Lease: len(p.report.Buckets), StartNS: now.Sub(p.start).Nanoseconds(), Counts: map[string]int{}}
		p.report.Buckets = append(p.report.Buckets, b)
		p.current = b
	} else if e.Phase == "released" && p.current != nil {
		p.current.EndNS = now.Sub(p.start).Nanoseconds()
		p.current = nil
	}
}

func cptQAMeasure(t *testing.T, label string, s *Service, call func() error) error {
	t.Helper()
	if s.store.timeout != 0 {
		t.Fatal("diagnostic requires unchanged default admission budget")
	}
	p := &cptQATrace{report: cptQAReport{
		DiagnosticOnly: true, Label: label, BudgetNS: int64(250 * time.Millisecond),
		PathComponents: len(strings.Split(strings.Trim(filepath.Clean(filepath.Dir(s.store.path)), "/"), "/")),
		Buckets:        []*cptQABucket{{Counts: map[string]int{}}}, FSCounts: map[string]int{},
	}}
	clock, resolve := s.clock, s.resolve
	s.clock = ClockFunc(func() (ClockSample, error) {
		p.mark("clock/enter")
		defer p.mark("clock/exit")
		return clock.Sample()
	})
	if resolve != nil {
		s.resolve = func(ctx context.Context, e Event) (BindingSnapshot, bool, error) {
			p.mark("resolver/enter")
			defer p.mark("resolver/exit")
			return resolve(ctx, e)
		}
	}
	ncQASetSQLHooks(ncQASQLHooks{Observe: p.sql})
	ncQASetFSHooks(ncQAFSHooks{Observe: p.fs})
	defer func() {
		ncQASetSQLHooks(ncQASQLHooks{})
		ncQASetFSHooks(ncQAFSHooks{})
		s.clock, s.resolve = clock, resolve
	}()
	p.start = time.Now()
	p.mark("call/enter")
	err := call()
	p.mark("call/exit")
	elapsed := time.Since(p.start)
	// Log only after measured work and after detaching the global observers.
	ncQASetSQLHooks(ncQASQLHooks{})
	ncQASetFSHooks(ncQAFSHooks{})
	p.mu.Lock()
	p.report.ElapsedNS = elapsed.Nanoseconds()
	p.report.Code = "ok"
	if err != nil {
		p.report.Code = "unexpected"
		var public *Error
		if errors.As(err, &public) {
			p.report.Code = public.Code
		}
		var native *sqliteio.Error
		if errors.As(err, &native) {
			p.report.NativePhase = string(native.Phase)
			p.report.NativeCategory = string(native.Category)
		}
	}
	b, marshalErr := json.Marshal(p.report)
	p.mu.Unlock()
	if marshalErr != nil {
		t.Fatal("diagnostic report encoding failed")
	}
	t.Log("CAPTURE_PHASE_DIAGNOSTIC " + string(b))
	if err != nil && p.report.Code != "state_busy" {
		t.Fatalf("diagnostic unexpected safe category: %s", p.report.Code)
	}
	return err
}

func TestSQLiteCapturePhaseTimingDiagnostic(t *testing.T) {
	t.Log("DIAGNOSTIC ONLY: completion records measurements; it does not accept the first flow, throughput, or state_busy behavior")
	t.Run("actual-host-prefix", func(t *testing.T) {
		h := hiQANew(t, "codex", 1)
		start := h.event("SessionStart", "", "")
		start.SessionSource = "startup"
		events := []HostEvent{start, h.event("UserPromptSubmit", "root-turn", ""), h.event("SubagentStart", "child-turn", "child")}
		for n, e := range events {
			h.at([]int64{0, 0, 10}[n])
			err := cptQAMeasure(t, "host/"+e.Kind, h.s, func() error {
				_, err := h.s.ingestHostSQLite(context.Background(), e)
				return err
			})
			if err != nil {
				t.Log("remaining prefix not attempted after recorded busy outcome")
				break
			}
		}
		_ = cptQAMeasure(t, "status/after-recorded-host-prefix", h.s, func() error {
			_, err := h.s.statusSQLite(context.Background())
			return err
		})
	})
	t.Run("child-preparation-only", func(t *testing.T) {
		h := hiQANew(t, "codex", 1)
		h.start()
		h.send(0, h.event("UserPromptSubmit", "root-turn", ""))
		h.at(10)
		e := h.event("SubagentStart", "child-turn", "child")
		// This direct preparation probe is separate from the preceding full-call
		// measurement. It does not write or replace the actual host operation.
		_ = cptQAMeasure(t, "host/SubagentStart/preparation-only", h.s, func() error {
			a := sqliteCaptureAdmission{Directory: h.f.directory, StateBasename: h.f.authority, DatabaseBasename: h.f.database, AcquireDeadline: time.Now().Add(250 * time.Millisecond)}
			_, found, err := h.s.sqlitePrepareHost(context.Background(), e, a)
			if err == nil && !found {
				return failure("state_corrupt")
			}
			return err
		})
	})
	t.Run("fresh-link-normalized", func(t *testing.T) {
		s, in, f := flQAService(t)
		deps := qaLinkDeps(t, qaNewLinkProvider(t))
		var linked BindingResult
		if err := cptQAMeasure(t, "fresh/Link", s, func() error {
			var err error
			linked, err = s.linkSQLite(context.Background(), in, deps)
			return err
		}); err != nil {
			return
		}
		// Exact identity read is fixture setup, outside every observed window.
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		meta, err := sqliteReadMeta(tx, f.authority, f.database)
		if err != nil {
			t.Fatal("diagnostic identity fixture failed")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		sample := ncQASample(0)
		s = New(Options{Path: s.store.path, Clock: ClockFunc(func() (ClockSample, error) { return sample, nil })})
		e := qaEvent("timing-actor", "1", "1", "work", linked.Binding.ID)
		e.Actor.ComputerID, e.BindingRevision = meta.ComputerID, linked.Binding.Revision
		if err := cptQAMeasure(t, "normalized/new", s, func() error {
			_, err := s.ingestSQLite(context.Background(), e)
			return err
		}); err != nil {
			return
		}
		sample = ncQASample(10)
		e.Sequence, e.Kind, e.BindingID, e.BindingRevision = "2", "observe_work", "", ""
		e.EventID = "timing-actor/1/2"
		_ = cptQAMeasure(t, "normalized/observe", s, func() error {
			_, err := s.ingestSQLite(context.Background(), e)
			return err
		})
	})
}
