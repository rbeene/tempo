//go:build (darwin || linux) && (amd64 || arm64)

package activity

// First-Link operation fixtures use real public native owners and the existing
// calibrated inert SQL hook. They do not implement any of the proposed APIs.
import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
	"golang.org/x/sys/unix"
)

const flQASecondRequest = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const flQAThirdRequest = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
const flQAMetaColumns = "singleton,schema_version,legacy_schema_version,computer_id,revision,sync_enabled,durability_nonce,state_basename,database_basename,migration_id,backup_sha256,logical_bytes"
const flQABindingColumns = "binding_id,revision,account_id,user_id,project_id,task_id,timezone,computer_id,active,record_present,kind,locator,deleted"

var flQATables = []string{"store_meta", "bindings", "requests", "actor_generations", "actors", "actor_uncertainties", "host_sessions", "host_turns", "host_tools", "host_receipts", "epochs", "segments", "segment_events", "uncertainties", "uncertainty_evidence", "event_receipts", "event_receipt_uncertainties", "event_ids", "union_frontier", "component_segments", "pending_finalization", "intervals", "interval_segments", "interval_components", "outbox", "sync_configurations", "sync_plans", "sync_parts", "sync_attempts", "pending_sync", "pending_sync_roots", "recovery_decisions"}

func flQAService(t *testing.T) (*Service, LinkInput, interopFixture) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(base, "absent", "activity.json")
	s := New(Options{Path: path, Clock: ClockFunc(func() (ClockSample, error) { t.Fatal("Link sampled Clock"); return ClockSample{}, nil }), ResolveBinding: func(context.Context, Event) (BindingSnapshot, bool, error) {
		t.Fatal("Link called BindingResolver")
		return BindingSnapshot{}, false, nil
	}})
	in := qaLinkInput(t)
	d, a, b, err := sqliteLocation(path)
	if err != nil {
		t.Fatal(err)
	}
	return s, in, interopFixture{d, a, b}
}
func flQAAttribution() Attribution {
	return Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}
}
func flQAResultError(t *testing.T, r BindingResult, err error, code string) *Error {
	t.Helper()
	if !reflect.DeepEqual(r, BindingResult{}) {
		t.Fatalf("refusal leaked result: %+v", r)
	}
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("expected %s, got %T %v", code, err, err)
	}
	if code == "local_write_unknown" && (!e.Uncertain || e.Details["request_id"] != qaLinkRequest || len(e.Details) != 1) {
		t.Fatalf("unknown missing safe retry identity: %+v", e)
	}
	return e
}
func flQASuccess(t *testing.T, s *Service, in LinkInput, p *qaLinkProvider) BindingResult {
	t.Helper()
	r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	if r.ContractVersion != 1 || r.RequestID != in.RequestID || !validUUID(r.Binding.ID) || r.Binding.AttachedActors == nil {
		t.Fatalf("unowned/invalid result %+v", r)
	}
	return r
}
func flQABootstrap(t *testing.T) (*Service, LinkInput, interopFixture, BindingResult) {
	t.Helper()
	s, in, f := flQAService(t)
	return s, in, f, flQASuccess(t, s, in, qaNewLinkProvider(t))
}
func flQAPrepared(t *testing.T, in LinkInput) sqliteLinkPrepared {
	t.Helper()
	path, err := lexicalPath(in.Path)
	if err != nil {
		t.Fatal(err)
	}
	in.Path = path
	loc, err := DiscoverLocation(context.Background(), in.Path)
	if err != nil {
		t.Fatal(err)
	}
	return sqliteLinkPrepared{Input: in, Fingerprint: mutationFingerprint("bindings.link", in), Location: loc, Attribution: flQAAttribution()}
}
func flQAForbiddenDeps(t *testing.T) LinkDependencies {
	return LinkDependencies{ResolveAccount: func(context.Context) (string, error) { t.Fatal("replay resolved account"); return "", nil }, NewProvider: func(context.Context, string) (harvest.Provider, error) {
		t.Fatal("replay constructed provider")
		return nil, nil
	}}
}
func flQARows(t *testing.T, tx *sqliteio.Tx, query string, values ...sqliteio.Value) [][]string {
	t.Helper()
	s := interopPrepare(t, tx, query, values...)
	out := [][]string{}
	for {
		present, err := s.Step()
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			break
		}
		row := []string{}
		for col := 0; col < s.ColumnCount(); col++ {
			kind, err := s.Kind(col)
			if err != nil {
				t.Fatal(err)
			}
			var value string
			switch kind {
			case sqliteio.TextKind:
				value, err = s.Text(col)
			case sqliteio.BlobKind:
				var b []byte
				b, err = s.Blob(col)
				value = fmt.Sprintf("%x", b)
			case sqliteio.IntegerKind:
				var n int64
				n, err = s.Int64(col)
				value = fmt.Sprint(n)
			case sqliteio.NullKind:
				value = "NULL"
			default:
				t.Fatal("unexpected native kind")
			}
			if err != nil {
				t.Fatal(err)
			}
			row = append(row, fmt.Sprintf("%v:%s", kind, value))
		}
		out = append(out, row)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}
func flQACatalog(t *testing.T, tx *sqliteio.Tx) [][]string {
	return flQARows(t, tx, "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")
}
func flQAExpectedCatalog(t *testing.T) [][]string {
	t.Helper()
	f := interopLocation(t)
	interopInitialize(t, f, interopMeta(f))
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	out := flQACatalog(t, tx)
	interopRollback(t, tx)
	interopClose(t, c)
	if len(out) != 85 {
		t.Fatalf("actual canonical application objects=%d", len(out))
	}
	return out
}
func flQASnapshotTx(t *testing.T, tx *sqliteio.Tx) map[string][][]string {
	t.Helper()
	out := map[string][][]string{"catalog": flQACatalog(t, tx)}
	for _, table := range flQATables {
		out[table] = flQARows(t, tx, "SELECT * FROM "+table+" ORDER BY 1,2")
	}
	return out
}
func flQASnapshot(t *testing.T, f interopFixture) (sqliteStoreMeta, map[string][][]string) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	m, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil {
		t.Fatal(err)
	}
	rows := flQASnapshotTx(t, tx)
	interopRollback(t, tx)
	interopClose(t, c)
	return m, rows
}
func flQAUnchanged(t *testing.T, f interopFixture, m sqliteStoreMeta, rows map[string][][]string) {
	t.Helper()
	got, snapshot := flQASnapshot(t, f)
	if !reflect.DeepEqual(got, m) || !reflect.DeepEqual(snapshot, rows) {
		t.Fatal("cold metadata/domain/history changed after refusal")
	}
}
func flQAWrite(t *testing.T, f interopFixture, fn func(*sqliteio.Tx, sqliteStoreMeta)) {
	t.Helper()
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	m, err := sqliteReadMeta(tx, f.authority, f.database)
	if err != nil {
		t.Fatal(err)
	}
	fn(tx, m)
	interopCommit(t, tx)
	interopClose(t, c)
}
func flQAReaccount(t *testing.T, tx *sqliteio.Tx, m sqliteStoreMeta) {
	t.Helper()
	_, mc := asQALiteralAudit(t, tx, "store_meta", flQAMetaColumns, "singleton")
	_, bc := asQALiteralAudit(t, tx, "bindings", flQABindingColumns, "binding_id")
	_, rc := asQALiteralAudit(t, tx, "requests", "request_id,operation,fingerprint,outcome_kind,payload", "request_id")
	m.LogicalBytes = mc - 34 + bc + rc
	interopDone(t, tx, "UPDATE store_meta SET logical_bytes=? WHERE singleton=1", sqliteio.Integer(m.LogicalBytes))
}
func flQAFixtureService(t *testing.T, f interopFixture) *Service {
	t.Helper()
	return New(Options{Path: filepath.Join(f.directory, f.authority), Clock: ClockFunc(func() (ClockSample, error) { t.Fatal("Link clock callback"); return ClockSample{}, nil })})
}
func flQAActorFixture(t *testing.T, kind string) (*Service, LinkInput, interopFixture, []ActorRef) {
	t.Helper()
	h := qaNew(t)
	h.seed()
	h.ingest(0, qaEvent("z", "1", "1", "work", qaBindingA))
	h.ingest(1, qaEvent("a", "1", "1", "work", qaBindingA))
	if kind != "working" {
		h.ingest(2, qaEvent("z", "1", "2", kind, ""))
		h.ingest(3, qaEvent("a", "1", "2", kind, ""))
	}
	source := bgQAReadLegacy(t, h.service)
	in := qaLinkInput(t)
	loc, err := DiscoverLocation(context.Background(), in.Path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := source.Bindings[qaBindingA]
	if source.BindingRecords == nil {
		source.BindingRecords = map[string]bindingRecord{}
	}
	source.BindingRecords[qaBindingA] = bindingRecord{Snapshot: snapshot, Kind: loc.Kind, Locator: loc.Locator}
	// Retain B for the unrelated-history control, on a distinct valid timer so
	// it cannot mask terminal-actor positives with an active same-timer conflict.
	peer := source.Bindings[qaBindingB]
	peer.Attribution.ProjectID = "7"
	source.Bindings[qaBindingB] = peer
	if !validState(source) {
		t.Fatal("complete actor fixture after optional-map/timer amendment is invalid")
	}
	refs := []ActorRef{}
	for _, a := range source.Actors {
		refs = append(refs, a.Ref)
	}
	if actorKey(refs[0].Key) > actorKey(refs[1].Key) {
		refs[0], refs[1] = refs[1], refs[0]
	}
	f, _, _ := asQASeed(t, source)
	return flQAFixtureService(t, f), in, f, refs
}
func flQAImage(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(path, p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(p)
			if err != nil {
				return err
			}
			out[rel] = "link:" + target
			return nil
		}
		if d.IsDir() {
			out[rel] = fmt.Sprint(info.Mode())
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out[rel] = fmt.Sprintf("%v:%x", info.Mode(), data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// These subprocesses inherit the outer native runner's process group. The
// caller owns stdin, Wait and the exact child PID; no shared group is signaled.
func TestSQLiteLinkOwnedAdmissionHelper(t *testing.T) {
	mode := os.Getenv("TEMPO_SQLITE_LINK_QA_HELPER")
	if mode == "" {
		return
	}
	dir, a, b := os.Getenv("TEMPO_SQLITE_LINK_QA_DIR"), os.Getenv("TEMPO_SQLITE_LINK_QA_A"), os.Getenv("TEMPO_SQLITE_LINK_QA_D")
	if !filepath.IsAbs(dir) || filepath.Base(a) != a || filepath.Base(b) != b {
		t.Fatal("invalid confined helper location")
	}
	// Optional source-first environment control. It observes the real launched
	// child before any fixture ownership; normal helper use leaves it inactive.
	if want, check := os.LookupEnv("TEMPO_SQLITE_LINK_QA_EXPECT_GORACE"); check {
		entries := 0
		for _, entry := range os.Environ() {
			if strings.HasPrefix(entry, "GORACE=") {
				entries++
			}
		}
		if entries != 1 || os.Getenv("GORACE") != want {
			t.Fatal("owned child did not receive exactly one preserved GORACE with final zero exit grace")
		}
	}
	if mode == "guard" {
		fd, err := unix.Open(filepath.Join(dir, a+".lock"), unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if err = unix.Flock(fd, unix.LOCK_EX); err != nil {
			closeErr := unix.Close(fd)
			t.Fatalf("checked failed guard acquisition/release: %v %v", err, closeErr)
		}
		fmt.Println("READY")
		_, readErr := io.Copy(io.Discard, os.Stdin)
		unlockErr := unix.Flock(fd, unix.LOCK_UN)
		closeErr := unix.Close(fd)
		if readErr != nil || unlockErr != nil || closeErr != nil {
			t.Fatalf("guard release %v %v %v", readErr, unlockErr, closeErr)
		}
		return
	}
	if mode != "writer" {
		t.Fatal("unknown helper mode")
	}
	c, err := sqliteio.Open(context.Background(), dir, b, sqliteio.Options{AcquireDeadline: time.Now().Add(time.Second)})
	flQARetainConn(t, c)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := c.Begin(context.Background(), sqliteio.Write)
	flQARetainTx(t, tx)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	_, readErr := io.Copy(io.Discard, os.Stdin)
	rollbackErr := tx.Rollback()
	closeErr := c.Close(context.Background())
	if readErr != nil || rollbackErr != nil || closeErr != nil {
		t.Fatalf("writer release %v %v %v", readErr, rollbackErr, closeErr)
	}
}

// Register fallback ownership immediately; explicit terminal calls remain
// checked at their original assertions. Arguments capture each concrete owner.
func flQARetainConn(t *testing.T, c *sqliteio.Conn) {
	t.Helper()
	if c != nil {
		t.Cleanup(func() {
			if err := c.Close(context.Background()); err != nil {
				t.Errorf("checked retained connection cleanup: %v", err)
			}
		})
	}
}
func flQARetainTx(t *testing.T, tx *sqliteio.Tx) {
	t.Helper()
	if tx != nil {
		t.Cleanup(func() {
			if err := tx.Rollback(); err != nil {
				t.Errorf("checked retained transaction cleanup: %v", err)
			}
		})
	}
}

func flQAOwnedHelper(t *testing.T, f interopFixture, mode string) func() {
	t.Helper()
	if err := os.MkdirAll(f.directory, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLiteLinkOwnedAdmissionHelper$", "-test.v")
	// Preserve the caller's options verbatim, changing only this owned child's
	// final exit-grace option. Do not replace detector/reporting configuration.
	raceOptions := ""
	for _, entry := range os.Environ() {
		if value, ok := strings.CutPrefix(entry, "GORACE="); ok {
			raceOptions = value
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	if raceOptions != "" {
		raceOptions += " "
	}
	raceOptions += "atexit_sleep_ms=0"
	cmd.Env = append(cmd.Env, "GORACE="+raceOptions, "TEMPO_SQLITE_LINK_QA_HELPER="+mode, "TEMPO_SQLITE_LINK_QA_DIR="+f.directory, "TEMPO_SQLITE_LINK_QA_A="+f.authority, "TEMPO_SQLITE_LINK_QA_D="+f.database)
	var stdin io.WriteCloser
	var stdout io.ReadCloser
	var scanned chan struct{}
	var scanErr error
	started := false
	var once sync.Once
	release := func() {
		once.Do(func() {
			if stdin != nil {
				if err := stdin.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Errorf("checked helper stdin release: %v", err)
				}
			}
			if started {
				// Drain/join the reader before Wait closes the parent's StdoutPipe. The
				// child exits on owned stdin EOF, or its original5s context kills that PID.
				<-scanned
				waitErr := cmd.Wait()
				exit := -1
				if cmd.ProcessState != nil {
					exit = cmd.ProcessState.ExitCode()
				}
				t.Logf("owned writer joined pid=%d exit=%d mode=%s", cmd.Process.Pid, exit, mode)
				if waitErr != nil || scanErr != nil {
					t.Errorf("checked helper Wait/scanner: %v %v", waitErr, scanErr)
				}
			}
			if stdout != nil {
				if err := stdout.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
					t.Errorf("checked helper stdout release: %v", err)
				}
			}
			cancel()
		})
	}
	// Own every subsequently acquired pipe, process and scanner before any READY
	// or PG assertion can terminate the caller; release is idempotent.
	t.Cleanup(release)
	var err error
	stdin, err = cmd.StdinPipe()
	if err != nil {
		release()
		t.Fatal(err)
	}
	stdout, err = cmd.StdoutPipe()
	if err != nil {
		release()
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		release()
		t.Fatal(err)
	}
	started = true
	ready := make(chan error, 1)
	scanned = make(chan struct{})
	go func() {
		defer close(scanned)
		sc := bufio.NewScanner(stdout)
		seen := false
		for sc.Scan() {
			if sc.Text() == "READY" && !seen {
				seen = true
				ready <- nil
			}
		}
		scanErr = sc.Err()
		if !seen {
			ready <- errors.New("helper did not publish actual lock ownership")
		}
	}()
	pgid, err := unix.Getpgid(cmd.Process.Pid)
	if err != nil {
		cancel()
		release()
		t.Fatal(err)
	}
	t.Logf("owned writer started pid=%d pgid=%d mode=%s", cmd.Process.Pid, pgid, mode)
	select {
	case err = <-ready:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		cancel()
		release()
		t.Fatal(err)
	}
	return release
}

// Read and Write share the hook's Begin label. Calibrate the successful new-
// request and replay paths independently: the final Begin preceding the one
// actual COMMIT101 and durable closure is the writer ordinal for that path.
func flQAFinalWriterOrdinal(t *testing.T, replay bool) int {
	t.Helper()
	s, in, f, first := flQABootstrap(t)
	if !replay {
		in.RequestID = flQASecondRequest
	}
	begins, writerOrdinal, commits, durable := 0, 0, 0, 0
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
		}
		if e.Phase == "commit-after-engine" && e.Operation == "commit" && e.Code == 101 {
			commits++
			writerOrdinal = begins
		}
		if e.Phase == "durable-native-closed" && e.Operation == "close-durably" && e.Code == 0 {
			durable++
		}
	}})
	deps := qaLinkDeps(t, qaNewLinkProvider(t))
	if replay {
		deps = flQAForbiddenDeps(t)
	}
	r, err := s.linkSQLite(context.Background(), in, deps)
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil || commits != 1 || durable != 1 || writerOrdinal < 2 || begins != writerOrdinal {
		t.Fatalf("successful writer calibration replay=%t begins=%d writer=%d COMMIT101=%d durable=%d err=%v", replay, begins, writerOrdinal, commits, durable, err)
	}
	if replay {
		if !reflect.DeepEqual(r, first) {
			t.Fatal("replay calibration lost historical result")
		}
	} else if r.Changed || r.SnapshotRevision != "2" || r.Binding.ID != first.Binding.ID {
		t.Fatal("new-request writer calibration not a real committed no-op")
	}
	m, rows := flQASnapshot(t, f)
	want := 1
	if !replay {
		want = 2
	}
	if len(rows["requests"]) != want || m.Revision != fmt.Sprint(want) {
		t.Fatal("cold writer calibration receipt/revision mismatch")
	}
	t.Logf("actual final Write calibration replay=%t ordinal=%d COMMIT101=%d durable=%d", replay, writerOrdinal, commits, durable)
	return writerOrdinal
}
func flQABeginWait(t *testing.T, s *Service, in LinkInput, f interopFixture, replay bool, occupied func()) (BindingResult, error) {
	t.Helper()
	writerOrdinal := flQAFinalWriterOrdinal(t, replay)
	release := flQAOwnedHelper(t, f, "writer")
	observed := make(chan struct{}, 1)
	begins := 0
	type result struct {
		r   BindingResult
		err error
	}
	done := make(chan result, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	started, joined := false, false
	var got result
	finish := func() {
		cancel()
		release()
		if started && !joined {
			got = <-done
			joined = true
		}
		// Never clear global hooks or let fixture cleanup run over a live operation.
		spQASetSQLHooks(spQASQLHooks{})
	}
	defer finish()
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "begin" {
			begins++
			if begins == writerOrdinal {
				select {
				case observed <- struct{}{}:
				default:
				}
			}
		}
	}})
	started = true
	go func() {
		var r result
		defer func() { done <- r }()
		deps := qaLinkDeps(t, qaNewLinkProvider(t))
		if replay {
			deps = flQAForbiddenDeps(t)
		}
		r.r, r.err = s.linkSQLite(ctx, in, deps)
	}()
	select {
	case <-observed:
		// The positively calibrated final Write is reached while the independent
		// native writer remains held. Only now create P, then release its blocker.
		occupied()
		release()
	case got = <-done:
		joined = true
		finish()
		t.Fatalf("no actual calibrated writer wait: %+v %v", got.r, got.err)
	case <-ctx.Done():
		finish()
		t.Fatal("no calibrated writer admission reached; operation joined")
	}
	got = <-done
	joined = true
	finish()
	return got.r, got.err
}
func flQACanonicalPayload(t *testing.T, r BindingResult) string {
	t.Helper()
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func flQAPressure(t *testing.T, err error, reason string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != "validation" || e.Details["reason"] != reason {
		t.Fatalf("expected pressure %s, got %v", reason, err)
	}
}
func flQAProviderTimer(t *testing.T, p *qaLinkProvider, task string) {
	t.Helper()
	p.assignments = []harvest.Object{{"is_active": true, "project": harvest.Object{"id": json.Number("3")}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number(task)}}}}}
}
func flQAWithoutNonce(rows map[string][][]string) map[string][][]string {
	out := map[string][][]string{}
	for table, value := range rows {
		copyRows := [][]string{}
		for _, row := range value {
			copyRow := append([]string{}, row...)
			if table == "store_meta" {
				copyRow[6] = "nonce omitted"
			}
			copyRows = append(copyRows, copyRow)
		}
		out[table] = copyRows
	}
	return out
}
func flQAStateAbsent(t *testing.T, s *Service) {
	t.Helper()
	if _, err := os.Lstat(filepath.Dir(s.store.path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("preparation created state parent: %v", err)
	}
}
func flQAAssertNoLeak(t *testing.T, err error, f interopFixture) {
	t.Helper()
	interopSafeError(t, err, f.directory, f.database, f.authority, "secret-transport")
	var pe *os.PathError
	if errors.As(err, &pe) {
		t.Fatal("retained raw PathError")
	}
}
func flQAGit(t *testing.T, dir string, args ...string) { t.Helper(); qaGit(t, dir, args...) }
func flQAClosedFault(t *testing.T, phase, operation string, cause error) func() bool {
	t.Helper()
	fired := false
	spQAHooks(t, spQASQLHooks{Fault: func(e spQASQLEvent) error {
		if !fired && e.Phase == phase && (operation == "" || e.Operation == operation) {
			fired = true
			return cause
		}
		return nil
	}})
	return func() bool { spQASetSQLHooks(spQASQLHooks{}); return fired }
}
func flQACalibrate(t *testing.T) {
	t.Helper()
	f := interopLocation(t)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	spQAHookPositive(t, tx)
	interopRollback(t, tx)
	interopClose(t, c)
}
func flQAText(t *testing.T, tx *sqliteio.Tx, query string, values ...sqliteio.Value) string {
	t.Helper()
	s := interopPrepare(t, tx, query, values...)
	present, err := s.Step()
	if !present || err != nil {
		t.Fatal("missing literal text", err)
	}
	v, err := s.Text(0)
	if err != nil {
		t.Fatal(err)
	}
	present, err = s.Step()
	closeErr := s.Close()
	if present || err != nil || closeErr != nil {
		t.Fatal("unchecked text terminal", err, closeErr)
	}
	return v
}
func flQAFiniteDeadline(t *testing.T, start time.Time, budget time.Duration) {
	t.Helper()
	elapsed := time.Since(start)
	if elapsed > budget+750*time.Millisecond {
		t.Fatalf("finite native budget reset or exceeded: %v", elapsed)
	}
}
