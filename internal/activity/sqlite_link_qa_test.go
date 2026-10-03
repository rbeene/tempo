//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Behavioral first-Link QA. The four activity functions and the two public
// native prerequisites are intentionally absent until the approved producers
// land. That is a compile prerequisite, never a behavioral RED classification.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
	"github.com/rbeene/tempo/internal/harvest"
)

func TestSQLiteLinkBootstrapExactRowsCatalogChargeAndOwnership(t *testing.T) {
	canonical := flQAExpectedCatalog(t)
	s, in, f := flQAService(t)
	original := in
	p := qaNewLinkProvider(t)
	r := flQASuccess(t, s, in, p)
	loc, err := DiscoverLocation(context.Background(), in.Path)
	if err != nil {
		t.Fatal(err)
	}
	if in != original || !r.Changed || r.SnapshotRevision != "1" || r.Binding.Revision != "1" || r.Binding.Kind != loc.Kind || r.Binding.Locator != loc.Locator || r.Binding.Attribution != flQAAttribution() || len(r.Binding.AttachedActors) != 0 {
		t.Fatalf("bootstrap result %+v", r)
	}
	if !reflect.DeepEqual(p.calls, []string{"accounts", "/users/me", "/users/me/project_assignments"}) {
		t.Fatalf("provider preparation %+v", p.calls)
	}
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	m, found, err := sqliteReadLinkSchema(tx, f.authority, f.database)
	if err != nil || !found {
		t.Fatal("typed current schema admission", err)
	}
	if m.Revision != "1" || !validUUID(m.ComputerID) || m.SyncEnabled || m.DurabilityNonce != ([16]byte{}) || m.MigrationID != nil || m.BackupSHA256 != nil {
		t.Fatalf("bootstrap meta %+v", m)
	}
	if !reflect.DeepEqual(flQACatalog(t, tx), canonical) {
		t.Fatal("bootstrap catalog differs from actual native-canonical current DDL")
	}
	for _, table := range flQATables {
		want := int64(0)
		if table == "store_meta" || table == "bindings" || table == "requests" {
			want = 1
		}
		if got := interopCount(t, tx, "SELECT count(*) FROM "+table); got != want {
			t.Fatalf("%s=%d want%d", table, got, want)
		}
	}
	b, present, err := sqliteReadBinding(tx, m.ComputerID, r.Binding.ID)
	if err != nil || !present || b.Record == nil || b.Record.Deleted || b.Snapshot != (BindingSnapshot{ID: r.Binding.ID, Revision: "1", Attribution: flQAAttribution()}) {
		t.Fatal("exact binding materialization", err)
	}
	payload := flQAText(t, tx, "SELECT payload FROM requests WHERE request_id=?", sqliteio.Text(in.RequestID))
	if payload != flQACanonicalPayload(t, r) {
		t.Fatal("receipt payload is not exact canonical concrete BindingResult")
	}
	req, present, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, in.RequestID, "1")
	if err != nil || !present || req.Value.Operation != "bindings.link" || req.Value.Fingerprint != mutationFingerprint("bindings.link", original) || req.Value.BindingResult == nil || !reflect.DeepEqual(*req.Value.BindingResult, r) {
		t.Fatal("receipt identity/result differs", err)
	}
	_, mc := asQALiteralAudit(t, tx, "store_meta", flQAMetaColumns, "singleton")
	_, bc := asQALiteralAudit(t, tx, "bindings", flQABindingColumns, "binding_id")
	_, rc := asQALiteralAudit(t, tx, "requests", "request_id,operation,fingerprint,outcome_kind,payload", "request_id")
	want := int64(114 + len(m.ComputerID) + len(f.authority) + len(f.database) + 157 + len(r.Binding.ID) + 4 + len("UTC") + len(m.ComputerID) + len(loc.Kind) + len(loc.Locator) + 77 + len(in.RequestID) + len("bindings.link") + 64 + len("binding_result") + len(payload))
	if mc-34+bc+rc != want || m.LogicalBytes != want {
		t.Fatalf("literal independent row charges native=%d formula=%d meta=%d", mc-34+bc+rc, want, m.LogicalBytes)
	}
	if err = tx.CheckForeignKeys(); err != nil {
		t.Fatal(err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	// Change the caller-owned result and retained decoder objects after closure.
	r.Binding.Locator = "caller-owned"
	req.Value.BindingResult.Binding.Attribution.AccountID = "caller-owned"
	restarted := flQAFixtureService(t, f)
	got, err := restarted.linkSQLite(context.Background(), original, flQAForbiddenDeps(t))
	if err != nil || got.Binding.Locator != loc.Locator || got.Binding.Attribution != flQAAttribution() {
		t.Fatal("closed-owner mutation contaminated durable receipt", err)
	}
}

func TestSQLiteLinkValidationPreparationAndAssignedConsentDoNotInitialize(t *testing.T) {
	cases := []struct {
		name, code string
		edit       func(*LinkInput, *qaLinkProvider)
	}{
		{"request UUID", "validation", func(in *LinkInput, p *qaLinkProvider) { in.RequestID = "secret-transport" }},
		{"missing project", "input_required", func(in *LinkInput, p *qaLinkProvider) { in.ProjectID = "" }},
		{"noncanonical revision", "validation", func(in *LinkInput, p *qaLinkProvider) { in.IfRevision = "01" }},
		{"first zero revision", "revision_conflict", func(in *LinkInput, p *qaLinkProvider) { in.IfRevision = "0" }},
		{"timezone Local", "validation", func(in *LinkInput, p *qaLinkProvider) { in.Timezone = "Local" }},
		{"inactive user", "forbidden", func(in *LinkInput, p *qaLinkProvider) { p.user["is_active"] = false }},
		{"unassigned project", "validation", func(in *LinkInput, p *qaLinkProvider) { p.assignments = nil }},
		{"inactive task", "validation", func(in *LinkInput, p *qaLinkProvider) {
			p.assignments[0]["task_assignments"] = []any{harvest.Object{"is_active": false, "task": harvest.Object{"id": json.Number("4")}}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, in, f := flQAService(t)
			p := qaNewLinkProvider(t)
			tc.edit(&in, p)
			original := in
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			flQAResultError(t, r, err, tc.code)
			flQAAssertNoLeak(t, err, f)
			if in != original {
				t.Fatal("input mutated")
			}
			flQAStateAbsent(t, s)
		})
	}
	t.Run("ambiguous inferred task", func(t *testing.T) {
		s, in, _ := flQAService(t)
		in.TaskID = ""
		p := qaNewLinkProvider(t)
		p.assignments[0]["task_assignments"] = []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("4")}}, harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("5")}}}
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
		flQAResultError(t, r, err, "input_required")
		flQAStateAbsent(t, s)
	})
	t.Run("provider failure preserves absent and pristine", func(t *testing.T) {
		for _, pristine := range []bool{false, true} {
			s, in, f := flQAService(t)
			if pristine {
				if err := os.MkdirAll(f.directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(f.directory, f.database), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			before := flQAImage(t, f.directory)
			p := qaNewLinkProvider(t)
			p.listErr = failure("response")
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			if err == nil || !reflect.DeepEqual(r, BindingResult{}) {
				t.Fatal("failed preparation returned result")
			}
			flQAAssertNoLeak(t, err, f)
			if !reflect.DeepEqual(flQAImage(t, f.directory), before) {
				t.Fatal("failed preparation altered physical absent/pristine image")
			}
		}
	})
	t.Run("location changes after provider", func(t *testing.T) {
		s, in, _ := flQAService(t)
		p := qaNewLinkProvider(t)
		p.beforeList = func() {
			if err := os.Remove(in.Path); err != nil {
				t.Fatal(err)
			}
		}
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
		if err == nil || !reflect.DeepEqual(r, BindingResult{}) {
			t.Fatal("vanished location accepted")
		}
		flQAStateAbsent(t, s)
	})
}

func TestSQLiteLinkRawFingerprintLexicalPathGitAndFinalEncoding(t *testing.T) {
	t.Run("inferred versus explicit", func(t *testing.T) {
		s, in, f := flQAService(t)
		in.AccountID = ""
		in.TaskID = ""
		original := in
		p := qaNewLinkProvider(t)
		deps := LinkDependencies{ResolveAccount: func(context.Context) (string, error) { return "1", nil }, NewProvider: func(context.Context, string) (harvest.Provider, error) { return p, nil }}
		r, err := s.linkSQLite(context.Background(), in, deps)
		if err != nil {
			t.Fatal(err)
		}
		if r.Binding.Attribution != flQAAttribution() || in != original {
			t.Fatal("inference changed caller input")
		}
		m, rows := flQASnapshot(t, f)
		explicit := in
		explicit.AccountID = "1"
		explicit.TaskID = "4"
		explicit.Timezone = "UTC"
		r, err = s.linkSQLite(context.Background(), explicit, flQAForbiddenDeps(t))
		flQAResultError(t, r, err, "request_conflict")
		flQAUnchanged(t, f, m, rows)
	})
	t.Run("equivalent lexical spelling replays", func(t *testing.T) {
		s, in, f, first := flQABootstrap(t)
		before, rows := flQASnapshot(t, f)
		original := in
		in.Path = in.Path + string(os.PathSeparator) + "."
		got, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		if err != nil || !reflect.DeepEqual(got, first) {
			t.Fatal("equivalent Abs/Clean spelling did not replay", err)
		}
		if in.Path == original.Path {
			t.Fatal("control did not preserve caller's different spelling")
		}
		after, afterRows := flQASnapshot(t, f)
		nextNonce, nonceErr := sqliteNextNonce(before.DurabilityNonce[:])
		if nonceErr != nil {
			t.Fatal(nonceErr)
		}
		if after.DurabilityNonce != nextNonce || after.Revision != before.Revision || !reflect.DeepEqual(flQAWithoutNonce(rows), flQAWithoutNonce(afterRows)) {
			t.Fatal("lexically equivalent replay changed more than nonce")
		}
	})
	t.Run("different lexical alias remains different intent", func(t *testing.T) {
		s, in, f, _ := flQABootstrap(t)
		before, rows := flQASnapshot(t, f)
		alias := filepath.Join(t.TempDir(), "alias")
		if err := os.Symlink(in.Path, alias); err != nil {
			t.Fatal(err)
		}
		in.Path = alias
		got, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		flQAResultError(t, got, err, "request_conflict")
		flQAUnchanged(t, f, before, rows)
	})
	t.Run("real Git worktree canonical root", func(t *testing.T) {
		s, in, f := flQAService(t)
		repo := t.TempDir()
		flQAGit(t, repo, "init")
		flQAGit(t, repo, "-c", "core.hooksPath=/dev/null", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "fixture")
		work := filepath.Join(t.TempDir(), "linked")
		flQAGit(t, repo, "worktree", "add", "-b", "fixture-link", work)
		child := filepath.Join(work, "child")
		if err := os.Mkdir(child, 0700); err != nil {
			t.Fatal(err)
		}
		in.Path = child
		r := flQASuccess(t, s, in, qaNewLinkProvider(t))
		loc, err := DiscoverLocation(context.Background(), child)
		if err != nil || r.Binding.Kind != loc.Kind || r.Binding.Locator != loc.Locator || loc.Kind != "repository" {
			t.Fatal("real worktree location not retained", err)
		}
		_, rows := flQASnapshot(t, f)
		if len(rows["bindings"]) != 1 {
			t.Fatal("worktree generated duplicate bindings")
		}
		in.Path = repo
		in.RequestID = flQASecondRequest
		shared := flQASuccess(t, s, in, qaNewLinkProvider(t))
		if shared.Changed || shared.Binding.ID != r.Binding.ID || shared.Binding.Locator != r.Binding.Locator {
			t.Fatal("linked worktree and original repository did not share canonical common Git locator")
		}
	})
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"short invalid locator materializes", "/raw/" + string([]byte{0xff, 0xfe}), true},
		{"final expansion over4096 refused", "/" + strings.Repeat(string([]byte{0xff}), 1366), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := interopLocation(t)
			in := qaLinkInput(t)
			original := in
			p := flQAPrepared(t, in)
			p.Location.Kind = "directory"
			p.Location.Locator = tc.raw
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
				t.Fatal(err)
			}
			r, err := sqliteLinkTransaction(tx, f.authority, f.database, p)
			if tc.valid {
				var repaired string
				b, encodeErr := json.Marshal(tc.raw)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				if decodeErr := json.Unmarshal(b, &repaired); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if err != nil || r.Binding.Locator != repaired || r.Binding.Locator == tc.raw {
					t.Fatal("actual JSON boundary short repair differs", err)
				}
				interopCommit(t, tx)
			} else {
				if err == nil || !reflect.DeepEqual(r, BindingResult{}) {
					t.Fatal("expanded locator accepted")
				}
				interopRollback(t, tx)
			}
			interopClose(t, c)
			if in != original || p.Location.Locator != tc.raw {
				t.Fatal("final encoder repaired caller-owned raw identity")
			}
		})
	}
}

func TestSQLiteLinkOccupiedAuthorityPrecedesExternalWork(t *testing.T) {
	for _, kind := range []string{"regular", "directory", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			s, in, f := flQAService(t)
			if err := os.MkdirAll(f.directory, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.directory, f.authority)
			var err error
			switch kind {
			case "regular":
				err = os.WriteFile(path, []byte("do not parse me"), 0600)
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink("missing-owned-target", path)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := flQAImage(t, f.directory)
			in.Path = filepath.Join(t.TempDir(), "missing")
			r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
			flQAResultError(t, r, err, "state_path_in_use")
			if !reflect.DeepEqual(flQAImage(t, f.directory), before) {
				t.Fatal("occupied authority or database image changed")
			}
		})
	}
}

func TestSQLiteLinkReplayBeforeProviderGitIsRealNonceCommit(t *testing.T) {
	s, in, f, r := flQABootstrap(t)
	before, rows := flQASnapshot(t, f)
	if err := os.Remove(in.Path); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir())
	commits := 0
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "commit" {
			commits++
		}
	}})
	got, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil || !reflect.DeepEqual(got, r) || commits != 1 {
		t.Fatalf("replay %+v error%v actualCOMMIT%d", got, err, commits)
	}
	after, afterRows := flQASnapshot(t, f)
	nextNonce, nonceErr := sqliteNextNonce(before.DurabilityNonce[:])
	if nonceErr != nil {
		t.Fatal("checked next nonce", nonceErr)
	}
	if after.Revision != before.Revision || after.LogicalBytes != before.LogicalBytes || after.ComputerID != before.ComputerID || after.SyncEnabled != before.SyncEnabled || after.DurabilityNonce != nextNonce || !reflect.DeepEqual(flQAWithoutNonce(rows), flQAWithoutNonce(afterRows)) {
		t.Fatal("replay altered more than nonce")
	}
}

func TestSQLiteLinkReceiptMismatchCorruptionRetainedBindingAndNonceCAS(t *testing.T) {
	for _, tc := range []struct{ name, code, sql string }{
		{"wrong operation", "request_conflict", "UPDATE requests SET operation='bindings.repair'"},
		{"wrong fingerprint", "request_conflict", "UPDATE requests SET fingerprint='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'"},
		{"unknown field", "state_corrupt", "UPDATE requests SET payload=substr(payload,1,length(payload)-1)||',\"unknown\":1}'"},
		{"duplicate field", "state_corrupt", "UPDATE requests SET payload=substr(payload,1,length(payload)-1)||',\"contract_version\":1}'"},
		{"wrong outcome", "state_corrupt", "UPDATE requests SET outcome_kind='error'"},
		{"missing retained binding", "state_corrupt", "DELETE FROM bindings"},
		{"wrong binding computer", "state_corrupt", "UPDATE bindings SET computer_id='99999999-9999-4999-8999-999999999999'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) { interopDone(t, tx, tc.sql); flQAReaccount(t, tx, m) })
			m, rows := flQASnapshot(t, f)
			r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
			flQAResultError(t, r, err, tc.code)
			flQAUnchanged(t, f, m, rows)
		})
	}
	t.Run("historical binding deleted moved newer remains replayable", func(t *testing.T) {
		s, in, f, r := flQABootstrap(t)
		flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
			interopDone(t, tx, "UPDATE bindings SET revision=?,active=0,deleted=1,locator=?,task_id='5'", interopCounter(t, "8"), sqliteio.Text(filepath.Join(t.TempDir(), "moved")))
			interopDone(t, tx, "UPDATE store_meta SET revision=?", interopCounter(t, "9"))
			flQAReaccount(t, tx, m)
		})
		got, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Fatal("retained historical receipt wrongly compared to current locator/attribution", err)
		}
	})
	t.Run("future receipt and older retained binding", func(t *testing.T) {
		for _, future := range []bool{false, true} {
			s, in, f, r := flQABootstrap(t)
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				if future {
					r.SnapshotRevision = "2"
				} else {
					r.Binding.Revision = "2"
				}
				interopDone(t, tx, "UPDATE requests SET payload=?", sqliteio.Text(flQACanonicalPayload(t, r)))
				flQAReaccount(t, tx, m)
			})
			m, rows := flQASnapshot(t, f)
			got, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
			flQAResultError(t, got, err, "state_corrupt")
			flQAUnchanged(t, f, m, rows)
		}
	})
	t.Run("full before metadata CAS and caller transaction", func(t *testing.T) {
		_, in, f, _ := flQABootstrap(t)
		m, before := flQASnapshot(t, f)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
			t.Fatal(err)
		}
		interopDone(t, tx, "UPDATE store_meta SET durability_nonce=?", sqliteio.Blob(make([]byte, 16)))
		stale := m
		stale.DurabilityNonce[0] = 1
		r, found, err := sqliteReplayLink(tx, stale, in.RequestID, mutationFingerprint("bindings.link", in))
		if err == nil || found || !reflect.DeepEqual(r, BindingResult{}) {
			t.Fatal("stale full-before CAS accepted")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		flQAUnchanged(t, f, m, before)
	})
}

func TestSQLiteLinkNonceWrapAndFullUint64RevisionBoundaries(t *testing.T) {
	t.Run("wrap and maximum public replay", func(t *testing.T) {
		s, in, f, want := flQABootstrap(t)
		flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
			interopDone(t, tx, "UPDATE store_meta SET revision=?,durability_nonce=?", interopCounter(t, "18446744073709551615"), sqliteio.Blob([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}))
		})
		m, rows := flQASnapshot(t, f)
		r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		if err != nil || !reflect.DeepEqual(r, want) {
			t.Fatal("maximum public revision replay refused", err)
		}
		after, afterRows := flQASnapshot(t, f)
		if after.Revision != m.Revision || after.DurabilityNonce != ([16]byte{}) || !reflect.DeepEqual(flQAWithoutNonce(rows), flQAWithoutNonce(afterRows)) {
			t.Fatal("nonce wrap modified logical revision/data")
		}
	})
	for _, bindingMax := range []bool{false, true} {
		t.Run(fmt.Sprintf("new request maximum binding=%t", bindingMax), func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				if bindingMax {
					interopDone(t, tx, "UPDATE bindings SET revision=?", interopCounter(t, "18446744073709551615"))
				} else {
					interopDone(t, tx, "UPDATE store_meta SET revision=?", interopCounter(t, "18446744073709551615"))
				}
			})
			m, rows := flQASnapshot(t, f)
			in.RequestID = flQASecondRequest
			p := qaNewLinkProvider(t)
			if bindingMax {
				in.IfRevision = "18446744073709551615"
				in.TaskID = "5"
				flQAProviderTimer(t, p, "5")
			}
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			flQAResultError(t, r, err, "validation")
			flQAUnchanged(t, f, m, rows)
		})
	}
}

func TestSQLiteLinkNewNoopAndChangedBindingRevisionRechecks(t *testing.T) {
	s, in, f, first := flQABootstrap(t)
	in.RequestID = flQASecondRequest
	second := flQASuccess(t, s, in, qaNewLinkProvider(t))
	if second.Changed || second.SnapshotRevision != "2" || second.Binding.ID != first.Binding.ID || second.Binding.Revision != "1" {
		t.Fatalf("new no-op %+v", second)
	}
	m, rows := flQASnapshot(t, f)
	if len(rows["requests"]) != 2 || m.Revision != "2" {
		t.Fatal("no-op did not create receipt and public revision")
	}
	in.RequestID = flQAThirdRequest
	in.TaskID = "5"
	p := qaNewLinkProvider(t)
	flQAProviderTimer(t, p, "5")
	r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
	flQAResultError(t, r, err, "revision_conflict")
	flQAUnchanged(t, f, m, rows)
	in.IfRevision = "1"
	changed := flQASuccess(t, s, in, p)
	if !changed.Changed || changed.SnapshotRevision != "3" || changed.Binding.ID != first.Binding.ID || changed.Binding.Revision != "2" || changed.Binding.Attribution.TaskID != "5" {
		t.Fatalf("changed %+v", changed)
	}
	t.Run("current revision changed during provider", func(t *testing.T) {
		s, in, f, _ := flQABootstrap(t)
		in.RequestID = flQASecondRequest
		in.IfRevision = "1"
		in.TaskID = "5"
		p := qaNewLinkProvider(t)
		flQAProviderTimer(t, p, "5")
		var after sqliteStoreMeta
		var rows map[string][][]string
		p.beforeList = func() {
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				interopDone(t, tx, "UPDATE bindings SET revision=?", interopCounter(t, "2"))
			})
			after, rows = flQASnapshot(t, f)
		}
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
		flQAResultError(t, r, err, "revision_conflict")
		flQAUnchanged(t, f, after, rows)
	})
}

func TestSQLiteLinkSelectedActorClosureConflictsAndOwnedSortedRefs(t *testing.T) {
	for _, kind := range []string{"working", "wait_user", "wait_permission", "wait_children"} {
		t.Run(kind, func(t *testing.T) {
			s, in, f, refs := flQAActorFixture(t, kind)
			r := flQASuccess(t, s, in, qaNewLinkProvider(t))
			if r.Changed || !reflect.DeepEqual(r.Binding.AttachedActors, refs) {
				t.Fatalf("selected attached refs %+v want%+v", r.Binding.AttachedActors, refs)
			}
			r.Binding.AttachedActors[0].Key.AgentID = "caller-mutated"
			m, rows := flQASnapshot(t, f)
			in.RequestID = flQASecondRequest
			in.IfRevision = "1"
			in.TaskID = "5"
			p := qaNewLinkProvider(t)
			flQAProviderTimer(t, p, "5")
			got, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			flQAResultError(t, got, err, "binding_in_use")
			flQAUnchanged(t, f, m, rows)
		})
	}
	for _, health := range []string{"stale", "order_blocked"} {
		t.Run(health, func(t *testing.T) {
			s, in, f, _ := flQAActorFixture(t, "wait_user")
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				interopDone(t, tx, "UPDATE actors SET health=?", sqliteio.Text(health))
			})
			m, rows := flQASnapshot(t, f)
			in.IfRevision = "1"
			in.TaskID = "5"
			p := qaNewLinkProvider(t)
			flQAProviderTimer(t, p, "5")
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			flQAResultError(t, r, err, "binding_in_use")
			flQAUnchanged(t, f, m, rows)
		})
	}
	for _, kind := range []string{"finish", "interrupt"} {
		t.Run("terminal "+kind, func(t *testing.T) {
			s, in, _, _ := flQAActorFixture(t, kind)
			in.TaskID = "5"
			in.IfRevision = "1"
			p := qaNewLinkProvider(t)
			flQAProviderTimer(t, p, "5")
			r := flQASuccess(t, s, in, p)
			if !r.Changed || len(r.Binding.AttachedActors) != 0 || r.Binding.AttachedActors == nil {
				t.Fatal("terminal actor blocked change")
			}
		})
	}
	t.Run("corrupt selected actor", func(t *testing.T) {
		s, in, f, _ := flQAActorFixture(t, "wait_user")
		flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) { interopDone(t, tx, "UPDATE actors SET account_id='9'") })
		m, rows := flQASnapshot(t, f)
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
		flQAResultError(t, r, err, "state_corrupt")
		flQAUnchanged(t, f, m, rows)
	})
}

func TestSQLiteLinkSameTimerPeersIncludeRecordlessAndExcludeDeleted(t *testing.T) {
	for _, kind := range []string{"recordless incompatible", "recordless compatible", "deleted", "other timer"} {
		t.Run(kind, func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				row := sqliteBindingRow{ComputerID: m.ComputerID, Snapshot: BindingSnapshot{ID: qaBindingB, Revision: "1", Attribution: flQAAttribution()}}
				if kind != "recordless compatible" {
					row.Snapshot.Attribution.TaskID = "5"
				}
				if kind == "other timer" {
					row.Snapshot.Attribution.ProjectID = "7"
				}
				if kind == "deleted" {
					record := bindingRecord{Snapshot: row.Snapshot, Kind: "directory", Locator: filepath.Join(t.TempDir(), "deleted"), Deleted: true}
					row.Record = &record
				}
				if _, err := sqliteInsertBinding(tx, row); err != nil {
					t.Fatal(err)
				}
				flQAReaccount(t, tx, m)
			})
			m, rows := flQASnapshot(t, f)
			in.RequestID = flQASecondRequest
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			if kind == "recordless incompatible" {
				flQAResultError(t, r, err, "attribution_conflict")
				flQAUnchanged(t, f, m, rows)
			} else if err != nil || r.Changed {
				t.Fatal("nonconflicting peer rejected", err)
			}
		})
	}
}

func TestSQLiteLinkTwoFirstContendersThroughOwnedProcessGuard(t *testing.T) {
	for _, scenario := range []string{"same request", "different request same intent", "different request changed intent"} {
		t.Run(scenario, func(t *testing.T) {
			s, in, f := flQAService(t)
			release := flQAOwnedHelper(t, f, "guard")
			second := in
			if scenario != "same request" {
				second.RequestID = flQASecondRequest
			}
			p1, p2 := qaNewLinkProvider(t), qaNewLinkProvider(t)
			if scenario == "different request changed intent" {
				second.TaskID = "5"
				flQAProviderTimer(t, p2, "5")
			}
			ready := make(chan struct{}, 2)
			p1.beforeList = func() { ready <- struct{}{} }
			p2.beforeList = func() { ready <- struct{}{} }
			type outcome struct {
				r   BindingResult
				err error
			}
			ch := make(chan outcome, 2)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			outcomes := []outcome{}
			started := 0
			finish := func() {
				cancel()
				release()
				// Every started worker publishes even on reused fixture Fatal/Goexit.
				// Join them before hook/helper/temp cleanup on both success and failure.
				for len(outcomes) < started {
					outcomes = append(outcomes, <-ch)
				}
			}
			defer finish()
			started++
			go func() {
				var got outcome
				defer func() { ch <- got }()
				got.r, got.err = s.linkSQLite(ctx, in, qaLinkDeps(t, p1))
			}()
			started++
			go func() {
				var got outcome
				defer func() { ch <- got }()
				got.r, got.err = s.linkSQLite(ctx, second, qaLinkDeps(t, p2))
			}()
			for i := 0; i < 2; i++ {
				select {
				case <-ready:
				case <-ctx.Done():
					finish()
					t.Fatal("contenders did not reach outside-SQL preparation; both joined")
				}
			}
			release()
			outcomes = append(outcomes, <-ch, <-ch)
			finish()
			a, b := outcomes[0], outcomes[1]
			if scenario == "different request changed intent" {
				if a.err != nil && b.err == nil {
					a, b = b, a
				}
				if a.err != nil {
					t.Fatal("no winner", a.err)
				}
				flQAResultError(t, b.r, b.err, "revision_conflict")
			} else {
				if a.err != nil || b.err != nil {
					t.Fatal("same intent contenders", a.err, b.err)
				}
				if a.r.Binding.ID != b.r.Binding.ID {
					t.Fatal("two first bindings")
				}
				if scenario == "same request" && !reflect.DeepEqual(a.r, b.r) {
					t.Fatal("same UUID not replayed")
				}
			}
			m, rows := flQASnapshot(t, f)
			want := 1
			if scenario == "different request same intent" {
				want = 2
			}
			if len(rows["bindings"]) != 1 || len(rows["requests"]) != want || m.Revision != fmt.Sprint(want) {
				t.Fatal("first contender durable cardinality/revision mismatch")
			}
		})
	}
}

func TestSQLiteLinkPreparationFailureWinnerReceiptWithoutRetry(t *testing.T) {
	for _, matching := range []bool{false, true} {
		t.Run(fmt.Sprintf("matching=%t", matching), func(t *testing.T) {
			s, in, f := flQAService(t)
			p := qaNewLinkProvider(t)
			p.listErr = failure("response")
			var winner BindingResult
			p.beforeList = func() {
				other := in
				if !matching {
					other.RequestID = flQASecondRequest
				}
				winner = flQASuccess(t, s, other, qaNewLinkProvider(t))
			}
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			if matching {
				if err != nil || !reflect.DeepEqual(r, winner) {
					t.Fatal("matching receipt did not win preparation error", err)
				}
			} else {
				flQAResultError(t, r, err, "response")
				if !errors.Is(err, p.listErr) {
					t.Fatal("different receipt erased original preparation failure")
				}
			}
			if len(p.calls) != 3 {
				t.Fatal("provider was retried")
			}
			m, rows := flQASnapshot(t, f)
			if m.Revision != "1" || len(rows["requests"]) != 1 || len(rows["bindings"]) != 1 {
				t.Fatal("preparation winner changed logical state")
			}
		})
	}
	t.Run("failed winner inspection is not absence", func(t *testing.T) {
		s, in, f := flQAService(t)
		p := qaNewLinkProvider(t)
		p.listErr = failure("response")
		p.beforeList = func() {
			if err := os.MkdirAll(f.directory, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(f.directory, f.database), []byte("foreign main"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
		flQAResultError(t, r, err, "state_corrupt")
		data, err := os.ReadFile(filepath.Join(f.directory, f.database))
		if err != nil || string(data) != "foreign main" {
			t.Fatal("failed inspection fell through to initializer")
		}
	})
}

func TestSQLiteLinkTransactionHelpersDoNotCommitOrCloseCallerOwner(t *testing.T) {
	f := interopLocation(t)
	in := qaLinkInput(t)
	p := flQAPrepared(t, in)
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
		t.Fatal(err)
	}
	spQAHookPositive(t, tx)
	commits, closes := 0, 0
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "commit" {
			commits++
		}
		if e.Phase == "close-before" {
			closes++
		}
	}})
	before, found, err := sqliteReadLinkSchema(tx, f.authority, f.database)
	if err != nil || found || !reflect.DeepEqual(before, sqliteStoreMeta{}) {
		t.Fatal("actual empty catalog classification", err)
	}
	r, err := sqliteLinkTransaction(tx, f.authority, f.database, p)
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil || !r.Changed || commits != 0 || closes != 0 {
		t.Fatalf("unit stole caller ownership %v commits%d closes%d", err, commits, closes)
	}
	if interopCount(t, tx, "SELECT count(*) FROM requests") != 1 {
		t.Fatal("unit missing in-flight receipt")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	c, tx = interopOpen(t, f, false, sqliteio.Read)
	if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
		t.Fatal("rollback did not undo all DDL and domain rows")
	}
	interopRollback(t, tx)
	interopClose(t, c)
}

func TestSQLiteLinkActualRowDoneFinalizeFaultsRollbackAtomicUnit(t *testing.T) {
	flQACalibrate(t)
	// Record a successful actual unit and fault each reached native ROW, DONE and
	// finalize in separate real caller transactions. This enumerates native
	// terminals, not a guessed query/bind field or an implementation mirror.
	f := interopLocation(t)
	p := flQAPrepared(t, qaLinkInput(t))
	c, tx := interopOpen(t, f, true, sqliteio.Write)
	if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
		t.Fatal(err)
	}
	checkpoints := []spQASQLEvent{}
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if (e.Phase == "step-after" && (e.Code == 100 || e.Code == 101)) || (e.Phase == "finalize-after" && e.Code == 0) {
			checkpoints = append(checkpoints, e)
		}
	}})
	r, err := sqliteLinkTransaction(tx, f.authority, f.database, p)
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil || !r.Changed || len(checkpoints) == 0 {
		t.Fatal("actual unit calibration", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	for ordinal, target := range checkpoints {
		t.Run(fmt.Sprintf("terminal%03d-%s-code%d", ordinal, target.Phase, target.Code), func(t *testing.T) {
			f := interopLocation(t)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
				t.Fatal(err)
			}
			seen := 0
			fired := false
			spQAHooks(t, spQASQLHooks{Fault: func(e spQASQLEvent) error {
				if (e.Phase == "step-after" && (e.Code == 100 || e.Code == 101)) || (e.Phase == "finalize-after" && e.Code == 0) {
					if seen == ordinal {
						fired = true
						seen++
						return syscall.EIO
					}
					seen++
				}
				return nil
			}})
			r, err := sqliteLinkTransaction(tx, f.authority, f.database, p)
			spQASetSQLHooks(spQASQLHooks{})
			if !fired || err == nil || !reflect.DeepEqual(r, BindingResult{}) {
				t.Fatal("reached typed terminal fault not refused with zero output")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
				t.Fatal("late unit failure leaked schema/rows")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteLinkCommitOutcomeAndDurableClosureOverrideCapacity(t *testing.T) {
	flQACalibrate(t)
	for _, phase := range []string{"commit-before-dispatch", "commit-after-engine", "commit-before-verify", "commit-after-verify"} {
		for _, cause := range []syscall.Errno{syscall.ENOSPC, syscall.EIO, syscall.EACCES} {
			t.Run(fmt.Sprintf("%s-%d", phase, cause), func(t *testing.T) {
				s, in, f := flQAService(t)
				commits := 0
				fired := false
				spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
					if e.Phase == "control-before-native" && e.Operation == "commit" {
						commits++
					}
				}, Fault: func(e spQASQLEvent) error {
					if !fired && e.Phase == phase {
						fired = true
						return cause
					}
					return nil
				}})
				r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
				spQASetSQLHooks(spQASQLHooks{})
				if !fired {
					t.Fatal("commit fault was not reached")
				}
				flQAAssertNoLeak(t, err, f)
				if phase == "commit-before-dispatch" {
					if commits != 0 {
						t.Fatal("pre-dispatch fault still COMMITted")
					}
					if cause == syscall.ENOSPC {
						flQAResultError(t, r, err, "validation")
						flQAPressure(t, err, "database_capacity")
					} else {
						flQAResultError(t, r, err, "state_corrupt")
					}
					c, tx := interopOpen(t, f, false, sqliteio.Read)
					if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
						t.Fatal("definite noncommit leaked schema")
					}
					interopRollback(t, tx)
					interopClose(t, c)
				} else {
					flQAResultError(t, r, err, "local_write_unknown")
					if commits != 1 {
						t.Fatal("uncertain COMMIT retried")
					}
					m, rows := flQASnapshot(t, f)
					if m.Revision != "1" || len(rows["bindings"]) != 1 || len(rows["requests"]) != 1 {
						t.Fatal("actual engine-committed receipt not preserved")
					}
					got, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
					if err != nil || got.RequestID != in.RequestID {
						t.Fatal("uncertain result not recoverable by exact receipt", err)
					}
				}
			})
		}
	}
	for _, operation := range []string{"durable-main", "durable-wal", "durable-parent"} {
		t.Run("durable "+operation, func(t *testing.T) {
			s, in, f := flQAService(t)
			flQACalibrate(t)
			fired := flQAClosedFault(t, "fsync-before", operation, syscall.ENOSPC)
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			if !fired() {
				t.Fatal("durable fsync checkpoint not reached")
			}
			flQAResultError(t, r, err, "local_write_unknown")
			m, rows := flQASnapshot(t, f)
			if m.Revision != "1" || len(rows["requests"]) != 1 {
				t.Fatal("postcommit fsync uncertainty lost receipt")
			}
		})
	}
	t.Run("cancellation after real engine COMMIT", func(t *testing.T) {
		s, in, f := flQAService(t)
		ctx, cancel := context.WithCancel(context.Background())
		fired := false
		spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
			if e.Phase == "commit-after-engine" && !fired {
				fired = true
				cancel()
			}
		}})
		r, err := s.linkSQLite(ctx, in, qaLinkDeps(t, qaNewLinkProvider(t)))
		spQASetSQLHooks(spQASQLHooks{})
		cancel()
		if !fired {
			t.Fatal("actual commit not reached")
		}
		flQAResultError(t, r, err, "local_write_unknown")
		_, rows := flQASnapshot(t, f)
		if len(rows["requests"]) != 1 {
			t.Fatal("cancellation lost committed receipt")
		}
	})
}

func TestSQLiteLinkLogicalCapacityAndSafeErrnoEvidence(t *testing.T) {
	for _, logical := range []int64{64 * 1024 * 1024, 64*1024*1024 + 1} {
		t.Run(fmt.Sprint(logical), func(t *testing.T) {
			s, in, f, want := flQABootstrap(t)
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				interopDone(t, tx, "UPDATE store_meta SET logical_bytes=?", sqliteio.Integer(logical))
			})
			m, rows := flQASnapshot(t, f)
			if logical == 64*1024*1024 {
				r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
				if err != nil || !reflect.DeepEqual(r, want) {
					t.Fatal("exact-capacity zero-charge replay refused", err)
				}
				m, rows = flQASnapshot(t, f)
			}
			in.RequestID = flQASecondRequest
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			flQAResultError(t, r, err, "validation")
			flQAPressure(t, err, "logical_capacity")
			flQAUnchanged(t, f, m, rows)
		})
	}
	for _, cause := range []error{syscall.ENOSPC, &os.PathError{Op: "write", Path: "secret-transport", Err: syscall.ENOSPC}, syscall.EIO, syscall.EACCES} {
		t.Run(fmt.Sprintf("errno-%T-%v", cause, cause), func(t *testing.T) {
			flQACalibrate(t)
			f := interopLocation(t)
			c, tx := interopOpen(t, f, true, sqliteio.Write)
			fired := flQAClosedFault(t, "prepare-before", "prepare", cause)
			s, err := tx.Prepare("SELECT 1")
			if s != nil {
				if closeErr := s.Close(); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if !fired() {
				t.Fatal("real adapter fault boundary not reached")
			}
			var native *sqliteio.Error
			if !errors.As(err, &native) || native.Category != sqliteio.IO || native.Code != 0 {
				t.Fatalf("native safe evidence %v", err)
			}
			if errors.Is(cause, syscall.ENOSPC) {
				if native.Cause != syscall.ENOSPC || !errors.Is(err, syscall.ENOSPC) {
					t.Fatal("known ENOSPC was not retained as bare sentinel")
				}
			} else if native.Cause != nil {
				t.Fatal("generic IO errno was retained/guessed")
			}
			var pathErr *os.PathError
			if errors.As(err, &pathErr) || strings.Contains(err.Error(), "secret-transport") {
				t.Fatal("sanitizer retained raw PathError")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	t.Run("known receipt precommit ENOSPC remains unknown", func(t *testing.T) {
		s, in, f, _ := flQABootstrap(t)
		m, rows := flQASnapshot(t, f)
		flQACalibrate(t)
		fired := flQAClosedFault(t, "commit-before-dispatch", "commit", syscall.ENOSPC)
		r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		if !fired() {
			t.Fatal("replay fence not reached")
		}
		flQAResultError(t, r, err, "local_write_unknown")
		flQAUnchanged(t, f, m, rows)
	})
}

func TestSQLiteLinkFiniteAdmissionCancellationAndProviderOutsideOwners(t *testing.T) {
	for _, budget := range []time.Duration{-time.Nanosecond, time.Second + time.Nanosecond} {
		t.Run(budget.String(), func(t *testing.T) {
			s, in, f := flQAService(t)
			s = New(Options{Path: s.store.path, LockTimeout: budget})
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			flQAResultError(t, r, err, "validation")
			flQAAssertNoLeak(t, err, f)
			flQAStateAbsent(t, s)
		})
	}
	t.Run("already canceled no creation", func(t *testing.T) {
		s, in, _ := flQAService(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		r, err := s.linkSQLite(ctx, in, qaLinkDeps(t, qaNewLinkProvider(t)))
		flQAResultError(t, r, err, "state_busy")
		flQAStateAbsent(t, s)
	})
	t.Run("real guard wait budget", func(t *testing.T) {
		s, in, f := flQAService(t)
		s = New(Options{Path: s.store.path, LockTimeout: 80 * time.Millisecond})
		release := flQAOwnedHelper(t, f, "guard")
		before := flQAImage(t, f.directory)
		start := time.Now()
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
		flQAFiniteDeadline(t, start, 80*time.Millisecond)
		release()
		flQAResultError(t, r, err, "state_busy")
		if !reflect.DeepEqual(flQAImage(t, f.directory), before) {
			t.Fatal("guard timeout created main or application rows")
		}
	})
	t.Run("provider may acquire actual writer", func(t *testing.T) {
		s, in, f, _ := flQABootstrap(t)
		in.RequestID = flQASecondRequest
		p := qaNewLinkProvider(t)
		called := false
		p.beforeList = func() {
			called = true
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
				interopDone(t, tx, "UPDATE store_meta SET durability_nonce=?", sqliteio.Blob([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7}))
			})
		}
		r := flQASuccess(t, s, in, p)
		if !called || r.Changed {
			t.Fatal("provider was not executed before SQL owners")
		}
	})
}

func TestSQLiteLinkPublicInspectionNoncreatingReadOnlyAndCleanupOwner(t *testing.T) {
	for _, profile := range []string{"missing parent", "empty parent", "zero-byte pristine", "sidecar only", "foreign main"} {
		t.Run(profile, func(t *testing.T) {
			_, _, f := flQAService(t)
			if profile != "missing parent" {
				if err := os.MkdirAll(f.directory, 0700); err != nil {
					t.Fatal(err)
				}
			}
			switch profile {
			case "zero-byte pristine":
				if err := os.WriteFile(filepath.Join(f.directory, f.database), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "sidecar only":
				if err := os.WriteFile(filepath.Join(f.directory, f.database+"-wal"), []byte("foreign sidecar"), 0600); err != nil {
					t.Fatal(err)
				}
			case "foreign main":
				if err := os.WriteFile(filepath.Join(f.directory, f.database), []byte("foreign main"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before := flQAImage(t, f.directory)
			c, kind, err := sqliteio.InspectForLink(context.Background(), f.directory, f.authority, f.database, time.Now().Add(250*time.Millisecond))
			flQARetainConn(t, c)
			if c != nil {
				closeErr := c.Close(context.Background())
				if closeErr != nil {
					t.Fatal("inspection cleanup-only release", closeErr)
				}
			}
			switch profile {
			case "missing parent", "empty parent":
				if err != nil || c != nil || kind != sqliteio.LinkAbsent {
					t.Fatal("qualified absence", err)
				}
			case "zero-byte pristine":
				if err != nil || c != nil || kind != sqliteio.LinkPristine {
					t.Fatal("qualified pristine", err)
				}
			default:
				if err == nil || kind != 0 {
					t.Fatal("unsafe inspection became absence/pristine")
				}
			}
			if !reflect.DeepEqual(flQAImage(t, f.directory), before) {
				t.Fatal("noncreating profile observation altered bytes/roles")
			}
		})
	}
	t.Run("actual WAL read-only owner", func(t *testing.T) {
		_, in, f, r := flQABootstrap(t)
		m, before := flQASnapshot(t, f)
		c, kind, err := sqliteio.InspectForLink(context.Background(), f.directory, f.authority, f.database, time.Now().Add(250*time.Millisecond))
		flQARetainConn(t, c)
		if err != nil || c == nil || kind != sqliteio.LinkWAL {
			t.Fatal("WAL inspection", err)
		}
		tx, err := c.Begin(context.Background(), sqliteio.Write)
		flQARetainTx(t, tx)
		if tx != nil {
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				t.Fatal(rollbackErr)
			}
		}
		var native *sqliteio.Error
		if !errors.As(err, &native) || native.Category != sqliteio.Misuse {
			t.Fatal("inspection owner authorized Write", err)
		}
		interopClose(t, c)
		c, kind, err = sqliteio.InspectForLink(context.Background(), f.directory, f.authority, f.database, time.Now().Add(250*time.Millisecond))
		flQARetainConn(t, c)
		if err != nil || kind != sqliteio.LinkWAL {
			t.Fatal(err)
		}
		tx, err = c.Begin(context.Background(), sqliteio.Read)
		flQARetainTx(t, tx)
		if err != nil {
			interopClose(t, c)
			t.Fatal(err)
		}
		row, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, in.RequestID, m.Revision)
		if err != nil || !found || row.Value.BindingResult == nil || !reflect.DeepEqual(*row.Value.BindingResult, r) {
			t.Fatal("real read-only receipt", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		flQAUnchanged(t, f, m, before)
	})
	// Reached private probe/directory-close failures and the nonnil cleanup-only
	// owner handoff are independent native prerequisites. The existing hook ABI
	// cannot fault closeNative/inspectLinkParent; no activity-only seam is added.
	t.Run("expired budget nil-owner counterpart", func(t *testing.T) {
		_, _, f := flQAService(t)
		ctx := context.Background()
		c, kind, err := sqliteio.InspectForLink(ctx, f.directory, f.authority, f.database, time.Now().Add(-time.Second))
		flQARetainConn(t, c)
		if c != nil {
			interopClose(t, c)
		}
		var native *sqliteio.Error
		if !errors.As(err, &native) || native.Category != sqliteio.Busy || native.Code != 0 || !errors.Is(err, sqliteio.ErrBusy) || ctx.Err() != nil || c != nil || kind != 0 {
			t.Fatal("expired admission budget not checked nil-owner Busy", err)
		}
	})
}

func TestSQLiteLinkFinalAuthorityObservationAfterProviderAndWriterWait(t *testing.T) {
	for _, kind := range []string{"regular", "directory", "symlink"} {
		t.Run("provider "+kind, func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			in.RequestID = flQASecondRequest
			m, rows := flQASnapshot(t, f)
			p := qaNewLinkProvider(t)
			path := filepath.Join(f.directory, f.authority)
			p.beforeList = func() {
				var err error
				switch kind {
				case "regular":
					err = os.WriteFile(path, []byte("occupied after preparation"), 0600)
				case "directory":
					err = os.Mkdir(path, 0700)
				case "symlink":
					err = os.Symlink("missing-owned-target", path)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			flQAResultError(t, r, err, "state_path_in_use")
			flQAUnchanged(t, f, m, rows)
			if _, err = os.Lstat(path); err != nil {
				t.Fatal("occupied authority removed")
			}
		})
	}
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprintf("actual Write Begin wait replay=%t", replay), func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			if !replay {
				in.RequestID = flQASecondRequest
			}
			s = New(Options{Path: s.store.path, LockTimeout: time.Second})
			m, rows := flQASnapshot(t, f)
			path := filepath.Join(f.directory, f.authority)
			r, err := flQABeginWait(t, s, in, f, replay, func() {
				if err := os.WriteFile(path, []byte("created during actual native Begin wait"), 0600); err != nil {
					t.Fatal(err)
				}
			})
			code := "state_path_in_use"
			if replay {
				code = "local_write_unknown"
			}
			flQAResultError(t, r, err, code)
			flQAUnchanged(t, f, m, rows)
		})
	}
	t.Run("concrete admission check typed cause and no SQL", func(t *testing.T) {
		_, _, f, _ := flQABootstrap(t)
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		path := filepath.Join(f.directory, f.authority)
		if err := os.WriteFile(path, []byte("opaque"), 0600); err != nil {
			t.Fatal(err)
		}
		sqlCount := 0
		spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
			if e.Phase == "prepare-before" || e.Phase == "step-before-native" {
				sqlCount++
			}
		}})
		err := tx.CheckAuthorityAbsent(f.authority)
		spQASetSQLHooks(spQASQLHooks{})
		var native *sqliteio.Error
		if !errors.As(err, &native) || native.Phase != sqliteio.Admission || native.Category != sqliteio.Unsafe || !errors.Is(err, os.ErrExist) || sqlCount != 0 {
			t.Fatal("pinned admission did not return exact occupied-P cause before SQL", err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteLinkCatalogAdmissionRejectsSupplementalAndPartialDDL(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"extra table", "CREATE TABLE fixture_extra(value TEXT) STRICT"},
		{"extra index", "CREATE INDEX fixture_extra ON bindings(task_id)"},
		{"extra trigger", "CREATE TRIGGER fixture_extra AFTER INSERT ON requests BEGIN SELECT 1; END"},
		{"extra view", "CREATE VIEW fixture_extra AS SELECT binding_id FROM bindings"},
		{"changed covering index", "DROP INDEX binding_timer"},
		{"wrong database name", "UPDATE store_meta SET database_basename='foreign.sqlite3'"},
		{"wrong computer", "UPDATE store_meta SET computer_id='not-a-computer-uuid'"},
		{"migration and backup reserved", "UPDATE store_meta SET migration_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',backup_sha256='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'"},
		{"missing singleton", "DELETE FROM store_meta"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) { interopDone(t, tx, tc.sql) })
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			before := flQASnapshotTx(t, tx)
			interopRollback(t, tx)
			interopClose(t, c)
			in.RequestID = flQASecondRequest
			r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
			flQAResultError(t, r, err, "state_corrupt")
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			after := flQASnapshotTx(t, tx)
			interopRollback(t, tx)
			interopClose(t, c)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("unsupported catalog/schema metadata mutated before refusal")
			}
		})
	}
	t.Run("partial catalog is not empty initialization", func(t *testing.T) {
		s, in, f := flQAService(t)
		if err := os.MkdirAll(f.directory, 0700); err != nil {
			t.Fatal(err)
		}
		c, tx := interopOpen(t, f, true, sqliteio.Write)
		interopDone(t, tx, "CREATE TABLE fixture_partial(value TEXT) STRICT")
		interopCommit(t, tx)
		interopClose(t, c)
		r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		flQAResultError(t, r, err, "state_corrupt")
		c, tx = interopOpen(t, f, false, sqliteio.Read)
		if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 1 {
			t.Fatal("partial schema initialized")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteLinkLateActorAndUnrelatedRetainedHistory(t *testing.T) {
	t.Run("provider creates current nonterminal attachment", func(t *testing.T) {
		s, in, f, _ := flQAActorFixture(t, "finish")
		in.IfRevision = "1"
		in.TaskID = "5"
		p := qaNewLinkProvider(t)
		flQAProviderTimer(t, p, "5")
		var before sqliteStoreMeta
		var rows map[string][][]string
		p.beforeList = func() {
			flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) { interopDone(t, tx, "UPDATE actors SET state='wait_user'") })
			before, rows = flQASnapshot(t, f)
		}
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
		flQAResultError(t, r, err, "binding_in_use")
		flQAUnchanged(t, f, before, rows)
	})
	t.Run("unrelated malformed terminal closure is not reconstructed", func(t *testing.T) {
		s, in, f, _ := flQAActorFixture(t, "finish")
		flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
			interopDone(t, tx, "UPDATE actors SET binding_id=?,account_id='9'", sqliteio.Text(qaBindingB))
		})
		_, before := flQASnapshot(t, f)
		r := flQASuccess(t, s, in, qaNewLinkProvider(t))
		if r.Changed || r.Binding.AttachedActors == nil || len(r.Binding.AttachedActors) != 0 {
			t.Fatal("unrelated retained actor affected selected binding")
		}
		_, after := flQASnapshot(t, f)
		for _, table := range []string{"actors", "actor_generations", "segments", "segment_events", "epochs"} {
			if !reflect.DeepEqual(before[table], after[table]) {
				t.Fatalf("existing Link repaired/scanned retained %s", table)
			}
		}
	})
}

func TestSQLiteLinkPendingOrphanAndNativeConstraintEvidence(t *testing.T) {
	t.Run("binding receipt with pending projection is corruption", func(t *testing.T) {
		s, in, f, _ := flQABootstrap(t)
		flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
			interopDone(t, tx, "INSERT INTO pending_sync(request_id,singleton,kind,effect_committed,snapshot_revision,limit_count,outbox_id,entry_id,if_revision,retry_rejected,confirmed) VALUES(?,1,'run',0,?,1,NULL,NULL,NULL,NULL,NULL)", sqliteio.Text(in.RequestID), interopCounter(t, "1"))
		})
		m, rows := flQASnapshot(t, f)
		r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		flQAResultError(t, r, err, "state_corrupt")
		flQAUnchanged(t, f, m, rows)
	})
	t.Run("actual schema version CHECK275 preserves caller unit", func(t *testing.T) {
		f := interopLocation(t)
		in := qaLinkInput(t)
		c, tx := interopOpen(t, f, true, sqliteio.Write)
		if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
			t.Fatal(err)
		}
		r, err := sqliteLinkTransaction(tx, f.authority, f.database, flQAPrepared(t, in))
		if err != nil || !r.Changed {
			t.Fatal(err)
		}
		spQAHookPositive(t, tx)
		s := interopPrepare(t, tx, "UPDATE store_meta SET schema_version=2")
		present, err := s.Step()
		closeErr := s.Close()
		var native *sqliteio.Error
		if present || !errors.As(err, &native) || native.Category != sqliteio.Constraint || native.Code != 275 {
			t.Fatalf("real CHECK evidence %v", err)
		}
		if closeErr != nil {
			var terminal *sqliteio.Error
			if !errors.As(closeErr, &terminal) || terminal.Category != sqliteio.Constraint || terminal.Code != 275 {
				t.Fatal("unexpected constraint finalizer", closeErr)
			}
		}
		interopRollback(t, tx)
		interopClose(t, c)
		c, tx = interopOpen(t, f, false, sqliteio.Read)
		if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
			t.Fatal("constraint rollback left DDL/rows")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteLinkReplayHelperOwnsOnlySelectedWrites(t *testing.T) {
	_, in, f, want := flQABootstrap(t)
	m, before := flQASnapshot(t, f)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
		t.Fatal(err)
	}
	spQAHookPositive(t, tx)
	commits, closes := 0, 0
	spQAHooks(t, spQASQLHooks{Observe: func(e spQASQLEvent) {
		if e.Phase == "control-before-native" && e.Operation == "commit" {
			commits++
		}
		if e.Phase == "close-before" {
			closes++
		}
	}})
	r, found, err := sqliteReplayLink(tx, m, in.RequestID, mutationFingerprint("bindings.link", in))
	spQASetSQLHooks(spQASQLHooks{})
	if err != nil || !found || !reflect.DeepEqual(r, want) || commits != 0 || closes != 0 {
		t.Fatal("replay helper acknowledged/stole owner or lost receipt", err)
	}
	now, err := sqliteReadMeta(tx, f.authority, f.database)
	nextNonce, nonceErr := sqliteNextNonce(m.DurabilityNonce[:])
	if nonceErr != nil {
		t.Fatal("checked next nonce", nonceErr)
	}
	if err != nil || now.DurabilityNonce != nextNonce || now.Revision != m.Revision {
		t.Fatal("actual in-flight nonce CAS", err)
	}
	interopRollback(t, tx)
	interopClose(t, c)
	flQAUnchanged(t, f, m, before)
	c, tx = interopOpen(t, f, false, sqliteio.Write)
	if err := tx.CheckAuthorityAbsent(f.authority); err != nil {
		t.Fatal(err)
	}
	r, found, err = sqliteReplayLink(tx, m, flQASecondRequest, mutationFingerprint("bindings.link", in))
	if err != nil || found || !reflect.DeepEqual(r, BindingResult{}) {
		t.Fatal("missing receipt was not exact zero/false", err)
	}
	interopCommit(t, tx)
	interopClose(t, c)
	flQAUnchanged(t, f, m, before)
}

func TestSQLiteLinkNativeCloseAndCleanupFailureRemainUnknown(t *testing.T) {
	for _, phase := range []string{"close-before", "durable-native-closed", "durable-release-before"} {
		t.Run(phase, func(t *testing.T) {
			flQACalibrate(t)
			s, in, f := flQAService(t)
			fired := flQAClosedFault(t, phase, "", syscall.ENOSPC)
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			if !fired() {
				t.Fatal("reached durable closure fault absent")
			}
			flQAResultError(t, r, err, "local_write_unknown")
			m, rows := flQASnapshot(t, f)
			if m.Revision != "1" || len(rows["requests"]) != 1 {
				t.Fatal("closure uncertainty discarded committed receipt")
			}
			r, err = s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
			if err != nil || r.RequestID != in.RequestID {
				t.Fatal("exact replay did not discharge new fence", err)
			}
		})
	}
	t.Run("cleanup-only ENOSPC cannot prove capacity/nonapplication", func(t *testing.T) {
		flQACalibrate(t)
		s, in, f := flQAService(t)
		pre, cleanup := false, false
		spQAHooks(t, spQASQLHooks{Fault: func(e spQASQLEvent) error {
			if e.Phase == "commit-before-dispatch" && !pre {
				pre = true
				return syscall.EIO
			}
			if e.Phase == "rollback-after" && !cleanup {
				cleanup = true
				return syscall.ENOSPC
			}
			return nil
		}})
		r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
		spQASetSQLHooks(spQASQLHooks{})
		if !pre || !cleanup {
			t.Fatal("cleanup error checkpoints not both reached")
		}
		flQAResultError(t, r, err, "local_write_unknown")
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
			t.Fatal("actual rolled back cold image unexpectedly retained DDL")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

// This public native prerequisite has two independent real child owners: the
// guard consumes the first part of the absolute budget, the writer consumes its
// remainder. It is not evidence that the activity coordinator already exists.
func TestSQLiteLinkSharedAbsoluteNativeGuardThenWriterDeadline(t *testing.T) {
	_, _, f, _ := flQABootstrap(t)
	writerRelease := flQAOwnedHelper(t, f, "writer")
	guardRelease := flQAOwnedHelper(t, f, "guard")
	start := time.Now()
	deadline := start.Add(400 * time.Millisecond)
	type opened struct {
		c   *sqliteio.Conn
		err error
	}
	done := make(chan opened, 1)
	openCtx, cancelOpen := context.WithCancel(context.Background())
	joinedOpen := false
	var got opened
	defer func() {
		cancelOpen()
		guardRelease()
		writerRelease()
		if !joinedOpen {
			got = <-done
			joinedOpen = true
			flQARetainConn(t, got.c)
		}
	}()
	go func() {
		var r opened
		defer func() { done <- r }()
		r.c, r.err = sqliteio.OpenForInitialLink(openCtx, f.directory, f.authority, f.database, deadline)
	}()
	timer := time.NewTimer(150 * time.Millisecond)
	<-timer.C
	guardRelease()
	got = <-done
	joinedOpen = true
	flQARetainConn(t, got.c)
	if got.err != nil || got.c == nil {
		writerRelease()
		if got.c != nil {
			interopClose(t, got.c)
		}
		t.Fatal("initial guard did not release into actual WAL follower", got.err)
	}
	ctx := context.Background()
	tx, err := got.c.Begin(ctx, sqliteio.Write)
	flQARetainTx(t, tx)
	writerRelease()
	if tx != nil {
		interopRollback(t, tx)
	}
	interopClose(t, got.c)
	if err == nil || ctx.Err() != nil || time.Now().Before(deadline) {
		t.Fatal("actual native writer did not consume original deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("guard then Begin appears to reset original400ms budget: %v", elapsed)
	}
	var native *sqliteio.Error
	if !errors.As(err, &native) || (native.Category != sqliteio.Busy && native.Category != sqliteio.Canceled) {
		t.Fatal("unexpected finite Begin refusal", err)
	}
}

func TestSQLiteLinkProviderInducedFilesystemDriftAndInitialGuardOwnership(t *testing.T) {
	for _, kind := range []string{"symlink drift", "Git boundary drift"} {
		t.Run(kind, func(t *testing.T) {
			s, in, _ := flQAService(t)
			p := qaNewLinkProvider(t)
			p.beforeList = func() {
				if kind == "symlink drift" {
					moved := filepath.Join(t.TempDir(), "moved")
					if err := os.Rename(in.Path, moved); err != nil {
						t.Fatal(err)
					}
					target := t.TempDir()
					if err := os.Symlink(target, in.Path); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.WriteFile(filepath.Join(in.Path, ".git"), []byte("gitdir: missing-secret-owned-git\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, p))
			flQAResultError(t, r, err, "binding_unavailable")
			flQAStateAbsent(t, s)
		})
	}
	t.Run("first provider can acquire actual initialization guard", func(t *testing.T) {
		s, in, f := flQAService(t)
		if err := os.MkdirAll(f.directory, 0700); err != nil {
			t.Fatal(err)
		}
		p := qaNewLinkProvider(t)
		called := false
		p.beforeList = func() { release := flQAOwnedHelper(t, f, "guard"); called = true; release() }
		r := flQASuccess(t, s, in, p)
		if !called || !r.Changed {
			t.Fatal("first external preparation did not run before initialization guard")
		}
	})
}

func TestSQLiteLinkCanonicalCatalogComparesActualDDLNotJustNames(t *testing.T) {
	s, in, f, _ := flQABootstrap(t)
	flQAWrite(t, f, func(tx *sqliteio.Tx, m sqliteStoreMeta) {
		interopDone(t, tx, "DROP INDEX binding_timer")
		interopDone(t, tx, "CREATE INDEX binding_timer ON bindings(computer_id,account_id,project_id,active,task_id,binding_id)")
	})
	m, rows := flQASnapshot(t, f)
	if len(rows["catalog"]) != 85 {
		t.Fatal("same-count altered covering DDL control did not retain85objects")
	}
	in.RequestID = flQASecondRequest
	r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
	flQAResultError(t, r, err, "state_corrupt")
	flQAUnchanged(t, f, m, rows)
}

func TestSQLiteLinkPublicInspectionSymlinkWalkIsNotProvenAbsence(t *testing.T) {
	root := t.TempDir()
	target := t.TempDir()
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(alias, "missing", "child")
	_, a, b, err := sqliteLocation(filepath.Join(directory, "activity.json"))
	if err != nil {
		t.Fatal(err)
	}
	before := flQAImage(t, root)
	targetBefore := flQAImage(t, target)
	c, kind, err := sqliteio.InspectForLink(context.Background(), directory, a, b, time.Now().Add(250*time.Millisecond))
	flQARetainConn(t, c)
	if c != nil {
		interopClose(t, c)
	}
	if err == nil || kind != 0 {
		t.Fatal("symlinked absent ancestor was accepted as qualified absence")
	}
	if !reflect.DeepEqual(before, flQAImage(t, root)) || !reflect.DeepEqual(targetBefore, flQAImage(t, target)) {
		t.Fatal("unsafe ancestor traversal created a role")
	}
}

// Sparse lengths exercise the actual pinned Footprint pressure policy. These
// are neither SQLite FULL fixtures nor evidence of filesystem exhaustion.
func TestSQLiteLinkObservedPinnedFilePressureBoundaries(t *testing.T) {
	for _, profile := range []string{"WAL below64MiB", "WAL exactly64MiB", "aggregate exactly320MiB"} {
		t.Run(profile, func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			m, rows := flQASnapshot(t, f)
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			before, err := tx.Footprint()
			if err != nil {
				t.Fatal(err)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			wantWAL := int64(64 * 1024 * 1024)
			wantJournal := int64(0)
			if profile == "WAL below64MiB" {
				wantWAL--
			}
			if profile == "aggregate exactly320MiB" {
				if before.Main <= 0 || before.Journal != 0 {
					t.Fatalf("cold nonempty WAL store prerequisite %+v", before)
				}
				wantJournal = int64(320*1024*1024) - before.Main - before.WAL - before.SHM
				if wantJournal <= 0 {
					t.Fatal("aggregate control does not reach exact native threshold")
				}
				journalPath := filepath.Join(f.directory, f.database+"-journal")
				file, err := os.OpenFile(journalPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				if err != nil {
					t.Fatal("exclusive cold journal", err)
				}
				n, writeErr := file.Write([]byte{0})
				var lengthErr error
				if writeErr == nil && n == 1 {
					lengthErr = file.Truncate(wantJournal)
				}
				closeErr := file.Close() // No fixture descriptor remains across a SQLite owner.
				if n != 1 || writeErr != nil || lengthErr != nil || closeErr != nil {
					t.Fatal("cold sparse journal creation", errors.Join(writeErr, lengthErr, closeErr))
				}
			} else {
				if err := os.Truncate(filepath.Join(f.directory, f.database+"-wal"), wantWAL); err != nil {
					t.Fatal(err)
				}
			}
			c, tx = interopOpen(t, f, false, sqliteio.Read)
			observed, err := tx.Footprint()
			if err != nil {
				t.Fatal(err)
			}
			if profile == "aggregate exactly320MiB" {
				if observed.Total != 320*1024*1024 || observed.Journal != wantJournal || observed.WAL >= 64*1024*1024 {
					t.Fatalf("aggregate native witness %+v", observed)
				}
			} else if observed.WAL != wantWAL {
				t.Fatalf("WAL native witness %+v", observed)
			}
			interopRollback(t, tx)
			interopClose(t, c)
			if profile != "aggregate exactly320MiB" {
				flQAUnchanged(t, f, m, rows)
			}
			in.RequestID = flQASecondRequest
			boundaryReached, statFailed := false, false
			var boundary [4]int64
			if profile == "aggregate exactly320MiB" {
				t.Cleanup(func() { ncQASetFSHooks(ncQAFSHooks{}) })
				ncQASetFSHooks(ncQAFSHooks{Observe: func(ev ncQAFSEvent) {
					if boundaryReached || ev.Op != "footprint" || ev.Role != "journal" || ev.Phase != "after" {
						return
					}
					boundaryReached = true
					for i, suffix := range []string{"", "-wal", "-shm", "-journal"} {
						info, err := os.Stat(filepath.Join(f.directory, f.database+suffix))
						if errors.Is(err, os.ErrNotExist) {
							continue
						}
						if err != nil {
							statFailed = true
							return
						}
						boundary[i] = info.Size()
					}
				}})
			}
			r, err := s.linkSQLite(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t)))
			if profile == "aggregate exactly320MiB" {
				ncQASetFSHooks(ncQAFSHooks{})
				if !boundaryReached || statFailed || boundary[0]+boundary[1]+boundary[2]+boundary[3] != 320*1024*1024 || boundary[3] != wantJournal || boundary[1] >= 64*1024*1024 {
					t.Fatalf("real Link Footprint aggregate boundary reached=%t statFailed=%t sizes=%v journal=%d", boundaryReached, statFailed, boundary, wantJournal)
				}
			}
			if profile == "WAL below64MiB" {
				if err != nil || r.Changed || r.SnapshotRevision != "2" {
					t.Fatal("below physical threshold refused new no-op", err)
				}
			} else {
				flQAResultError(t, r, err, "validation")
				flQAPressure(t, err, "maintenance_required")
				flQAUnchanged(t, f, m, rows)
				in.RequestID = qaLinkRequest
				r, err = s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
				flQAResultError(t, r, err, "local_write_unknown")
				flQAUnchanged(t, f, m, rows)
			}
		})
	}
}

func TestSQLiteLinkRequiredAccountTimezoneAuthAndRevisionPrecheck(t *testing.T) {
	for _, kind := range []string{"account required", "timezone required", "provider absent"} {
		t.Run(kind, func(t *testing.T) {
			s, in, _ := flQAService(t)
			deps := qaLinkDeps(t, qaNewLinkProvider(t))
			switch kind {
			case "account required":
				in.AccountID = ""
				deps.ResolveAccount = func(context.Context) (string, error) { return "", nil }
			case "timezone required":
				in.Timezone = ""
			case "provider absent":
				deps.NewProvider = nil
			}
			r, err := s.linkSQLite(context.Background(), in, deps)
			code := "input_required"
			if kind == "provider absent" {
				code = "auth"
			}
			flQAResultError(t, r, err, code)
			flQAStateAbsent(t, s)
		})
	}
	for _, revision := range []string{"0", "2"} {
		t.Run("saved precheck "+revision, func(t *testing.T) {
			s, in, f, _ := flQABootstrap(t)
			m, rows := flQASnapshot(t, f)
			in.RequestID = flQASecondRequest
			in.IfRevision = revision
			r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
			flQAResultError(t, r, err, "revision_conflict")
			flQAUnchanged(t, f, m, rows)
		})
	}
	t.Run("revision supplied against absent exact location", func(t *testing.T) {
		s, in, f, _ := flQABootstrap(t)
		m, rows := flQASnapshot(t, f)
		in.RequestID = flQASecondRequest
		in.IfRevision = "1"
		in.Path = t.TempDir()
		r, err := s.linkSQLite(context.Background(), in, flQAForbiddenDeps(t))
		flQAResultError(t, r, err, "revision_conflict")
		flQAUnchanged(t, f, m, rows)
	})
}
