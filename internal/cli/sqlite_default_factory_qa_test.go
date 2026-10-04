//go:build (darwin || linux) && (amd64 || arm64)

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/worker"
)

// Every command constructs its own default service. No Activity, Worker, or
// Hooks dependency is injected; all OS-derived paths point into this fixture.
type dfQAFixture struct {
	t                                  *testing.T
	root, state, cwd, hook             string
	d                                  cli.Dependencies
	store                              *fakeStore
	api                                *qaCLILinkAPI
	credentials, providers, nativeAuth int
	local                              bool
}

func dfQANew(t *testing.T) *dfQAFixture {
	t.Helper()
	owned, err := os.MkdirTemp("/tmp", "tempo-default-sqlite-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(owned); err != nil {
			t.Error("remove owned default-factory fixture", err)
		}
	})
	root, err := filepath.EvalSymlinks(owned)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", filepath.Join(root, "home"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "xdg"))
	q := &dfQAFixture{t: t, root: root, state: filepath.Join(root, "activity", "state.json"), cwd: filepath.Join(root, "project"), hook: filepath.Join(root, "hook", "policy.json"), store: &fakeStore{}, api: &qaCLILinkAPI{}}
	getenv := func(key string) string {
		switch key {
		case "TEMPO_STATE":
			return q.state
		case "TEMPO_CONFIG":
			return filepath.Join(root, "config", "config.json")
		case "TEMPO_HOOK_STATE":
			return q.hook
		case "HARVEST_ACCOUNT_ID":
			return "11"
		case "HARVEST_TOKEN":
			q.credentials++
			if q.local {
				t.Error("local default factory read credentials")
			}
			return "synthetic-default-token"
		default:
			return ""
		}
	}
	provider := func(token, account string) harvest.Provider {
		q.providers++
		if q.local || token != "synthetic-default-token" || account != "11" && account != "" {
			t.Error("unexpected default-factory provider scope")
		}
		return q.api
	}
	config := filepath.Join(root, "config", "config.json")
	q.d = cli.Dependencies{Store: q.store, ConfigPath: config, Getenv: getenv, NewProvider: provider, TerminalEligible: func(io.Reader, io.Writer) bool { return false }, Now: func() time.Time { return time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC) }}
	q.d.Auth = auth.NewService(auth.Options{ConfigPath: config, LockPath: filepath.Join(root, "auth", "lock"), Getenv: getenv, NewProvider: provider, PersistentAvailable: func() bool { return false }, Runner: auth.RunnerFunc(func(context.Context, auth.NativeRequest, *os.File) (auth.NativeReply, error) {
		q.nativeAuth++
		return auth.NativeReply{}, errors.New("native credential runner forbidden")
	})})
	return q
}

func (q *dfQAFixture) json(args []string, input []byte, target any) {
	q.t.Helper()
	var out, stderr bytes.Buffer
	argv := append(append([]string{}, args...), "--json", "--non-interactive")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	code := cli.Run(ctx, argv, bytes.NewReader(input), &out, &stderr, q.d)
	if code != 0 || stderr.Len() != 0 {
		q.t.Fatalf("default CLI %v exit=%d stderr=%q", args, code, stderr.String())
	}
	var envelope struct {
		SchemaVersion int             `json:"schema_version"`
		Data          json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(out.Bytes(), &envelope); err != nil || envelope.SchemaVersion != 1 || len(envelope.Data) == 0 {
		q.t.Fatal("default CLI lost finite envelope", err)
	}
	if err := json.Unmarshal(envelope.Data, target); err != nil {
		q.t.Fatal("default CLI lost typed data", err)
	}
}

func (q *dfQAFixture) noJSONAuthority() {
	q.t.Helper()
	if _, err := os.Lstat(q.state); !errors.Is(err, os.ErrNotExist) {
		q.t.Fatal("default factory created/adopted JSON authority", err)
	}
}

func (q *dfQAFixture) oneSQLiteDatabase() {
	q.t.Helper()
	entries, err := os.ReadDir(filepath.Dir(q.state))
	if err != nil {
		q.t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "activity-") && strings.HasSuffix(entry.Name(), ".sqlite3") && !entry.IsDir() {
			count++
		}
	}
	if count != 1 {
		q.t.Fatal("default factory did not retain exactly one SQLite database", count)
	}
	q.noJSONAuthority()
}

func TestSQLiteDefaultFactoryAbsentReadsStayNoncreating(t *testing.T) {
	q := dfQANew(t)
	q.local = true
	var bindings activity.BindingList
	q.json([]string{"links", "list"}, nil, &bindings)
	if len(bindings.Bindings) != 0 || bindings.SnapshotRevision != "0" {
		t.Fatal("absent default bindings changed", bindings)
	}
	var doctor setup.Diagnostics
	q.json([]string{"doctor"}, nil, &doctor)
	foundMissing := false
	for _, item := range doctor.Items {
		foundMissing = foundMissing || item.Code == "bindings_missing"
	}
	if !foundMissing {
		t.Fatal("absent Doctor did not see missing bindings")
	}
	var status worker.Result
	q.json([]string{"worker", "status"}, nil, &status)
	if status.Status.QueuedCount != 0 || status.Status.SyncEnabled {
		t.Fatal("absent default worker reported stored work", status)
	}
	for _, path := range []string{filepath.Dir(q.state), filepath.Dir(q.hook), filepath.Join(q.root, "config"), filepath.Join(q.root, "xdg")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("absent local read created private storage", path, err)
		}
	}
	if q.credentials != 0 || q.providers != 0 || q.nativeAuth != 0 || q.store.gets+q.store.sets+q.store.deletes != 0 {
		t.Fatal("absent local read reached provider or credentials")
	}
}

func TestSQLiteDefaultFactoriesShareFreshLinkHookAndWorkerState(t *testing.T) {
	q := dfQANew(t)
	if err := os.Mkdir(q.cwd, 0700); err != nil {
		t.Fatal(err)
	}
	var linked activity.BindingResult
	q.json([]string{"link", "100", "--account", "11", "--task", "200", "--timezone", "UTC", "--path", q.cwd, "--request-id", "df000000-0000-4000-8000-000000000001"}, nil, &linked)
	if !linked.Changed || linked.Binding.ID == "" || linked.Binding.Attribution != (activity.Attribution{AccountID: "11", UserID: "7", ProjectID: "100", TaskID: "200", Timezone: "UTC"}) {
		t.Fatal("fresh default Link did not persist current-user attribution", linked)
	}
	q.oneSQLiteDatabase()
	credentials, providers := q.credentials, q.providers
	q.local = true
	var bindings activity.BindingList
	q.json([]string{"links", "list"}, nil, &bindings)
	if len(bindings.Bindings) != 1 || bindings.Bindings[0].ID != linked.Binding.ID || bindings.Bindings[0].Revision != linked.Binding.Revision || bindings.Bindings[0].Kind != linked.Binding.Kind || bindings.Bindings[0].Locator != linked.Binding.Locator || bindings.Bindings[0].Attribution != linked.Binding.Attribution || len(bindings.Bindings[0].AttachedActors) != 0 {
		t.Fatal("cold default binding read lost Link", bindings)
	}
	var doctor setup.Diagnostics
	q.json([]string{"doctor"}, nil, &doctor)
	foundPresent := false
	for _, item := range doctor.Items {
		foundPresent = foundPresent || item.Code == "bindings_present"
	}
	if !foundPresent {
		t.Fatal("cold default Doctor did not see SQL binding")
	}
	if q.credentials != credentials || q.providers != providers {
		t.Fatal("cold binding/Doctor read accessed provider or credentials")
	}
	q.local = false
	policies := hookstate.New(hookstate.Options{Path: q.hook})
	profileContext := hookstate.Context{Host: "codex", Scope: "project", Path: q.cwd, RuntimeVersion: "0.159.3", Surface: "local", Conflicts: []string{}}
	for _, role := range []string{"runtime", "executable", "definitions"} {
		path := filepath.Join(q.cwd, role)
		if err := os.WriteFile(path, []byte("synthetic default "+role), 0600); err != nil {
			t.Fatal(err)
		}
		profileContext.Artifacts = append(profileContext.Artifacts, hookstate.Artifact{Role: role, Path: path})
	}
	preview, err := policies.Preview(context.Background(), profileContext)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := policies.Confirm(context.Background(), hookstate.ConfirmInput{Context: preview.Context, Fingerprint: preview.Fingerprint, DeclarationVersion: hookstate.DeclarationVersion, RequestID: "df000000-0000-4000-8000-000000000002", Confirmed: true})
	if err != nil || !profile.CaptureEligible || profile.Basis != "operator_declared" {
		t.Fatal("synthetic default hook profile not eligible", err)
	}
	q.local = true
	raw, err := json.Marshal(map[string]string{"hook_event_name": "SessionStart", "session_id": "default-factory-session", "cwd": q.cwd, "source": "startup"})
	if err != nil {
		t.Fatal(err)
	}
	var hostOut, hostErr bytes.Buffer
	code := cli.Run(context.Background(), []string{"hook", "codex", "--input-stdin"}, bytes.NewReader(raw), &hostOut, &hostErr, q.d)
	if code != 0 || hostOut.String() != "{}\n" || hostErr.Len() != 0 {
		t.Fatal("uninjected default hook did not commit", code, hostOut.String(), hostErr.String())
	}
	q.noJSONAuthority()
	q.local = true
	list, err := activity.New(activity.Options{Path: q.state}).HostReceipts(context.Background(), activity.HostReceiptFilter{Source: "codex", SessionID: "default-factory-session"})
	if err != nil || len(list.Receipts) != 1 || list.Receipts[0].ID == "" || list.Receipts[0].Kind != "SessionStart" || list.Receipts[0].Disposition != "applied" || list.Receipts[0].Durability != "committed" || list.Receipts[0].ProfileBasis != "operator_declared" {
		t.Fatal("cold default hook receipt not durable", err, list)
	}
	var snapshot activity.ActivitySnapshot
	q.json([]string{"activity", "status"}, nil, &snapshot)
	if snapshot.ComputerID == nil {
		t.Fatal("default hook lost linked computer identity")
	}
	actor := activity.ActorKey{ComputerID: *snapshot.ComputerID, Source: "manual-test", SessionID: "default-factory-normalized", AgentID: "root"}
	work := activity.Event{ContractVersion: 1, Actor: actor, Generation: "1", Sequence: "1", EventID: "default/work/1", Kind: "work", BindingID: linked.Binding.ID, BindingRevision: linked.Binding.Revision, CWD: q.cwd}
	for _, event := range []activity.Event{work, {ContractVersion: 1, Actor: actor, Generation: "1", Sequence: "2", EventID: "default/finish/2", Kind: "finish"}} {
		encoded, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		var receipt activity.EventResult
		q.json([]string{"activity", "event", "--input-stdin"}, encoded, &receipt)
		if receipt.Disposition != "applied" {
			t.Fatal("default normalized capture not applied", receipt)
		}
	}
	var configured activity.SyncConfigurationResult
	q.local = false
	q.json([]string{"sync", "configure", "--account", "11", "--user", "7", "--mode", "duration", "--duration-policy", "exact", "--if-revision", "0", "--yes", "--request-id", "df000000-0000-4000-8000-000000000003"}, nil, &configured)
	if !configured.Changed || configured.Configuration.AccountID != "11" || configured.Configuration.UserID != "7" {
		t.Fatal("default consent was not saved", configured)
	}
	q.local = true
	var resumed activity.MutationResult
	q.json([]string{"sync", "resume", "--request-id", "df000000-0000-4000-8000-000000000004"}, nil, &resumed)
	if !resumed.Changed {
		t.Fatal("default worker consent not enabled")
	}
	q.local = true
	var queued activity.SyncStatus
	q.json([]string{"sync", "status"}, nil, &queued)
	var workerStatus worker.Result
	q.json([]string{"worker", "status"}, nil, &workerStatus)
	if !queued.Enabled || len(queued.Configurations) != 1 || len(queued.Items) != 1 || queued.Items[0].State != "queued" || queued.Worker.QueuedCount != 1 || !workerStatus.Status.SyncEnabled || workerStatus.Status.QueuedCount != 1 {
		t.Fatal("uninjected worker factory lost shared consent/outbox", queued, workerStatus)
	}
	q.oneSQLiteDatabase()
	if q.nativeAuth != 0 || q.store.gets+q.store.sets+q.store.deletes != 0 {
		t.Fatal("default workflow touched native credentials")
	}
}
