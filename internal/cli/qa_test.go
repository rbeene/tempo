package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

type fakeStore struct{ gets, sets, deletes int }

func (s *fakeStore) Get() (string, error) { s.gets++; return "synthetic-secret", nil }
func (s *fakeStore) Set(string) error     { s.sets++; return nil }
func (s *fakeStore) Delete() error        { s.deletes++; return nil }

type request struct {
	method, path string
	query        url.Values
	body         harvest.Object
}
type fakeAPI struct {
	calls       []request
	running     []harvest.Object
	entry       harvest.Object
	timestamps  bool
	listErr     error
	writeErr    error
	accounts    []harvest.Object
	accountsErr error
}

func (a *fakeAPI) Accounts(context.Context) ([]harvest.Object, error) {
	a.calls = append(a.calls, request{method: "accounts"})
	if a.accountsErr != nil || a.accounts != nil {
		return a.accounts, a.accountsErr
	}
	return []harvest.Object{{"id": json.Number("11"), "product": "harvest"}}, nil
}
func (a *fakeAPI) Get(_ context.Context, p string) (harvest.Object, error) {
	a.calls = append(a.calls, request{method: "GET", path: p})
	switch p {
	case "/users/me":
		return harvest.Object{"id": json.Number("7")}, nil
	case "/company":
		return harvest.Object{"wants_timestamp_timers": a.timestamps}, nil
	}
	if a.entry != nil {
		return a.entry, nil
	}
	return ownedEntry("91", false), nil
}
func (a *fakeAPI) List(_ context.Context, p string, q url.Values) ([]harvest.Object, error) {
	a.calls = append(a.calls, request{method: "LIST", path: p, query: q})
	return a.running, a.listErr
}
func (a *fakeAPI) Create(_ context.Context, p string, b harvest.Object) (harvest.Object, error) {
	a.calls = append(a.calls, request{method: "POST", path: p, body: b})
	return ownedEntry("91", true), a.writeErr
}
func (a *fakeAPI) Update(_ context.Context, p string, b harvest.Object) (harvest.Object, error) {
	a.calls = append(a.calls, request{method: "PATCH", path: p, body: b})
	return ownedEntry("91", false), a.writeErr
}
func (a *fakeAPI) Delete(_ context.Context, p string) error {
	a.calls = append(a.calls, request{method: "DELETE", path: p})
	return a.writeErr
}
func ownedEntry(id string, running bool) harvest.Object {
	return harvest.Object{"id": json.Number(id), "user": harvest.Object{"id": json.Number("7")}, "is_running": running}
}
func writes(a *fakeAPI) []request {
	var r []request
	for _, c := range a.calls {
		if c.method == "POST" || c.method == "PATCH" || c.method == "DELETE" {
			r = append(r, c)
		}
	}
	return r
}

type result struct {
	code      int
	out, err  string
	store     *fakeStore
	api       *fakeAPI
	factories int
}

func run(t *testing.T, a *fakeAPI, args ...string) result {
	t.Helper()
	s := &fakeStore{}
	var out, stderr bytes.Buffer
	n := 0
	d := cli.Dependencies{Store: s, ConfigPath: filepath.Join(t.TempDir(), "config.json"), Getenv: func(k string) string {
		if k == "HARVEST_ACCOUNT_ID" {
			return "11"
		}
		return ""
	}, Now: func() time.Time { return time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC) }, NewProvider: func(token, account string) harvest.Provider {
		n++
		if token != "synthetic-secret" || account != "11" {
			t.Errorf("unexpected synthetic credentials or account")
		}
		return a
	}}
	code := qaLegacyRun(t, context.Background(), args, strings.NewReader(""), &out, &stderr, d)
	return result{code, out.String(), stderr.String(), s, a, n}
}
func envelope(t *testing.T, r result, want int, code string) {
	t.Helper()
	if r.code != want {
		t.Fatalf("exit=%d want %d; out=%s err=%s", r.code, want, r.out, r.err)
	}
	var v map[string]any
	raw := r.out
	if want != 0 {
		raw = r.err
		if r.out != "" {
			t.Errorf("failure polluted stdout: %s", r.out)
		}
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("invalid JSON envelope: %q: %v", raw, err)
	}
	if v["schema_version"] != float64(1) {
		t.Errorf("schema_version=%v", v["schema_version"])
	}
	if want == 0 {
		if _, ok := v["data"]; !ok {
			t.Error("missing data")
		}
	} else {
		e, ok := v["error"].(map[string]any)
		if !ok {
			t.Fatal("missing error object")
		}
		if e["code"] != code {
			t.Errorf("error code=%v want %s", e["code"], code)
		}
		for _, k := range []string{"retryable", "uncertain"} {
			if _, ok := e[k].(bool); !ok {
				t.Errorf("missing boolean %s", k)
			}
		}
	}
	if strings.Contains(r.out+r.err, "synthetic-secret") {
		t.Error("credential leaked")
	}
}

func TestQAOfflineCommandsNeverReadCredentials(t *testing.T) {
	for _, cmd := range []string{"help", "schema", "version", "config show"} {
		t.Run(cmd, func(t *testing.T) {
			r := run(t, &fakeAPI{}, append(strings.Fields(cmd), "--json")...)
			envelope(t, r, 0, "")
			if r.store.gets+r.store.sets+r.store.deletes+r.factories != 0 {
				t.Fatal("offline command touched credentials or network")
			}
		})
	}
}
func TestQAInvalidInputsHaveNoSideEffects(t *testing.T) {
	cases := [][]string{{"time", "create", "--project", "1", "--task", "2", "--date", "2026-02-30", "--hours", "1"}, {"time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01"}, {"time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "NaN"}, {"time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "-1"}, {"time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "25"}, {"time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "1", "--duration", "1h"}, {"time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--start", "23:00", "--end", "01:00"}, {"time", "show", "0"}, {"time", "update", "91"}, {"time", "list", "--running", "maybe"}, {"time", "list", "--from", "2026-10-02", "--to", "2026-10-01"}, {"auth", "login", "--token", "synthetic-secret"}}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			r := run(t, &fakeAPI{}, append(args, "--json")...)
			if r.code != 2 {
				t.Fatalf("exit %d, want validation exit 2: %s", r.code, r.err)
			}
			if r.store.gets+r.store.sets+r.store.deletes+r.factories != 0 {
				t.Fatalf("invalid command touched store/provider")
			}
		})
	}
}
func TestQADeleteRequiresExplicitYes(t *testing.T) {
	r := run(t, &fakeAPI{}, "time", "delete", "91", "--json", "--non-interactive")
	envelope(t, r, 6, "confirmation_required")
	if len(writes(r.api)) != 0 {
		t.Fatal("unconfirmed deletion")
	}
}
func TestQAOwnUserScope(t *testing.T) {
	for _, cmd := range [][]string{{"time", "show", "91"}, {"time", "update", "91", "--notes", "edited"}, {"time", "delete", "91", "--yes"}, {"timer", "start", "91"}, {"timer", "stop", "91"}} {
		t.Run(strings.Join(cmd, " "), func(t *testing.T) {
			a := &fakeAPI{entry: harvest.Object{"id": json.Number("91"), "user": harvest.Object{"id": json.Number("8")}, "is_running": true}}
			r := run(t, a, append(cmd, "--json")...)
			envelope(t, r, 4, "forbidden")
			if len(writes(a)) != 0 {
				t.Fatal("mutated another user's entry")
			}
		})
	}
}
func TestQAListScopesCurrentUser(t *testing.T) {
	r := run(t, &fakeAPI{}, "time", "list", "--from", "2026-09-01", "--to", "2026-10-01", "--json")
	envelope(t, r, 0, "")
	for _, c := range r.api.calls {
		if c.method == "LIST" && c.path == "/time_entries" {
			if c.query.Get("user_id") != "7" || c.query.Get("from") != "2026-09-01" || c.query.Get("to") != "2026-10-01" {
				t.Fatalf("wrong scope/filters: %v", c.query)
			}
			return
		}
	}
	t.Fatal("missing list")
}
func TestQACreatePreservesZeroAndNotes(t *testing.T) {
	r := run(t, &fakeAPI{}, "time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "0", "--notes", "line one\n日", "--json")
	envelope(t, r, 0, "")
	w := writes(r.api)
	if len(w) != 1 || w[0].path != "/time_entries" {
		t.Fatalf("writes=%v", w)
	}
	b, _ := json.Marshal(w[0].body)
	var v map[string]any
	_ = json.Unmarshal(b, &v)
	if v["hours"] != float64(0) || v["notes"] != "line one\n日" {
		t.Fatalf("wrong body: %s", b)
	}
}
func TestQAUpdateCanClearNotesWithoutDuration(t *testing.T) {
	r := run(t, &fakeAPI{}, "time", "update", "91", "--notes", "", "--json")
	envelope(t, r, 0, "")
	w := writes(r.api)
	if len(w) != 1 {
		t.Fatalf("writes=%v", w)
	}
	if v, ok := w[0].body["notes"]; !ok || v != "" {
		t.Fatal("notes not cleared")
	}
	if _, ok := w[0].body["hours"]; ok {
		t.Fatal("notes edit injected hours")
	}
}
func TestQATimerStartTransitions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		running []harvest.Object
		want    int
		errCode string
		count   int
	}{{"stopped", nil, 0, "", 1}, {"same", []harvest.Object{ownedEntry("91", true)}, 0, "", 0}, {"different", []harvest.Object{ownedEntry("92", true)}, 6, "conflict", 0}, {"ambiguous", []harvest.Object{ownedEntry("91", true), ownedEntry("92", true)}, 6, "conflict", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			a := &fakeAPI{running: tc.running}
			r := run(t, a, "timer", "start", "91", "--json")
			envelope(t, r, tc.want, tc.errCode)
			if len(writes(a)) != tc.count {
				t.Fatalf("writes=%v", writes(a))
			}
			for _, c := range a.calls {
				if c.method == "LIST" {
					if c.query.Get("user_id") != "7" || c.query.Get("is_running") != "true" || c.query.Get("from") != "" {
						t.Fatalf("timer list incorrectly scoped: %v", c.query)
					}
				}
			}
		})
	}
}
func TestQATimerPaginationFailurePreventsMutation(t *testing.T) {
	a := &fakeAPI{listErr: &harvest.Error{Code: "response", Message: "incomplete pagination"}}
	r := run(t, a, "timer", "start", "91", "--json")
	envelope(t, r, 7, "response")
	if len(writes(a)) != 0 {
		t.Fatal("write after incomplete timer lookup")
	}
}
func TestQATimestampCompanyRejectsDuration(t *testing.T) {
	a := &fakeAPI{timestamps: true}
	r := run(t, a, "time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "1", "--json")
	envelope(t, r, 2, "validation")
	if len(writes(a)) != 0 {
		t.Fatal("write with incompatible mode")
	}
}
func TestQAUncertainWriteEnvelope(t *testing.T) {
	a := &fakeAPI{writeErr: &harvest.Error{Code: "uncertain_write", Message: "write outcome unknown", Uncertain: true}}
	r := run(t, a, "time", "delete", "91", "--yes", "--json")
	envelope(t, r, 8, "uncertain_write")
	if len(writes(a)) != 1 {
		t.Fatal("uncertain write repeated")
	}
	var v struct {
		Error struct {
			Uncertain bool `json:"uncertain"`
		} `json:"error"`
	}
	_ = json.Unmarshal([]byte(r.err), &v)
	if !v.Error.Uncertain {
		t.Fatal("uncertainty lost")
	}
}

var _ auth.Store = (*fakeStore)(nil)

func TestQATimerStopUsesRunningEntryAcrossDates(t *testing.T) {
	a := &fakeAPI{running: []harvest.Object{ownedEntry("91", true)}, entry: ownedEntry("91", true)}
	r := run(t, a, "timer", "stop", "--json")
	envelope(t, r, 0, "")
	w := writes(a)
	if len(w) != 1 || w[0].path != "/time_entries/91/stop" {
		t.Fatalf("wrong stop requests: %v", w)
	}
	for _, c := range a.calls {
		if c.method == "LIST" && (c.query.Get("user_id") != "7" || c.query.Get("is_running") != "true" || c.query.Get("from") != "" || c.query.Get("to") != "") {
			t.Fatalf("timer lookup excluded dates or users: %v", c.query)
		}
	}
}
func TestQATimerStopAmbiguousDoesNotWrite(t *testing.T) {
	a := &fakeAPI{running: []harvest.Object{ownedEntry("91", true), ownedEntry("92", true)}}
	r := run(t, a, "timer", "stop", "--json")
	envelope(t, r, 6, "conflict")
	if len(writes(a)) != 0 {
		t.Fatal("ambiguous stop mutated an entry")
	}
}
func TestQATimestampPairPreserved(t *testing.T) {
	a := &fakeAPI{timestamps: true}
	r := run(t, a, "time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--start", "09:15", "--end", "10:45", "--json")
	envelope(t, r, 0, "")
	w := writes(a)
	if len(w) != 1 {
		t.Fatalf("writes=%v", w)
	}
	if w[0].body["started_time"] != "9:15am" || w[0].body["ended_time"] != "10:45am" {
		t.Fatalf("timestamps changed: %v", w[0].body)
	}
	if _, ok := w[0].body["hours"]; ok {
		t.Fatal("timestamp request also sent hours")
	}
}

func TestQAClockMidnightAndNoon(t *testing.T) {
	a := &fakeAPI{timestamps: true}
	r := run(t, a, "time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--start", "00:00", "--end", "12:00", "--json")
	envelope(t, r, 0, "")
	w := writes(a)
	if len(w) != 1 || w[0].body["started_time"] != "12:00am" || w[0].body["ended_time"] != "12:00pm" {
		t.Fatalf("midnight/noon conversion: %v", w)
	}
}

func TestQAAuthLoginValidatesBeforeStorage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		api     *fakeAPI
		account string
		exit    int
		code    string
		sets    int
	}{{"success", &fakeAPI{}, "11", 0, "", 1}, {"inaccessible account", &fakeAPI{}, "22", 4, "forbidden", 0}, {"API rejects", &fakeAPI{accountsErr: &harvest.Error{Code: "auth", Message: "synthetic-secret"}}, "11", 3, "auth", 0}, {"ambiguous", &fakeAPI{accounts: []harvest.Object{{"id": json.Number("11"), "product": "harvest"}, {"id": json.Number("22"), "product": "harvest"}}}, "", 6, "conflict", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &fakeStore{}
			path := filepath.Join(t.TempDir(), "config.json")
			var out, errOut bytes.Buffer
			args := []string{"auth", "login", "--token-stdin", "--json"}
			if tc.account != "" {
				args = append(args, "--account", tc.account)
			}
			code := qaLegacyRun(t, context.Background(), args, strings.NewReader(" synthetic-secret\n"), &out, &errOut, cli.Dependencies{Store: s, ConfigPath: path, Getenv: func(string) string { return "" }, NewProvider: func(token, account string) harvest.Provider {
				if token != "synthetic-secret" || account != "" {
					t.Error("login provider input not trimmed/isolated")
				}
				return tc.api
			}})
			envelope(t, result{code: code, out: out.String(), err: errOut.String()}, tc.exit, tc.code)
			if s.sets != tc.sets || s.gets != 0 {
				t.Fatalf("credential calls: %+v", s)
			}
			data, err := os.ReadFile(path)
			if tc.exit == 0 {
				if err != nil {
					t.Fatal(err)
				}
				var cfg map[string]any
				if json.Unmarshal(data, &cfg) != nil || len(cfg) != 1 || cfg["account_id"] != "11" {
					t.Fatalf("invalid nonsecret config: %s", data)
				}
			} else if !os.IsNotExist(err) {
				t.Fatal("failed login changed config")
			}
		})
	}
}

func TestQAEnvironmentTokenOverridesStoreAndAccountPrecedence(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "env account", true: "flag account"}[explicit], func(t *testing.T) {
			s := &fakeStore{}
			a := &fakeAPI{}
			path := filepath.Join(t.TempDir(), "config.json")
			if err := auth.Save(path, auth.Config{Account: "33"}); err != nil {
				t.Fatal(err)
			}
			var out, stderr bytes.Buffer
			args := []string{"time", "list", "--json"}
			want := "22"
			if explicit {
				args = append(args, "--account", "44")
				want = "44"
			}
			code := qaLegacyRun(t, context.Background(), args, strings.NewReader(""), &out, &stderr, cli.Dependencies{Store: s, ConfigPath: path, Getenv: func(k string) string {
				if k == "HARVEST_TOKEN" {
					return "synthetic-env"
				}
				if k == "HARVEST_ACCOUNT_ID" {
					return "22"
				}
				return ""
			}, NewProvider: func(token, account string) harvest.Provider {
				if token != "synthetic-env" || account != want {
					t.Errorf("wrong precedence %q %q", token, account)
				}
				return a
			}})
			envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
			if s.gets != 0 {
				t.Fatal("environment token caused Keychain read")
			}
		})
	}
}

func TestQAAuthStatusOfflineAndLogout(t *testing.T) {
	r := run(t, &fakeAPI{}, "auth", "status", "--json")
	envelope(t, r, 0, "")
	if r.factories != 0 || r.store.gets != 1 {
		t.Fatal("status must read credential availability without network")
	}
	r = run(t, &fakeAPI{}, "auth", "logout", "--json")
	envelope(t, r, 6, "confirmation_required")
	if r.store.deletes != 0 {
		t.Fatal("unconfirmed logout deleted token")
	}
	r = run(t, &fakeAPI{}, "auth", "logout", "--yes", "--json")
	envelope(t, r, 0, "")
	if r.store.deletes != 1 || r.factories != 0 {
		t.Fatal("logout did not remove local token exactly once")
	}
}

func TestQADiscoveryUsesAssignmentsAndExplicitAll(t *testing.T) {
	for _, kind := range []string{"projects", "tasks", "clients"} {
		for _, all := range []bool{false, true} {
			t.Run(kind+map[bool]string{true: " all", false: " assigned"}[all], func(t *testing.T) {
				a := &fakeAPI{}
				args := []string{kind, "list", "--json"}
				path := "/users/me/project_assignments"
				if all {
					args = append(args, "--all")
					path = "/" + kind
				}
				r := run(t, a, args...)
				envelope(t, r, 0, "")
				if len(a.calls) != 1 || a.calls[0].method != "LIST" || a.calls[0].path != path {
					t.Fatalf("discovery calls=%v", a.calls)
				}
			})
		}
	}
}

func TestQADiscoveryTaskFilterAndClientDeduplication(t *testing.T) {
	assignment := func(pid string) harvest.Object {
		return harvest.Object{"project": harvest.Object{"id": json.Number(pid)}, "client": harvest.Object{"id": json.Number("5")}, "task_assignments": []any{map[string]any{"id": json.Number("70"), "task": harvest.Object{"id": json.Number("8")}}}}
	}
	for _, tc := range []struct {
		args  []string
		count int
	}{{[]string{"tasks", "list", "--project", "2", "--json"}, 1}, {[]string{"clients", "list", "--json"}, 1}, {[]string{"projects", "list", "--json"}, 2}} {
		a := &fakeAPI{running: []harvest.Object{assignment("1"), assignment("2")}}
		r := run(t, a, tc.args...)
		envelope(t, r, 0, "")
		var v struct {
			Data []harvest.Object `json:"data"`
		}
		if err := json.Unmarshal([]byte(r.out), &v); err != nil {
			t.Fatal(err)
		}
		if len(v.Data) != tc.count {
			t.Fatalf("wrong discovery count: %s", r.out)
		}
		if tc.args[0] == "tasks" {
			if v.Data[0]["project"].(map[string]any)["id"] != float64(2) {
				t.Fatal("task from wrong project")
			}
		}
	}
}

func TestQANotesThatLookLikeFlagsRemainLiteral(t *testing.T) {
	for _, note := range []string{"--help", "-h", "--json"} {
		t.Run(note, func(t *testing.T) {
			a := &fakeAPI{}
			r := run(t, a, "time", "create", "--project", "1", "--task", "2", "--date", "2026-10-01", "--hours", "1", "--notes", note)
			if r.code != 0 {
				t.Fatalf("exit %d: %s", r.code, r.err)
			}
			w := writes(a)
			if len(w) != 1 || w[0].body["notes"] != note {
				t.Fatalf("literal note replaced by option behavior: writes=%v out=%s", w, r.out)
			}
			var v map[string]any
			if err := json.Unmarshal([]byte(r.out), &v); err != nil {
				t.Fatal(err)
			}
			if _, ok := v["schema_version"]; ok {
				t.Fatal("literal note changed output to JSON envelope")
			}
		})
	}
}

func TestQADurationFormatsAndRelativeDates(t *testing.T) {
	for _, tc := range []struct {
		duration string
		want     float64
	}{{"1h30m", 1.5}, {"1:30", 1.5}, {"24", 24}, {"30s", 1.0 / 120}} {
		t.Run(tc.duration, func(t *testing.T) {
			a := &fakeAPI{}
			r := run(t, a, "time", "create", "--project", "1", "--task", "2", "--date", "yesterday", "--duration", tc.duration, "--json")
			envelope(t, r, 0, "")
			w := writes(a)
			if len(w) != 1 || w[0].body["hours"] != tc.want || w[0].body["spent_date"] != "2026-09-30" {
				t.Fatalf("duration/date body=%v", w)
			}
		})
	}
}

func TestQANewTimerHasNoCompletedDuration(t *testing.T) {
	a := &fakeAPI{}
	r := run(t, a, "timer", "start", "--project", "1", "--task", "2", "--notes", "tracking", "--json")
	envelope(t, r, 0, "")
	w := writes(a)
	if len(w) != 1 || w[0].method != "POST" || w[0].body["spent_date"] != "2026-10-01" || w[0].body["notes"] != "tracking" {
		t.Fatalf("wrong timer creation %v", w)
	}
	if _, ok := w[0].body["hours"]; ok {
		t.Fatal("timer created completed duration")
	}
}

func TestQATimerRejectsUnexpectedOwnerOrMalformedRunningData(t *testing.T) {
	for _, entry := range []harvest.Object{{"id": json.Number("91"), "user": harvest.Object{"id": json.Number("8")}, "is_running": true}, {"id": json.Number("91"), "user": harvest.Object{"id": json.Number("7")}, "is_running": false}, {"id": "bad", "user": harvest.Object{"id": json.Number("7")}, "is_running": true}} {
		a := &fakeAPI{running: []harvest.Object{entry}}
		r := run(t, a, "timer", "start", "91", "--json")
		envelope(t, r, 7, "response")
		if len(writes(a)) != 0 {
			t.Fatal("malformed running data allowed write")
		}
	}
}

func TestQANoncanonicalIDsRejectedBeforeAnySideEffect(t *testing.T) {
	for _, args := range [][]string{{"time", "delete", "091", "--yes", "--json"}, {"time", "create", "--project", "01", "--task", "2", "--date", "2026-10-01", "--hours", "1", "--json"}, {"time", "list", "--account", "011", "--json"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			r := run(t, &fakeAPI{}, args...)
			envelope(t, r, 2, "validation")
			if r.store.gets+r.store.sets+r.store.deletes+r.factories != 0 {
				t.Fatal("noncanonical ID touched credential store or provider")
			}
		})
	}
}

func TestQARestartRejectsMalformedTargetTimerState(t *testing.T) {
	for _, state := range []any{nil, "false"} {
		a := &fakeAPI{entry: ownedEntry("91", false)}
		a.entry["is_running"] = state
		r := run(t, a, "timer", "start", "91", "--json")
		envelope(t, r, 7, "response")
		if len(writes(a)) != 0 {
			t.Fatal("malformed target state restarted timer")
		}
	}
}

func TestQAMismatchedReturnedEntryNeverMutatesTarget(t *testing.T) {
	for _, args := range [][]string{{"time", "show", "91"}, {"time", "update", "91", "--notes", "edited"}, {"time", "delete", "91", "--yes"}, {"timer", "start", "91"}, {"timer", "stop", "91"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			a := &fakeAPI{entry: ownedEntry("92", true)}
			r := run(t, a, append(args, "--json")...)
			envelope(t, r, 7, "response")
			if len(writes(a)) != 0 {
				t.Fatal("mismatched returned entry authorized mutation of requested target")
			}
		})
	}
}
func TestQAAPIErrorExitMatrix(t *testing.T) {
	for _, tc := range []struct {
		code string
		exit int
	}{{"auth", 3}, {"forbidden", 4}, {"not_found", 5}, {"validation", 2}, {"rate_limit", 7}, {"network", 7}, {"api", 7}, {"response", 7}} {
		t.Run(tc.code, func(t *testing.T) {
			a := &fakeAPI{listErr: &harvest.Error{Code: tc.code, Message: "synthetic-secret must never escape"}}
			r := run(t, a, "time", "list", "--json")
			envelope(t, r, tc.exit, tc.code)
		})
	}
}

func TestQATimerStopIsIdempotent(t *testing.T) {
	for _, args := range [][]string{{"timer", "stop", "--json"}, {"timer", "stop", "91", "--json"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			a := &fakeAPI{entry: ownedEntry("91", false)}
			r := run(t, a, args...)
			envelope(t, r, 0, "")
			if len(writes(a)) != 0 {
				t.Fatal("stopping stopped timer wrote again")
			}
			if len(args) == 3 {
				var v struct {
					Data map[string]any `json:"data"`
				}
				if err := json.Unmarshal([]byte(r.out), &v); err != nil {
					t.Fatal(err)
				}
				if v.Data["stopped"] != false {
					t.Fatal("missing stopped:false for no running timer")
				}
			}
		})
	}
}
