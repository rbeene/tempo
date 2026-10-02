package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/harvest"
)

type qaNeverRead struct{ t *testing.T }

func (r qaNeverRead) Read([]byte) (int, error) {
	r.t.Fatal("noninteractive missing choice read stdin")
	return 0, errors.New("forbidden input")
}
func TestQABindingCLINoninteractiveMissingProjectNeverTouchesDependencies(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "absent", "state.json")
	var out, stderr bytes.Buffer
	d := cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Store: &fakeStore{}, Getenv: func(string) string { t.Fatal("missing project read environment/config"); return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("missing project built provider"); return nil }}
	code := qaLegacyRun(t, context.Background(), []string{"link", "--non-interactive"}, qaNeverRead{t}, &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 2, "input_required")
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing project initialized state: %v", err)
	}
}
func TestQABindingCLILocalListOfflineFiniteEnvelope(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "absent", "state.json")
	var out, stderr bytes.Buffer
	store := &fakeStore{}
	d := cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Store: store, ConfigPath: filepath.Join(root, "missing"), Getenv: func(string) string { t.Fatal("local list read account environment"); return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("local list constructed provider"); return nil }}
	code := qaLegacyRun(t, context.Background(), []string{"links", "list", "--non-interactive"}, qaNeverRead{t}, &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
	if store.gets+store.sets+store.deletes != 0 {
		t.Fatal("local list accessed credentials")
	}
	if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("list initialized state: %v", err)
	}
}

type qaCLILinkAPI struct{ fakeAPI }

func (a *qaCLILinkAPI) Get(_ context.Context, p string) (harvest.Object, error) {
	return harvest.Object{"id": json.Number("7"), "is_active": true}, nil
}
func (a *qaCLILinkAPI) List(_ context.Context, p string, _ url.Values) ([]harvest.Object, error) {
	return []harvest.Object{{"is_active": true, "project": harvest.Object{"id": json.Number("100")}, "task_assignments": []any{harvest.Object{"is_active": true, "task": harvest.Object{"id": json.Number("200")}}}}}, nil
}
func TestQABindingCLIReplaySkipsChangedConfigAccountAndCredentials(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state", "activity.json")
	location := filepath.Join(root, "project")
	if err := os.Mkdir(location, 0700); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config.json")
	store := &fakeStore{}
	api := &qaCLILinkAPI{}
	d := cli.Dependencies{Activity: activity.New(activity.Options{Path: path}), Store: store, ConfigPath: config, Getenv: func(k string) string {
		if k == "HARVEST_ACCOUNT_ID" {
			return "11"
		}
		return ""
	}, NewProvider: func(_ string, account string) harvest.Provider {
		if account != "11" {
			t.Fatalf("account factory=%s", account)
		}
		return api
	}}
	args := []string{"link", "100", "--task", "200", "--timezone", "UTC", "--path", location, "--request-id", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "--non-interactive"}
	var out, stderr bytes.Buffer
	code := qaLegacyRun(t, context.Background(), args, qaNeverRead{t}, &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
	first := out.String()
	if err := os.Remove(location); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte("PRIVATE-BROKEN-CONFIG"), 0600); err != nil {
		t.Fatal(err)
	}
	gets := store.gets
	d.Activity = activity.New(activity.Options{Path: path})
	d.Getenv = func(string) string { t.Fatal("replay read changed account environment"); return "" }
	d.NewProvider = func(string, string) harvest.Provider { t.Fatal("replay built authenticated provider"); return nil }
	out.Reset()
	stderr.Reset()
	code = qaLegacyRun(t, context.Background(), args, qaNeverRead{t}, &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
	if first != out.String() || store.gets != gets {
		t.Fatalf("replay changed result or accessed credentials: first=%s next=%s gets=%d/%d", first, out.String(), gets, store.gets)
	}
	out.Reset()
	stderr.Reset()
	args = append(args, "--account", "9")
	code = qaLegacyRun(t, context.Background(), args, qaNeverRead{t}, &out, &stderr, d)
	envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 6, "request_conflict")
}

func TestQABindingCLILocalInspectionRepairUnlinkStayOffline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state", "activity.json")
	s := activity.New(activity.Options{Path: path})
	location := filepath.Join(root, "original")
	if err := os.Mkdir(location, 0700); err != nil {
		t.Fatal(err)
	}
	first, err := s.Link(context.Background(), activity.LinkInput{Path: location, ProjectID: "100", TaskID: "200", AccountID: "11", Timezone: "UTC", RequestID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, activity.LinkDependencies{NewProvider: func(context.Context, string) (harvest.Provider, error) { return &qaCLILinkAPI{}, nil }})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	d := cli.Dependencies{Activity: s, Store: store, ConfigPath: filepath.Join(root, "missing"), Getenv: func(string) string { t.Fatal("local mutation read account environment"); return "" }, NewProvider: func(string, string) harvest.Provider { t.Fatal("local mutation created provider"); return nil }}
	runLocal := func(args ...string) string {
		t.Helper()
		var out, stderr bytes.Buffer
		args = append(args, "--non-interactive")
		code := qaLegacyRun(t, context.Background(), args, qaNeverRead{t}, &out, &stderr, d)
		envelope(t, result{code: code, out: out.String(), err: stderr.String()}, 0, "")
		return out.String()
	}
	runLocal("links", "show", first.Binding.ID)
	moved := filepath.Join(root, "moved")
	if err := os.Rename(location, moved); err != nil {
		t.Fatal(err)
	}
	raw := runLocal("links", "repair", first.Binding.ID, "--path", moved, "--if-revision", first.Binding.Revision, "--yes", "--request-id", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	var result struct {
		Data activity.BindingResult `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatal(err)
	}
	runLocal("links", "unlink", first.Binding.ID, "--if-revision", result.Data.Binding.Revision, "--yes", "--request-id", "cccccccc-cccc-4ccc-8ccc-cccccccccccc")
	list, err := s.ListBindings(context.Background())
	if err != nil || len(list.Bindings) != 0 {
		t.Fatalf("CLI unlink did not remove live binding %+v %v", list, err)
	}
	if store.gets+store.sets+store.deletes != 0 {
		t.Fatal("local commands accessed credentials")
	}
}
