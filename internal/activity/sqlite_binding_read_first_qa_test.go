//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func TestSQLiteBindingReadFirstAbsentAndValidationNeverInitialize(t *testing.T) {
	root := brQAHome(t)
	project := brQADirectory(t, filepath.Join(root, "project"))
	path := filepath.Join(root, "missing-state-parent", "activity.json")
	s := brQAService(t, path)
	got, err := s.ListBindings(context.Background())
	brQAList(t, got, err, "0", []Binding{})
	for _, input := range []ShowBindingInput{{BindingID: brQAID(99)}, {Path: project}} {
		got, err = brQAService(t, path).ShowBinding(context.Background(), input)
		brQARefusal(t, got, err, "not_found")
	}
	for _, input := range []ShowBindingInput{{BindingID: "invalid"}, {BindingID: brQAID(99), Path: project}} {
		got, err = s.ShowBinding(context.Background(), input)
		brQARefusal(t, got, err, "validation")
	}
	if _, err := os.Lstat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("absent binding reads initialized storage or locks", err)
	}
}

func TestSQLiteBindingReadFirstDirectoryNearestColdHistoryAndUnavailablePath(t *testing.T) {
	root := brQAHome(t)
	parent := brQADirectory(t, filepath.Join(root, "project"))
	child := brQADirectory(t, filepath.Join(parent, "child"))
	deep := brQADirectory(t, filepath.Join(child, "deep"))
	sibling := brQADirectory(t, filepath.Join(parent, "child-prefix-sibling"))
	unlinked := brQADirectory(t, filepath.Join(root, "unlinked"))
	path := filepath.Join(root, "state", "activity.json")
	s := brQAService(t, path)
	input := brQAInput(parent, 1)
	first := brQALink(t, s, input)
	if first.Binding.Kind != "directory" || first.Binding.Locator != parent || first.Binding.Revision != "1" {
		t.Fatal("SETUP directory scope")
	}
	changed := input
	changed.IfRevision, changed.Timezone, changed.RequestID = "1", "America/New_York", brQAID(2)
	current := brQALink(t, s, changed)
	if current.Binding.ID != first.Binding.ID || current.Binding.Revision != "2" || current.Binding.Attribution.Timezone != changed.Timezone {
		t.Fatal("SETUP current binding revision")
	}
	f := brQALocation(t, path)
	beforeReplay := brQARead(t, f, []Binding{current.Binding}, []BindingResult{first, current})
	replay, err := brQAService(t, path).Link(context.Background(), input, brQAOffline(t))
	if err != nil || !reflect.DeepEqual(replay, first) {
		t.Fatal("SETUP exact historical public Link replay", err)
	}
	afterReplay := brQARead(t, f, []Binding{current.Binding}, []BindingResult{first, current})
	wantMeta := beforeReplay.meta
	wantMeta.DurabilityNonce, err = sqliteNextNonce(wantMeta.DurabilityNonce[:])
	if err != nil || !reflect.DeepEqual(wantMeta, afterReplay.meta) || !reflect.DeepEqual(beforeReplay.bindings, afterReplay.bindings) || !reflect.DeepEqual(beforeReplay.requests, afterReplay.requests) {
		t.Fatal("historical replay rewrote current binding or missed nonce", err)
	}
	for table, rows := range beforeReplay.rows {
		if table != "store_meta" && !reflect.DeepEqual(rows, afterReplay.rows[table]) {
			t.Fatal("historical replay changed retained rows", table)
		}
	}
	childInput := brQAInput(child, 3)
	childInput.Timezone = changed.Timezone
	nearest := brQALink(t, s, childInput)
	if nearest.Binding.ID == current.Binding.ID || nearest.Binding.Kind != "directory" || nearest.Binding.Locator != child {
		t.Fatal("SETUP distinct nearest directory")
	}
	bindings := []Binding{current.Binding, nearest.Binding}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].ID < bindings[j].ID })
	receipts := []BindingResult{first, current, nearest}
	before := brQARead(t, f, bindings, receipts)
	got, err := brQAService(t, path).ListBindings(context.Background())
	brQAList(t, got, err, before.meta.Revision, bindings)
	for _, tc := range []struct {
		input ShowBindingInput
		want  Binding
	}{
		{ShowBindingInput{BindingID: first.Binding.ID}, current.Binding},
		{ShowBindingInput{Path: parent}, current.Binding},
		{ShowBindingInput{Path: deep}, nearest.Binding},
		{ShowBindingInput{Path: sibling}, current.Binding},
	} {
		got, err = brQAService(t, path).ShowBinding(context.Background(), tc.input)
		brQAList(t, got, err, before.meta.Revision, []Binding{tc.want})
	}
	got, err = s.ShowBinding(context.Background(), ShowBindingInput{Path: unlinked})
	brQARefusal(t, got, err, "not_found")
	got, err = s.ShowBinding(context.Background(), ShowBindingInput{BindingID: brQAID(99)})
	brQARefusal(t, got, err, "not_found")
	if after := brQARead(t, f, bindings, receipts); !reflect.DeepEqual(before, after) {
		t.Fatal("binding reads changed rows, receipts, revision, nonce or charge")
	}
	brQAPrivate(t, f)
	// A disappeared locator is an availability problem, not a deleted binding.
	moved := filepath.Join(root, "moved-child")
	if err := os.Rename(child, moved); err != nil {
		t.Fatal(err)
	}
	got, err = brQAService(t, path).ShowBinding(context.Background(), ShowBindingInput{BindingID: nearest.Binding.ID})
	brQARefusal(t, got, err, "binding_unavailable")
	got, err = s.ShowBinding(context.Background(), ShowBindingInput{Path: moved})
	brQARefusal(t, got, err, "not_found")
	got, err = s.ListBindings(context.Background())
	brQAList(t, got, err, before.meta.Revision, bindings)
	if after := brQARead(t, f, bindings, receipts); !reflect.DeepEqual(before, after) {
		t.Fatal("unavailable locator changed retained mapping")
	}
	brQAPrivate(t, f)
}

func TestSQLiteBindingReadFirstRepositoryWorktreeAndGitBoundary(t *testing.T) {
	root := brQAHome(t)
	repo := brQADirectory(t, filepath.Join(root, "repo"))
	qaGit(t, repo, "init")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "owned binding-read fixture")
	work := filepath.Join(root, "worktree")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "worktree", "add", "-b", "binding-read-fixture", work)
	sub := brQADirectory(t, filepath.Join(work, "sub"))
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(sub, alias); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "state", "activity.json")
	s := brQAService(t, path)
	linked := brQALink(t, s, brQAInput(sub, 10))
	common, err := filepath.EvalSymlinks(filepath.Join(repo, ".git"))
	if err != nil || linked.Binding.Kind != "repository" || linked.Binding.Locator != common {
		t.Fatal("SETUP actual shared Git common-directory scope", err)
	}
	f := brQALocation(t, path)
	bindings, receipts := []Binding{linked.Binding}, []BindingResult{linked}
	before := brQARead(t, f, bindings, receipts)
	// Enter Show independently of List so the initial unsupported route is a
	// meaningful behavior failure for each of the two existing public methods.
	got, err := s.ShowBinding(context.Background(), ShowBindingInput{BindingID: linked.Binding.ID})
	brQAList(t, got, err, before.meta.Revision, bindings)
	for _, location := range []string{repo, work, sub, alias} {
		got, err = brQAService(t, path).ShowBinding(context.Background(), ShowBindingInput{Path: location})
		brQAList(t, got, err, before.meta.Revision, bindings)
	}
	got, err = brQAService(t, path).ListBindings(context.Background())
	brQAList(t, got, err, before.meta.Revision, bindings)
	nested := brQADirectory(t, filepath.Join(sub, "independent"))
	qaGit(t, nested, "init")
	got, err = s.ShowBinding(context.Background(), ShowBindingInput{Path: nested})
	brQARefusal(t, got, err, "not_found")
	if after := brQARead(t, f, bindings, receipts); !reflect.DeepEqual(before, after) {
		t.Fatal("worktree/nested lookup changed durable rows")
	}
	brQAPrivate(t, f)
}
