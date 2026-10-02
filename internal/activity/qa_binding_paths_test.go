package activity

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func qaGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "GIT_") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "GIT_AUTHOR_NAME=QA", "GIT_AUTHOR_EMAIL=qa@example.invalid", "GIT_COMMITTER_NAME=QA", "GIT_COMMITTER_EMAIL=qa@example.invalid")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func TestQABindingDiscoverWorktreesShareRepositoryScope(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	qaGit(t, repo, "init")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "-m", "fixture")
	worktree := filepath.Join(root, "worktree")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "worktree", "add", "-b", "qa-branch", worktree)
	sub := filepath.Join(repo, "sub")
	if err := os.Mkdir(sub, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(sub, alias); err != nil {
		t.Fatal(err)
	}
	var locator string
	for _, path := range []string{repo, sub, worktree, alias} {
		got, err := DiscoverLocation(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		if got.Kind != "repository" || !filepath.IsAbs(got.Locator) {
			t.Fatalf("bad scope %+v", got)
		}
		if locator == "" {
			locator = got.Locator
		}
		if got.Locator != locator {
			t.Fatalf("worktree/symlink escaped shared identity: %+v want %s", got, locator)
		}
	}
}
func TestQABindingDiscoverMalformedGitNeverFallsBackToDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("gitdir: /missing/private-git-dir\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := DiscoverLocation(context.Background(), root)
	qaCode(t, err, "binding_unavailable")
}

func TestQABindingDiscoveryNeutralizesGitEnvironment(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	for _, p := range []string{a, b} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
		qaGit(t, p, "init")
	}
	for k, v := range map[string]string{"GIT_DIR": filepath.Join(b, ".git"), "GIT_WORK_TREE": b, "GIT_COMMON_DIR": filepath.Join(b, ".git"), "GIT_CEILING_DIRECTORIES": root, "GIT_CONFIG_COUNT": "1", "GIT_CONFIG_KEY_0": "core.worktree", "GIT_CONFIG_VALUE_0": b} {
		t.Setenv(k, v)
	}
	got, err := DiscoverLocation(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(filepath.Join(a, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Locator != canonical {
		t.Fatalf("environment hijacked Git identity: %+v", got)
	}
}
func TestQABindingDiscoveryMissingGitCannotGuessDirectory(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PATH", t.TempDir())
	_, err := DiscoverLocation(context.Background(), root)
	qaCode(t, err, "binding_unavailable")
}
func TestQABindingDirectoryNearestMatchStopsAtGitBoundary(t *testing.T) {
	s, _ := qaLinkService(t)
	p := qaNewLinkProvider(t)
	in := qaLinkInput(t)
	first, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(in.Path, "child")
	deep := filepath.Join(child, "deep")
	if err := os.MkdirAll(deep, 0700); err != nil {
		t.Fatal(err)
	}
	in.Path = child
	in.RequestID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	second, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.ShowBinding(context.Background(), ShowBindingInput{Path: deep})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Bindings) != 1 || got.Bindings[0].ID != second.Binding.ID || first.Binding.ID == second.Binding.ID {
		t.Fatalf("nearest binding=%+v", got)
	}
	nested := filepath.Join(deep, "nested")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	qaGit(t, nested, "init")
	_, err = s.ShowBinding(context.Background(), ShowBindingInput{Path: nested})
	qaCode(t, err, "not_found")
	sibling := in.Path + "-prefix-sibling"
	if err := os.Mkdir(sibling, 0700); err != nil {
		t.Fatal(err)
	}
	// This sibling is still inside the first ancestor, but must not match child.
	got, err = s.ShowBinding(context.Background(), ShowBindingInput{Path: sibling})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Bindings) != 1 || got.Bindings[0].ID != first.Binding.ID {
		t.Fatalf("prefix sibling matched child: %+v", got)
	}
}
func TestQABindingIndependentCloneNestedRepoAndSubmoduleDoNotInherit(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	source := filepath.Join(root, "source")
	for _, p := range []string{repo, source} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
		qaGit(t, p, "init")
		qaGit(t, p, "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "-m", "fixture")
	}
	clone := filepath.Join(root, "clone")
	qaGit(t, root, "-c", "core.hooksPath=/dev/null", "clone", repo, clone)
	nested := filepath.Join(repo, "nested")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	qaGit(t, nested, "init")
	qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=always", "submodule", "add", source, "module")
	s, _ := qaLinkService(t)
	in := qaLinkInput(t)
	in.Path = repo
	if _, err := s.Link(context.Background(), in, qaLinkDeps(t, qaNewLinkProvider(t))); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{clone, nested, filepath.Join(repo, "module")} {
		_, err := s.ShowBinding(context.Background(), ShowBindingInput{Path: path})
		qaCode(t, err, "not_found")
	}
}
func TestQABindingLocationDriftDuringRemoteValidationCannotCommit(t *testing.T) {
	root := t.TempDir()
	a := filepath.Join(root, "a")
	b := filepath.Join(root, "b")
	alias := filepath.Join(root, "alias")
	for _, p := range []string{a, b} {
		if err := os.Mkdir(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(a, alias); err != nil {
		t.Fatal(err)
	}
	s, path := qaLinkService(t)
	in := qaLinkInput(t)
	in.Path = alias
	p := qaNewLinkProvider(t)
	p.beforeList = func() {
		if err := os.Remove(alias); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(b, alias); err != nil {
			t.Fatal(err)
		}
	}
	_, err := s.Link(context.Background(), in, qaLinkDeps(t, p))
	qaCode(t, err, "binding_unavailable")
	qaAbsentLinkState(t, path)
}

func TestQABindingGitPointerDriftDuringValidationCannotCommit(t *testing.T) {
	root := t.TempDir()
	var worktrees []string
	for _, name := range []string{"a", "b"} {
		repo := filepath.Join(root, name)
		if err := os.Mkdir(repo, 0700); err != nil {
			t.Fatal(err)
		}
		qaGit(t, repo, "init")
		qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "-m", "fixture")
		worktree := filepath.Join(root, name+"-worktree")
		qaGit(t, repo, "-c", "core.hooksPath=/dev/null", "worktree", "add", "-b", "qa-branch", worktree)
		worktrees = append(worktrees, worktree)
	}
	replacement, err := os.ReadFile(filepath.Join(worktrees[1], ".git"))
	if err != nil {
		t.Fatal(err)
	}
	s, path := qaLinkService(t)
	in := qaLinkInput(t)
	in.Path = worktrees[0]
	p := qaNewLinkProvider(t)
	p.beforeList = func() {
		if err := os.WriteFile(filepath.Join(worktrees[0], ".git"), replacement, 0600); err != nil {
			t.Fatal(err)
		}
	}
	_, err = s.Link(context.Background(), in, qaLinkDeps(t, p))
	qaCode(t, err, "binding_unavailable")
	qaAbsentLinkState(t, path)
}
