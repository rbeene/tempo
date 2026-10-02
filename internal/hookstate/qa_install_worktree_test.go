package hookstate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func qaInstallGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	prefix := []string{"-c", "init.defaultBranch=main", "-c", "user.name=Tempo QA", "-c", "user.email=qa@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}
	cmd := exec.CommandContext(ctx, git, append(prefix, args...)...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_TERMINAL_PROMPT=0")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("synthetic local git %v: %v %s", args, err, output)
	}
}
func qaInstallWorktree(t *testing.T) (qaInstallFixture, string) {
	t.Helper()
	f := qaNewInstallFixture(t)
	qaInstallGit(t, f.project, "init")
	qaInstallGit(t, f.project, "commit", "--allow-empty", "-m", "synthetic baseline")
	linked := filepath.Join(f.root, "linked-checkout")
	qaInstallGit(t, f.project, "worktree", "add", "--detach", linked, "HEAD")
	return f, linked
}
func TestQAInstallCodexLinkedWorktreePreviewsActualSharedHooksAndLocalResources(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "root", true: "nested"}[nested], func(t *testing.T) {
			f, linked := qaInstallWorktree(t)
			mainTarget := f.project
			selected := linked
			if nested {
				mainTarget = filepath.Join(mainTarget, "subproject")
				selected = filepath.Join(selected, "subproject")
				if err := os.MkdirAll(mainTarget, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(selected, 0700); err != nil {
					t.Fatal(err)
				}
			}
			intent := InstallIntent{Host: "codex", Scope: "project", Path: selected, Operation: "install"}
			s := New(f.options)
			before := qaInstallTree(t, f.root)
			preview, err := s.PreviewInstall(context.Background(), intent)
			if err != nil {
				t.Fatal(err)
			}
			qaInstallAssertUnchanged(t, f, before)
			paths := map[string]bool{}
			description := strings.Join(preview.ApprovalSteps, " ")
			for _, change := range preview.Changes {
				paths[change.Path] = true
				description += " " + change.SafeSummary
			}
			for _, path := range []string{filepath.Join(mainTarget, ".codex", "hooks.json"), filepath.Join(selected, ".codex", "config.toml"), filepath.Join(selected, ".agents", "skills", "tempo", "SKILL.md")} {
				if !paths[path] {
					t.Fatalf("preview misses effective target %s: %+v", path, preview.Changes)
				}
			}
			if paths[filepath.Join(selected, ".codex", "hooks.json")] {
				t.Fatal("preview targets inert linked-checkout hooks instead of host consumer")
			}
			if !strings.Contains(strings.ToLower(description), "worktree") {
				t.Fatal("preview did not disclose shared linked-worktree effect")
			}
			qaInstallApply(t, s, intent, 101)
			qaInstallOwnedCommands(t, filepath.Join(mainTarget, ".codex", "hooks.json"), f.executable)
			if _, err := os.Stat(filepath.Join(selected, ".codex", "hooks.json")); !os.IsNotExist(err) {
				t.Fatal("install wrote inert linked hooks")
			}
			if _, err := os.Stat(filepath.Join(mainTarget, ".codex", "config.toml")); !os.IsNotExist(err) {
				t.Fatal("install silently changed main-checkout tooltip config")
			}
			status := qaInstallStatus(t, s, HookSelector{Host: "codex", Scope: "project", Path: selected})
			if status.Profile.Context.Path != selected {
				t.Fatalf("policy context replaced selected worktree with main: %+v", status.Profile.Context)
			}
			found := false
			for _, a := range status.Profile.Context.Artifacts {
				if a.Role == "definitions" && a.Path == filepath.Join(mainTarget, ".codex", "hooks.json") {
					found = true
				}
			}
			if !found {
				t.Fatal("policy did not bind effective shared definitions")
			}
		})
	}
}
func TestQAInstallClaudeLinkedWorktreeStaysLocal(t *testing.T) {
	f, linked := qaInstallWorktree(t)
	intent := InstallIntent{Host: "claude", Scope: "project", Path: linked, Operation: "install"}
	preview, err := New(f.options).PreviewInstall(context.Background(), intent)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, change := range preview.Changes {
		if strings.HasPrefix(change.Path, f.project+string(filepath.Separator)) {
			t.Fatal("Codex shared-target rule leaked into Claude")
		}
		if change.Path == filepath.Join(linked, ".claude", "settings.json") {
			found = true
		}
	}
	if !found {
		t.Fatal("Claude local target missing")
	}
}
func TestQAInstallCodexGitRepointRejectsStalePreviewWithoutWrites(t *testing.T) {
	f, linked := qaInstallWorktree(t)
	other := filepath.Join(f.root, "other-linked")
	qaInstallGit(t, f.project, "worktree", "add", "--detach", other, "HEAD")
	intent := InstallIntent{Host: "codex", Scope: "project", Path: linked, Operation: "install"}
	s := New(f.options)
	in := qaInstallInput(t, s, intent, 102)
	qaInstallWrite(t, filepath.Join(linked, ".git"), qaInstallRead(t, filepath.Join(other, ".git")))
	before := qaInstallTree(t, f.root)
	_, err := s.ApplyInstall(context.Background(), in)
	if err == nil {
		t.Fatal("repointed Git context accepted stale reviewed target")
	}
	qaInstallAssertUnchanged(t, f, before)
}
func qaInstallRecreateGitDirectory(t *testing.T, path string) {
	t.Helper()
	saved := path + "-previous-inode"
	if err := os.Rename(path, saved); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(path, os.DirFS(saved)); err != nil {
		t.Fatal(err)
	}
}
func TestQAInstallCodexGitDirectoryRecreationInvalidatesPreviewAndPolicy(t *testing.T) {
	for _, phase := range []string{"preview", "policy"} {
		t.Run(phase, func(t *testing.T) {
			f, linked := qaInstallWorktree(t)
			s := New(f.options)
			intent := InstallIntent{Host: "codex", Scope: "project", Path: linked, Operation: "install"}
			if phase == "preview" {
				in := qaInstallInput(t, s, intent, 103)
				qaInstallRecreateGitDirectory(t, filepath.Join(f.project, ".git"))
				before := qaInstallTree(t, f.root)
				_, err := s.ApplyInstall(context.Background(), in)
				if err == nil {
					t.Fatal("same-byte recreated Git identity accepted stale preview")
				}
				qaInstallAssertUnchanged(t, f, before)
			} else {
				qaInstallApply(t, s, intent, 104)
				selector := HookSelector{Host: "codex", Scope: "project", Path: linked}
				qaInstallConfirmFromStatus(t, s, selector, 105)
				qaInstallRecreateGitDirectory(t, filepath.Join(f.project, ".git"))
				status := qaInstallStatus(t, s, selector)
				if status.Profile.CaptureEligible {
					t.Fatal("same-byte recreated Git identity retained status eligibility")
				}
				eligible, err := s.Eligibility(context.Background(), "codex", linked)
				if err != nil {
					t.Fatal(err)
				}
				if eligible.CaptureEligible {
					t.Fatal("adapter admitted policy after Git identity recreation")
				}
			}
		})
	}
}
func TestQAInstallCodexGitUnsafeOrBareContextsRejectPurely(t *testing.T) {
	for _, kind := range []string{"malformed-link", "symlink-git", "bare"} {
		t.Run(kind, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			switch kind {
			case "malformed-link":
				qaInstallWrite(t, filepath.Join(f.project, ".git"), []byte("gitdir: PRIVATE_SETTINGS_SENTINEL invalid\n"))
			case "symlink-git":
				outside := filepath.Join(f.root, "other-repository")
				if err := os.Mkdir(outside, 0700); err != nil {
					t.Fatal(err)
				}
				qaInstallGit(t, outside, "init")
				if err := os.Symlink(filepath.Join(outside, ".git"), filepath.Join(f.project, ".git")); err != nil {
					t.Fatal(err)
				}
			case "bare":
				qaInstallGit(t, f.project, "init", "--bare")
			}
			before := qaInstallTree(t, f.root)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := New(f.options).PreviewInstall(ctx, f.intent("codex", "project"))
			if err == nil {
				t.Fatal("unsafe/bare Git context accepted")
			}
			if strings.Contains(err.Error(), "PRIVATE_SETTINGS_SENTINEL") {
				t.Fatal("raw Git metadata leaked")
			}
			qaInstallAssertUnchanged(t, f, before)
		})
	}
}
func TestQAInstallCodexGitCancellationIsBoundedAndPure(t *testing.T) {
	f, linked := qaInstallWorktree(t)
	before := qaInstallTree(t, f.root)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, err := New(f.options).PreviewInstall(ctx, InstallIntent{Host: "codex", Scope: "project", Path: linked, Operation: "install"})
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("cancelled discovery did not stop promptly: %v", err)
	}
	qaInstallAssertUnchanged(t, f, before)
}
