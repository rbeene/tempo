package hookstate

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type gitEvidence struct {
	HookRoot     string
	CheckoutRoot string
	MainRoot     string
	Artifacts    []Artifact
}
type gitOutput struct{ bytes.Buffer }

func (b *gitOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 16384 {
		return 0, problem("validation")
	}
	return b.Buffer.Write(p)
}
func probeGit(ctx context.Context, path string, args ...string) (string, string, error) {
	limited, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(limited, "git", args...)
	cmd.Dir = path
	cmd.WaitDelay = 100 * time.Millisecond
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "GIT_") && !strings.HasPrefix(v, "LC_ALL=") && !strings.HasPrefix(v, "LANG=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_TERMINAL_PROMPT=0", "LC_ALL=C", "LANG=C")
	var out, stderr gitOutput
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	return out.String(), stderr.String(), err
}
func discoverGit(ctx context.Context, selected string) (gitEvidence, error) {
	result := gitEvidence{HookRoot: selected, CheckoutRoot: selected, MainRoot: selected, Artifacts: []Artifact{}}
	for dir := selected; ; dir = filepath.Dir(dir) {
		info, e := os.Lstat(filepath.Join(dir, ".git"))
		if e == nil && info.Mode()&os.ModeSymlink != 0 {
			return result, problem("binding_unavailable")
		}
		if e != nil && !errors.Is(e, os.ErrNotExist) {
			return result, problem("binding_unavailable")
		}
		if filepath.Dir(dir) == dir {
			break
		}
	}
	out, stderr, err := probeGit(ctx, selected, "rev-parse", "--path-format=absolute", "--git-common-dir", "--absolute-git-dir", "--show-toplevel", "--is-inside-work-tree")
	if err != nil {
		if ctx.Err() != nil {
			return result, problem("state_busy")
		}
		if out == "" && stderr == "fatal: not a git repository (or any of the parent directories): .git\n" {
			for dir := selected; ; dir = filepath.Dir(dir) {
				if _, e := os.Lstat(filepath.Join(dir, ".git")); !errors.Is(e, os.ErrNotExist) {
					return result, problem("binding_unavailable")
				}
				if filepath.Dir(dir) == dir {
					break
				}
			}
			return result, nil
		}
		return result, problem("binding_unavailable")
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 4 || lines[3] != "true" {
		return result, problem("binding_unavailable")
	}
	for i := 0; i < 3; i++ {
		if !filepath.IsAbs(lines[i]) {
			return result, problem("binding_unavailable")
		}
		lines[i], err = canonicalInstallPath(lines[i], true)
		if err != nil {
			return result, err
		}
	}
	common, gitdir, checkout := lines[0], lines[1], lines[2]
	result.CheckoutRoot, result.MainRoot = checkout, checkout
	if !contains(checkout, selected) {
		return result, problem("binding_unavailable")
	}
	result.Artifacts = append(result.Artifacts, Artifact{Role: "repository", Path: common})
	if gitdir != common {
		result.Artifacts = append(result.Artifacts, Artifact{Role: "repository", Path: gitdir})
	}
	link := filepath.Join(checkout, ".git")
	if fi, e := os.Lstat(link); e != nil {
		return result, problem("binding_unavailable")
	} else if fi.Mode().IsRegular() {
		result.Artifacts = append(result.Artifacts, Artifact{Role: "configuration", Path: link})
	} else if !fi.IsDir() {
		return result, problem("binding_unavailable")
	}
	if gitdir == common {
		return sampleGit(ctx, result)
	}
	out, _, err = probeGit(ctx, selected, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return result, problem("binding_unavailable")
	}
	records := strings.Split(out, "\x00\x00")
	if len(records) == 0 {
		return result, problem("binding_unavailable")
	}
	fields := strings.Split(records[0], "\x00")
	if len(fields) < 2 || !strings.HasPrefix(fields[0], "worktree ") {
		return result, problem("binding_unavailable")
	}
	for _, field := range fields {
		if field == "bare" {
			return result, problem("unsupported_contract")
		}
	}
	main, err := canonicalInstallPath(strings.TrimPrefix(fields[0], "worktree "), true)
	if err != nil {
		return result, err
	}
	mainOut, _, err := probeGit(ctx, main, "rev-parse", "--path-format=absolute", "--git-common-dir", "--absolute-git-dir")
	if err != nil {
		return result, problem("binding_unavailable")
	}
	mainParts := strings.Split(strings.TrimSuffix(mainOut, "\n"), "\n")
	if len(mainParts) != 2 {
		return result, problem("binding_unavailable")
	}
	for _, part := range mainParts {
		resolved, e := canonicalInstallPath(part, true)
		if e != nil || resolved != common {
			return result, problem("binding_unavailable")
		}
	}
	result.MainRoot = main
	rel, err := filepath.Rel(checkout, selected)
	if err != nil {
		return result, problem("binding_unavailable")
	}
	result.HookRoot, err = canonicalInstallPath(filepath.Join(main, rel), false)
	if err != nil {
		return result, err
	}
	for _, name := range []string{"commondir", "gitdir"} {
		result.Artifacts = append(result.Artifacts, Artifact{Role: "configuration", Path: filepath.Join(gitdir, name)})
	}
	return sampleGit(ctx, result)
}
func sampleGit(ctx context.Context, g gitEvidence) (gitEvidence, error) {
	for i, a := range g.Artifacts {
		hash, _, err := hashArtifact(ctx, a.Path, a.Role)
		if err != nil {
			return g, err
		}
		g.Artifacts[i].SHA256 = hash
	}
	return g, nil
}
