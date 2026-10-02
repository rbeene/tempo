package activity

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

func lexicalPath(path string) (string, error) {
	if path != "" && !safeIdentifier(path, 4096) {
		return "", failure("validation")
	}
	p, err := filepath.Abs(path)
	if err != nil {
		return "", failure("binding_unavailable")
	}
	return filepath.Clean(p), nil
}
func canonicalDirectory(path string) (string, error) {
	p, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", failure("binding_unavailable")
	}
	fi, err := os.Stat(p)
	if err != nil || !fi.IsDir() || !safeIdentifier(p, 4096) {
		return "", failure("binding_unavailable")
	}
	return filepath.Clean(p), nil
}
func containsPath(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

type boundedGitOutput struct{ bytes.Buffer }

func (b *boundedGitOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 16384 {
		return 0, errors.New("git output exceeds limit")
	}
	return b.Buffer.Write(p)
}
func gitProbe(ctx context.Context, dir string, args ...string) (string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "GIT_") && !strings.HasPrefix(v, "LC_ALL=") && !strings.HasPrefix(v, "LANG=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "LC_ALL=C", "LANG=C")
	var out, stderr boundedGitOutput
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err := cmd.Run()
	return out.String(), stderr.String(), err
}

// DiscoverLocation never creates files or reads credentials. A failed Git
// discovery is not evidence that a path has ordinary-directory scope.
func DiscoverLocation(ctx context.Context, path string) (Location, error) {
	lexical, err := lexicalPath(path)
	if err != nil {
		return Location{}, err
	}
	p, err := canonicalDirectory(lexical)
	if err != nil {
		return Location{}, err
	}
	out, stderr, probeErr := gitProbe(ctx, p, "rev-parse", "--path-format=absolute", "--git-common-dir", "--show-toplevel", "--is-inside-work-tree")
	if probeErr != nil {
		if out != "" || stderr != "fatal: not a git repository (or any of the parent directories): .git\n" {
			return Location{}, failure("binding_unavailable")
		}
		// A malformed/inaccessible .git boundary must never inherit a directory link.
		for cur := p; ; cur = filepath.Dir(cur) {
			_, e := os.Lstat(filepath.Join(cur, ".git"))
			if !errors.Is(e, os.ErrNotExist) {
				return Location{}, failure("binding_unavailable")
			}
			if filepath.Dir(cur) == cur {
				break
			}
		}
		return Location{Kind: "directory", Locator: p, Root: p, Path: p}, nil
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 || lines[2] != "true" {
		return Location{}, failure("binding_unavailable")
	}
	paths := make([]string, 2)
	for i := 0; i < 2; i++ {
		value := lines[i]
		if !filepath.IsAbs(value) {
			value = filepath.Join(p, value)
		}
		paths[i], err = canonicalDirectory(value)
		if err != nil {
			return Location{}, err
		}
	}
	if !containsPath(paths[1], p) {
		return Location{}, failure("binding_unavailable")
	}
	return Location{Kind: "repository", Locator: paths[0], Root: paths[1], Path: p}, nil
}
