package hookstate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

func problem(code string) *Error {
	return &Error{Code: code, Retryable: code == "state_busy", Uncertain: code == "local_write_unknown"}
}

func safeText(s string, max int) bool {
	if s == "" || len(s) > max || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func (s *Service) location() (string, error) {
	p := s.options.Path
	if p == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return "", problem("state_corrupt")
		}
		p = filepath.Join(base, "tempo", "hooks-state.json")
	}
	if !filepath.IsAbs(p) || filepath.Clean(p) != p || !safeText(p, 4096) {
		return "", problem("validation")
	}
	if runtime.GOOS == "darwin" && (strings.HasPrefix(p, "/var/") || strings.HasPrefix(p, "/tmp/")) {
		p = "/private" + p
	}
	return p, nil
}

func contextFingerprint(c Context) string {
	b, _ := json.Marshal(c)
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}

func sampleContext(ctx context.Context, c Context) (Context, error) {
	if c.Host != "codex" && c.Host != "claude" || c.Scope != "project" || c.Surface != "local" || !filepath.IsAbs(c.Path) || !safeText(c.Path, 4096) || len(c.Artifacts) < 3 || len(c.Artifacts) > 16 || len(c.Conflicts) > 16 {
		return Context{}, problem("validation")
	}
	if c.Host == "codex" && c.RuntimeVersion != "0.159.3" || c.Host == "claude" && c.RuntimeVersion != "2.1.286" {
		return Context{}, problem("unsupported_contract")
	}
	path, err := filepath.EvalSymlinks(c.Path)
	if err != nil {
		return Context{}, problem("binding_unavailable")
	}
	fi, err := os.Stat(path)
	if err != nil || !fi.IsDir() {
		return Context{}, problem("binding_unavailable")
	}
	c.Path = filepath.Clean(path)
	c.Artifacts = append([]Artifact{}, c.Artifacts...)
	c.Conflicts = append([]string{}, c.Conflicts...)
	for _, conflict := range c.Conflicts {
		switch conflict {
		case "prompt_veto", "stop_continuation", "async", "untrusted", "runtime_unsupported":
		default:
			return Context{}, problem("validation")
		}
	}
	seen, roles := map[string]bool{}, map[string]bool{}
	var total int64
	for i, artifact := range c.Artifacts {
		if ctx.Err() != nil {
			return Context{}, problem("state_busy")
		}
		switch artifact.Role {
		case "runtime", "executable", "definitions", "skill", "configuration":
		default:
			return Context{}, problem("validation")
		}
		if !filepath.IsAbs(artifact.Path) || filepath.Clean(artifact.Path) != artifact.Path || !safeText(artifact.Path, 4096) || seen[artifact.Path] {
			return Context{}, problem("validation")
		}
		seen[artifact.Path] = true
		digest, size, err := hashArtifact(ctx, artifact.Path, artifact.Role)
		if err != nil {
			return Context{}, err
		}
		total += size
		if total > 512<<20 {
			return Context{}, problem("validation")
		}
		if digest == "absent" && (artifact.Role == "runtime" || artifact.Role == "executable" || artifact.Role == "definitions") {
			return Context{}, problem("validation")
		}
		roles[artifact.Role] = true
		c.Artifacts[i].SHA256 = digest
	}
	if !roles["runtime"] || !roles["executable"] || !roles["definitions"] {
		return Context{}, problem("validation")
	}
	sort.Slice(c.Artifacts, func(i, j int) bool { return c.Artifacts[i].Path < c.Artifacts[j].Path })
	sort.Strings(c.Conflicts)
	return c, nil
}

func hashArtifact(ctx context.Context, path, role string) (string, int64, error) {
	// Reject symbolic-link components; callers supply reviewed concrete files.
	for current := path; current != filepath.Dir(current); current = filepath.Dir(current) {
		fi, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) && current == path {
			// Still inspect existing parent components before accepting absence.
			continue
		}
		if err != nil || fi.Mode()&os.ModeSymlink != 0 {
			// Standard macOS temp aliases are not user-controlled substitutions.
			if current == "/var" || current == "/tmp" {
				continue
			}
			return "", 0, problem("validation")
		}
	}
	before, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "absent", 0, nil
	}
	limit := int64(8 << 20)
	if role == "runtime" || role == "executable" {
		limit = 256 << 20
	}
	if err != nil || !before.Mode().IsRegular() || before.Size() > limit {
		return "", 0, problem("validation")
	}
	f, err := openArtifact(path)
	if err != nil {
		return "", 0, problem("state_corrupt")
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() {
		return "", 0, problem("state_corrupt")
	}
	h := sha256.New()
	buf := make([]byte, 128<<10)
	var size int64
	for {
		if ctx.Err() != nil {
			return "", 0, problem("state_busy")
		}
		n, err := f.Read(buf)
		size += int64(n)
		if size > limit {
			return "", 0, problem("validation")
		}
		_, _ = h.Write(buf[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", 0, problem("state_corrupt")
		}
	}
	end, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, end) || size != before.Size() || end.Size() != before.Size() || !end.ModTime().Equal(before.ModTime()) {
		return "", 0, problem("revision_conflict")
	}
	return strings.ToLower(hex.EncodeToString(h.Sum(nil))), size, nil
}
