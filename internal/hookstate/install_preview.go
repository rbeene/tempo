package hookstate

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const maxConfigBytes = 8 << 20

// BundledSkill is installed as an owned instruction-only file for either host.
//
//go:embed skill/SKILL.md
var BundledSkill string

type previewFile struct {
	Host, Path, Role, Hash string
	Bytes                  []byte
	Mode                   os.FileMode
	Exists                 bool
}
type installPlan struct {
	Preview    HookPreview
	Files      []previewFile
	Runtimes   map[string]Runtime
	Executable string
	Root       string
	Settings   []previewFile
}

func (s *Service) PreviewInstall(ctx context.Context, in InstallIntent) (HookPreview, error) {
	if ctx.Err() != nil {
		return HookPreview{}, problem("state_busy")
	}
	m, _, err := s.read(ctx)
	if err != nil {
		return HookPreview{}, err
	}
	p, err := s.planInstall(ctx, in, m)
	return p.Preview, err
}

func canonicalInstallPath(path string, exists bool) (string, error) {
	if !safeText(path, 4096) {
		return "", problem("validation")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", problem("validation")
	}
	// Resolve only the standard Darwin temporary aliases, never user symlinks.
	if runtime.GOOS == "darwin" && (strings.HasPrefix(path, "/var/") || strings.HasPrefix(path, "/tmp/")) {
		path = "/private" + path
	}
	if err := safeTargetPath(path); err != nil {
		return "", err
	}
	if exists {
		fi, e := os.Stat(path)
		if e != nil || !fi.IsDir() {
			return "", problem("binding_unavailable")
		}
	}
	return filepath.Clean(path), nil
}

func safeTargetPath(path string) error {
	for p := path; p != filepath.Dir(p); p = filepath.Dir(p) {
		fi, err := os.Lstat(p)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || fi.Mode()&os.ModeSymlink != 0 {
			return problem("validation")
		}
		if p != path && !fi.IsDir() {
			return problem("validation")
		}
	}
	return nil
}

func readInstallFile(ctx context.Context, host, path, role string) (previewFile, error) {
	v := previewFile{Host: host, Path: path, Role: role, Hash: "absent", Mode: 0600}
	if ctx.Err() != nil {
		return v, problem("state_busy")
	}
	if err := safeTargetPath(path); err != nil {
		return v, err
	}
	before, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return v, nil
	}
	if err != nil || !before.Mode().IsRegular() || before.Size() > maxConfigBytes {
		return v, problem("validation")
	}
	f, err := openArtifact(path)
	if err != nil {
		return v, problem("state_corrupt")
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !os.SameFile(before, fi) {
		return v, problem("revision_conflict")
	}
	b, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil || len(b) > maxConfigBytes {
		return v, problem("validation")
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return v, problem("revision_conflict")
	}
	if ctx.Err() != nil {
		return v, problem("state_busy")
	}
	h := sha256.Sum256(b)
	v.Bytes, v.Hash, v.Mode, v.Exists = b, hex.EncodeToString(h[:]), before.Mode().Perm(), true
	return v, nil
}

func validateHookDocument(b []byte) error {
	if !strictJSON(b) {
		return problem("validation")
	}
	var root map[string]json.RawMessage
	if json.Unmarshal(b, &root) != nil || root == nil {
		return problem("validation")
	}
	if raw, ok := root["hooks"]; ok {
		var events map[string]json.RawMessage
		if json.Unmarshal(raw, &events) != nil || events == nil {
			return problem("validation")
		}
		for _, raw := range events {
			var groups []json.RawMessage
			if json.Unmarshal(raw, &groups) != nil || groups == nil {
				return problem("validation")
			}
			for _, raw := range groups {
				var group map[string]json.RawMessage
				if json.Unmarshal(raw, &group) != nil || group == nil {
					return problem("validation")
				}
				var handlers []map[string]json.RawMessage
				if json.Unmarshal(group["hooks"], &handlers) != nil || handlers == nil {
					return problem("validation")
				}
				for _, handler := range handlers {
					var typ string
					if handler == nil || json.Unmarshal(handler["type"], &typ) != nil || typ == "" {
						return problem("validation")
					}
				}
			}
		}
	}
	return nil
}

func (s *Service) discover(ctx context.Context, host string) (Runtime, error) {
	if s.options.DiscoverRuntime != nil {
		return s.options.DiscoverRuntime(ctx, host)
	}
	path, err := exec.LookPath(host)
	if err != nil {
		return Runtime{}, problem("unsupported_contract")
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return Runtime{}, problem("unsupported_contract")
	}
	limited, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cmd := exec.CommandContext(limited, path, "--version")
	cmd.WaitDelay = 100 * time.Millisecond
	var output cappedInventory
	cmd.Stdout, cmd.Stderr = &output, io.Discard
	if cmd.Run() != nil {
		return Runtime{}, problem("unsupported_contract")
	}
	fields := strings.Fields(string(output))
	version := ""
	for _, field := range fields {
		if field == "0.159.3" || field == "2.1.286" {
			version = field
		}
	}
	return Runtime{Path: path, Version: version, Surface: "local"}, nil
}

type cappedInventory []byte

func (b *cappedInventory) Write(p []byte) (int, error) {
	if len(*b)+len(p) > 1024 {
		return 0, problem("validation")
	}
	*b = append(*b, p...)
	return len(p), nil
}

func (s *Service) planInstall(ctx context.Context, in InstallIntent, m *metadata) (installPlan, error) {
	p := installPlan{Preview: HookPreview{ContractVersion: 1, Changes: []HookChange{}, ApprovalSteps: []string{}}, Runtimes: map[string]Runtime{}}
	if ctx.Err() != nil {
		return p, problem("state_busy")
	}
	if in.Host != "codex" && in.Host != "claude" && in.Host != "both" || in.Scope != "user" && in.Scope != "project" || in.Operation != "install" && in.Operation != "repair" && in.Operation != "uninstall" {
		return p, problem("validation")
	}
	home := s.options.HomeDir
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return p, problem("validation")
		}
	}
	root := home
	if in.Scope == "project" {
		if in.Path == "" {
			return p, problem("validation")
		}
		root = in.Path
	}
	root, err := canonicalInstallPath(root, in.Scope == "project")
	if err != nil {
		return p, err
	}
	if in.Scope == "project" {
		in.Path = root
	} else if in.Path != "" {
		in.Path, err = canonicalInstallPath(in.Path, true)
		if err != nil {
			return p, err
		}
	}
	p.Preview.Intent = in
	p.Root = root
	executable := s.options.Executable
	if executable == "" {
		executable, err = os.Executable()
		if err != nil {
			return p, problem("validation")
		}
	}
	if !filepath.IsAbs(executable) || !safeText(executable, 4096) {
		return p, problem("validation")
	}
	exeHash, _, err := hashArtifact(ctx, executable, "executable")
	if err != nil || exeHash == "absent" {
		if err == nil {
			err = problem("validation")
		}
		return p, err
	}
	p.Executable = executable
	hosts := []string{in.Host}
	if in.Host == "both" {
		hosts = []string{"codex", "claude"}
	}
	evidence := []string{exeHash, s.options.BuildVersion, BundledSkill, digest(m)}
	for _, host := range hosts {
		r, err := s.discover(ctx, host)
		if err != nil {
			return p, failureFrom(err)
		}
		if r.Surface != "local" || host == "codex" && r.Version != "0.159.3" || host == "claude" && r.Version != "2.1.286" {
			return p, problem("unsupported_contract")
		}
		rh, _, err := hashArtifact(ctx, r.Path, "runtime")
		if err != nil || rh == "absent" {
			if err == nil {
				err = problem("validation")
			}
			return p, err
		}
		p.Runtimes[host] = r
		evidence = append(evidence, digest(r), rh)
		config, skill := filepath.Join(root, ".codex", "hooks.json"), filepath.Join(root, ".agents", "skills", "tempo", "SKILL.md")
		if host == "codex" && in.Scope == "project" {
			g, e := discoverGit(ctx, root)
			if e != nil {
				return p, e
			}
			config = filepath.Join(g.HookRoot, ".codex", "hooks.json")
			evidence = append(evidence, digest(g.Artifacts))
			if g.HookRoot != root {
				p.Preview.ApprovalSteps = append(p.Preview.ApprovalSteps, "Codex linked worktree: hook definitions are shared from "+g.HookRoot+"; selected worktree skill and tooltip configuration stay local. Review the explicit cross-checkout destination.")
			}
		}
		if host == "claude" {
			config, skill = filepath.Join(root, ".claude", "settings.json"), filepath.Join(root, ".claude", "skills", "tempo", "SKILL.md")
		}
		for i, path := range []string{config, skill} {
			role := "definitions"
			if i == 1 {
				role = "skill"
			}
			file, err := readInstallFile(ctx, host, path, role)
			if err != nil {
				return p, err
			}
			if i == 0 && file.Exists {
				if err = validateHookDocument(file.Bytes); err != nil {
					return p, err
				}
			}
			p.Files = append(p.Files, file)
			evidence = append(evidence, path, file.Hash, file.Mode.String())
			operation := "insert_owned"
			if in.Operation == "uninstall" {
				operation = "remove_owned"
			} else if file.Exists {
				operation = "replace_owned"
			}
			p.Preview.Changes = append(p.Preview.Changes, HookChange{Host: host, Path: path, Operation: operation, SafeSummary: "Tempo-owned " + role + ": " + quoteCommand(executable) + " hook " + host + " --input-stdin; preserve unrelated content."})
		}
		if host == "codex" {
			setting, e := readInstallFile(ctx, host, filepath.Join(root, ".codex", "config.toml"), "configuration")
			if e != nil {
				return p, e
			}
			if _, e = inspectTooltip(setting.Bytes); e != nil {
				return p, e
			}
			p.Settings = append(p.Settings, setting)
			evidence = append(evidence, setting.Path, setting.Hash, setting.Mode.String())
			operation := "replace_owned"
			if !setting.Exists {
				operation = "insert_owned"
			}
			if in.Operation == "uninstall" {
				operation = "remove_owned"
			}
			p.Preview.Changes = append(p.Preview.Changes, HookChange{Host: host, Path: setting.Path, Operation: operation, SafeSummary: "Set tui.show_tooltips = false in this scope to disable startup tooltips and prevent the normal NUX counter writer. Uninstall restores only an unchanged owned preference; all configuration remains fingerprinted."})
			p.Preview.ApprovalSteps = append(p.Preview.ApprovalSteps, "Codex: trust this project normally, reload, and review effective show_tooltips=false. Other contexts or CLI/managed overrides can still change global configuration and invalidate the retained profile.")
		}
		p.Preview.ApprovalSteps = append(p.Preview.ApprovalSteps, host+": review normal workspace and hook trust in /hooks; reload the host after changes. Installation does not prove delivery or grant capture policy.")
	}
	p.Preview.Fingerprint = digest(struct {
		Intent   InstallIntent
		Evidence []string
	}{in, evidence})
	return p, nil
}
