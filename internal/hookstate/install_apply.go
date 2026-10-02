package hookstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/rbeene/tempo/internal/privatefs"
)

func bytesHash(b []byte) string               { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func installArtifact(id string, n int) string { return fmt.Sprintf(".install-%s-%d", id, n) }
func installInputHash(in ApplyInstallInput) string {
	return digest(struct {
		Intent      InstallIntent
		Fingerprint string
	}{in.Intent, in.Fingerprint})
}
func (s *Service) prepareInstall(p installPlan, m *metadata, in ApplyInstallInput) (installRequest, [][]byte, error) {
	r := installRequest{InputHash: installInputHash(in), Fingerprint: in.Fingerprint, Intent: in.Intent, State: "pending", Set: map[string]installation{}, Remove: []string{}, Targets: []installTarget{}, Result: HookList{RequestID: in.RequestID, ContractVersion: 1, Hooks: []HookStatus{}}}
	payloads := [][]byte{}
	for i := 0; i < len(p.Files); i += 2 {
		definition, skill := p.Files[i], p.Files[i+1]
		root := p.Root
		key := installKey(definition.Host, in.Intent.Scope, root)
		old, owned := m.Installations[key]
		next := nativeGroup(definition.Host, p.Executable)
		oldGroup := ""
		if owned {
			if old.Definition != definition.Path || old.Skill != skill.Path {
				return r, nil, problem("revision_conflict")
			}
			oldGroup = old.Group
			if skill.Hash != old.SkillHash {
				return r, nil, problem("revision_conflict")
			}
		} else if skill.Exists {
			return r, nil, problem("revision_conflict")
		}
		if in.Intent.Operation == "uninstall" {
			next = ""
		}
		updated, err := ownedJSON(definition.Host, definition.Bytes, oldGroup, next)
		if err != nil {
			return r, nil, err
		}
		skillAfter := []byte(BundledSkill)
		if in.Intent.Operation == "uninstall" {
			skillAfter = nil
			r.Remove = append(r.Remove, key)
		} else {
			r.Set[key] = installation{Host: definition.Host, Scope: in.Intent.Scope, Root: root, Definition: definition.Path, Skill: skill.Path, Group: next, SkillHash: bytesHash(skillAfter), Runtime: p.Runtimes[definition.Host], Executable: p.Executable}
		}
		for j, before := range []previewFile{definition, skill} {
			after := updated
			if j == 1 {
				after = skillAfter
			}
			afterHash := bytesHash(after)
			if j == 1 && after == nil {
				afterHash = "absent"
			}
			if !owned && in.Intent.Operation == "uninstall" {
				continue
			}
			if before.Hash == afterHash {
				continue
			}
			n := len(r.Targets)
			target := installTarget{Path: before.Path, Before: before.Hash, After: afterHash, Mode: uint32(before.Mode.Perm())}
			if before.Exists {
				target.Backup = installArtifact(in.RequestID, n) + ".before"
			}
			if afterHash != "absent" {
				target.Staged = installArtifact(in.RequestID, n) + ".after"
			}
			r.Targets = append(r.Targets, target)
			payloads = append(payloads, after)
		}
		state := "approval_required"
		if in.Intent.Operation == "uninstall" {
			state = "not_installed"
		}
		r.Result.Hooks = append(r.Result.Hooks, HookStatus{Host: definition.Host, Scope: in.Intent.Scope, Path: p.Preview.Intent.Path, RuntimeVersion: p.Runtimes[definition.Host].Version, State: state, Ordering: "unavailable", Diagnostics: []Diagnostic{}, Profile: absentInstallProfile(definition.Host, in.Intent.Scope, p.Preview.Intent.Path)})
	}

	for _, setting := range p.Settings {
		key := installKey(setting.Host, in.Intent.Scope, p.Root)
		old := m.Installations[key]
		if old.Configuration != "" && old.Configuration != setting.Path {
			return r, nil, problem("revision_conflict")
		}
		after, ownership, e := editTooltip(setting, old.Tooltip, in.Intent.Operation == "uninstall")
		if e != nil {
			return r, nil, e
		}
		if item, ok := r.Set[key]; ok {
			item.Configuration = setting.Path
			item.Tooltip = ownership
			r.Set[key] = item
		}
		afterHash := bytesHash(after)
		if after == nil {
			afterHash = "absent"
		}
		if setting.Hash == afterHash {
			continue
		}
		n := len(r.Targets)
		target := installTarget{Path: setting.Path, Before: setting.Hash, After: afterHash, Mode: uint32(setting.Mode.Perm())}
		if setting.Exists {
			target.Backup = installArtifact(in.RequestID, n) + ".before"
		}
		if afterHash != "absent" {
			target.Staged = installArtifact(in.RequestID, n) + ".after"
		}
		r.Targets = append(r.Targets, target)
		payloads = append(payloads, after)
	}
	return r, payloads, nil
}
func replayInstall(m *metadata, in ApplyInstallInput) (installRequest, bool, error) {
	if _, ok := m.Requests[in.RequestID]; ok {
		return installRequest{}, false, problem("request_conflict")
	}
	r, ok := m.InstallRequests[in.RequestID]
	if ok && r.InputHash != installInputHash(in) {
		return r, true, problem("request_conflict")
	}
	return r, ok, nil
}
func (s *Service) ApplyInstall(ctx context.Context, in ApplyInstallInput) (result HookList, err error) {
	defer func() {
		if uuid(in.RequestID) {
			result.RequestID = in.RequestID
		}
		if result.ContractVersion == 0 {
			result.ContractVersion = 1
		}
		if result.Hooks == nil {
			result.Hooks = []HookStatus{}
		}
		err = requestError(err, in.RequestID)
	}()
	if !in.Confirmed {
		return HookList{}, problem("confirmation_required")
	}
	if !uuid(in.RequestID) || !hash(in.Fingerprint) {
		return HookList{}, problem("validation")
	}
	if ctx.Err() != nil {
		return HookList{}, problem("state_busy")
	}
	m, _, err := s.read(ctx)
	if err != nil {
		return HookList{}, err
	}
	r, replay, err := replayInstall(m, in)
	if err != nil {
		return HookList{}, err
	}
	if replay && r.State == "complete" {
		err = s.update(ctx, func(current *metadata) (bool, error) {
			saved, found, e := replayInstall(current, in)
			if e != nil {
				return false, e
			}
			if !found || saved.State != "complete" {
				return false, problem("revision_conflict")
			}
			r = saved
			return false, nil
		})
		return r.Result, err
	}
	// Validation and stale-evidence rejection precede even first metadata mkdir.
	if !replay {
		p, e := s.planInstall(ctx, in.Intent, m)
		if e != nil {
			return HookList{}, e
		}
		if p.Preview.Fingerprint != in.Fingerprint {
			return HookList{}, problem("revision_conflict")
		}
		if _, _, e = s.prepareInstall(p, m, in); e != nil {
			return HookList{}, e
		}
	}
	l, _, err := s.acquire(ctx, true)
	if err != nil {
		return HookList{}, err
	}
	defer l.close()
	m, _, err = decodeMetadata(l)
	if err != nil {
		return HookList{}, err
	}
	r, replay, err = replayInstall(m, in)
	if err != nil {
		return HookList{}, err
	}
	if replay && r.State == "complete" {
		return r.Result, s.syncVisible(l)
	}
	var payloads [][]byte
	if !replay {
		for _, other := range m.InstallRequests {
			if other.State == "pending" {
				return HookList{}, problem("revision_conflict")
			}
		}
		p, e := s.planInstall(ctx, in.Intent, m)
		if e != nil {
			return HookList{}, e
		}
		if p.Preview.Fingerprint != in.Fingerprint {
			return HookList{}, problem("revision_conflict")
		}
		r, payloads, err = s.prepareInstall(p, m, in)
		if err != nil {
			return HookList{}, err
		}
	}
	locks, err := s.lockInstallTargets(ctx, r.Targets)
	if err != nil {
		return HookList{}, err
	}
	defer closeTargetLocks(locks)
	// All targets are checked together before any next replacement, including on
	// interrupted replay. Never absorb an intervening edit as a new base.
	if err = checkInstallTargets(ctx, r, replay); err != nil {
		return HookList{}, err
	}
	if !replay {
		for n, t := range r.Targets {
			if t.Backup != "" {
				if s.fault("install_backup") != nil {
					return HookList{}, problem("state_corrupt")
				}
				before, e := readInstallFile(ctx, "", t.Path, "")
				if e != nil || before.Hash != t.Before {
					return HookList{}, problem("revision_conflict")
				}
				if e = s.stageInstall(l, t.Backup, before.Bytes, false); e != nil {
					return HookList{}, e
				}
			}
			if t.Staged != "" {
				if err = s.stageInstall(l, t.Staged, payloads[n], true); err != nil {
					return HookList{}, err
				}
			}
		}
		if s.fault("install_intent") != nil {
			return HookList{}, problem("state_corrupt")
		}
		if m.InstallRequests == nil {
			m.InstallRequests = map[string]installRequest{}
		}
		m.InstallRequests[in.RequestID] = r
		if err = s.writeMetadata(ctx, l, m); err != nil {
			return HookList{}, err
		}
	}
	crossed := false
	for _, t := range r.Targets {
		current, e := readInstallFile(ctx, "", t.Path, "")
		if e != nil {
			if crossed {
				return HookList{}, problem("local_write_unknown")
			}
			return HookList{}, e
		}
		if current.Hash == t.After {
			crossed = true
			if err = s.syncInstallTarget(locks, t); err != nil {
				return HookList{}, err
			}
			continue
		}
		if current.Hash != t.Before {
			if crossed {
				return HookList{}, problem("local_write_unknown")
			}
			return HookList{}, problem("revision_conflict")
		}
		if s.fault("install_target_rename") != nil {
			if crossed {
				return HookList{}, problem("local_write_unknown")
			}
			return HookList{}, problem("state_corrupt")
		}
		if err = s.publishInstall(ctx, l, locks, t); err != nil {
			if crossed {
				return HookList{}, problem("local_write_unknown")
			}
			return HookList{}, err
		}
		crossed = true
	}
	if s.fault("install_complete") != nil {
		if crossed {
			return HookList{}, problem("local_write_unknown")
		}
		return HookList{}, problem("state_corrupt")
	}
	if m.Installations == nil {
		m.Installations = map[string]installation{}
	}
	for key, value := range r.Set {
		m.Installations[key] = value
	}
	for _, key := range r.Remove {
		delete(m.Installations, key)
	}
	// Any installation operation invalidates affected retained policy; a later
	// explicit declaration must inspect the resulting effective profile again.
	for key, profile := range m.Profiles {
		for _, row := range r.Result.Hooks {
			if profile.Context.Host == row.Host && profile.Context.Scope == row.Scope && (row.Scope == "user" || profile.Context.Path == row.Path) {
				profile.State = "invalidated"
				profile.CaptureEligible = false
				profile.DiagnosticCode = "profile_invalidated"
				n, _ := number(profile.Revision)
				profile.Revision = fmt.Sprint(n + 1)
				profile.Fingerprint = profileFingerprint(profile.Context, profile.Revision)
				m.Profiles[key] = profile
			}
		}
	}
	r.State = "complete"
	m.InstallRequests[in.RequestID] = r
	if err = s.writeMetadata(ctx, l, m); err != nil {
		if crossed {
			return HookList{}, problem("local_write_unknown")
		}
		return HookList{}, err
	}
	return r.Result, nil
}
func (s *Service) stageInstall(l *lockedStore, name string, b []byte, after bool) error {
	f, err := openNoFollow(l.root, name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		v, e := readInstallFile(context.Background(), "", filepath.Join(l.dir, name), "")
		if e != nil || !bytes.Equal(v.Bytes, b) || v.Mode != 0600 {
			return problem("state_corrupt")
		}
		existing, e := openNoFollow(l.root, name, os.O_RDONLY, 0)
		if e != nil {
			return problem("state_corrupt")
		}
		e = existing.Sync()
		existing.Close()
		if e != nil {
			return problem("state_corrupt")
		}
		return nil
	}
	if err != nil {
		return problem("state_corrupt")
	}
	defer f.Close()
	if _, err = f.Write(b); err == nil && after {
		err = s.fault("install_stage_sync")
	}
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		_ = l.root.Remove(name)
		return problem("state_corrupt")
	}
	return nil
}
func checkInstallTargets(ctx context.Context, r installRequest, replay bool) error {
	for _, t := range r.Targets {
		v, e := readInstallFile(ctx, "", t.Path, "")
		if e != nil {
			return e
		}
		if v.Hash != t.Before && (!replay || v.Hash != t.After) {
			return problem("revision_conflict")
		}
	}
	return nil
}

type targetLock struct {
	root *os.Root
	file *os.File
	info os.FileInfo
	dir  string
}

func closeTargetLocks(locks []targetLock) {
	for i := len(locks) - 1; i >= 0; i-- {
		unlockFile(locks[i].file)
		locks[i].file.Close()
		locks[i].root.Close()
	}
}
func (s *Service) lockInstallTargets(ctx context.Context, targets []installTarget) ([]targetLock, error) {
	dirs := map[string]bool{}
	for _, t := range targets {
		dirs[filepath.Dir(t.Path)] = true
	}
	order := []string{}
	for dir := range dirs {
		order = append(order, dir)
	}
	sort.Strings(order)
	out := []targetLock{}
	fail := func(e error) ([]targetLock, error) { closeTargetLocks(out); return nil, e }
	for _, dir := range order {
		root, info, e := privatefs.OpenServiceDirectory(dir, true)
		if e != nil {
			return fail(problem("state_corrupt"))
		}
		f, e := openNoFollow(root, ".tempo-install.lock", os.O_RDWR|os.O_CREATE, 0600)
		if e != nil {
			root.Close()
			return fail(problem("state_corrupt"))
		}
		out = append(out, targetLock{root, f, info, dir})
		fi, e := f.Stat()
		if e != nil || !privateInfo(fi, false) {
			return fail(problem("state_corrupt"))
		}
		timeout := s.options.LockTimeout
		if timeout == 0 {
			timeout = 250 * time.Millisecond
		}
		limit, cancel := context.WithTimeout(ctx, timeout)
		for {
			if limit.Err() != nil {
				cancel()
				return fail(problem("state_busy"))
			}
			ok, e := tryLockFile(f)
			if e != nil {
				cancel()
				return fail(problem("state_corrupt"))
			}
			if ok {
				break
			}
			select {
			case <-limit.Done():
			case <-time.After(5 * time.Millisecond):
			}
		}
		cancel()
	}
	return out, nil
}
func (s *Service) publishInstall(ctx context.Context, l *lockedStore, locks []targetLock, t installTarget) error {
	var lock *targetLock
	for i := range locks {
		if locks[i].dir == filepath.Dir(t.Path) {
			lock = &locks[i]
			break
		}
	}
	if lock == nil {
		return problem("state_corrupt")
	}
	fi, e := os.Lstat(lock.dir)
	if e != nil || fi.Mode()&os.ModeSymlink != 0 || !os.SameFile(fi, lock.info) {
		return problem("state_corrupt")
	}
	lockInfo, e := lock.file.Stat()
	namedLock, nameErr := lock.root.Lstat(".tempo-install.lock")
	if e != nil || nameErr != nil || !os.SameFile(lockInfo, namedLock) || !privateInfo(namedLock, false) {
		return problem("state_corrupt")
	}
	current, e := readInstallFile(ctx, "", t.Path, "")
	if e != nil || current.Hash != t.Before {
		return problem("revision_conflict")
	}
	name := filepath.Base(t.Path)
	if t.After == "absent" {
		if e = lock.root.Remove(name); e != nil {
			return problem("local_write_unknown")
		}
	} else {
		staged, e := readInstallFile(ctx, "", filepath.Join(l.dir, t.Staged), "")
		if e != nil || staged.Hash != t.After {
			return problem("state_corrupt")
		}
		tmp := ".tempo-install-" + randomID() + ".tmp"
		f, e := openNoFollow(lock.root, tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return problem("state_corrupt")
		}
		defer lock.root.Remove(tmp)
		_, e = f.Write(staged.Bytes)
		if e == nil {
			e = f.Chmod(os.FileMode(t.Mode))
		}
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e != nil || ce != nil {
			return problem("state_corrupt")
		}
		if e = lock.root.Rename(tmp, name); e != nil {
			return problem("local_write_unknown")
		}
	}
	return s.syncInstallTarget(locks, t)
}
func (s *Service) syncInstallTarget(locks []targetLock, t installTarget) error {
	var lock *targetLock
	for i := range locks {
		if locks[i].dir == filepath.Dir(t.Path) {
			lock = &locks[i]
			break
		}
	}
	if lock == nil {
		return problem("state_corrupt")
	}
	fi, e := os.Lstat(lock.dir)
	if e != nil || fi.Mode()&os.ModeSymlink != 0 || !os.SameFile(fi, lock.info) {
		return problem("local_write_unknown")
	}
	if t.After != "absent" {
		f, e := openNoFollow(lock.root, filepath.Base(t.Path), os.O_RDONLY, 0)
		if e != nil {
			return problem("local_write_unknown")
		}
		e = f.Sync()
		f.Close()
		if e != nil {
			return problem("local_write_unknown")
		}
	}
	if s.fault("install_target_sync") != nil {
		return problem("local_write_unknown")
	}
	d, e := lock.root.Open(".")
	if e != nil {
		return problem("local_write_unknown")
	}
	e = d.Sync()
	d.Close()
	if e != nil {
		return problem("local_write_unknown")
	}
	return nil
}
