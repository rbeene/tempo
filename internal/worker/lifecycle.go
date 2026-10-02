package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rbeene/tempo/internal/privatefs"
)

func (s *Service) lifecycle(ctx context.Context, action string, d definition) error {
	if s.options.Platform == "linux" {
		return s.lifecycleLinux(ctx, action, d)
	}
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	loaded, e := s.managerLoaded(ctx, d.ServiceID)
	if e != nil {
		return e
	}
	domain := fmt.Sprintf("gui/%d", s.options.UID)
	target := domain + "/" + d.ServiceID
	if action == "start" {
		if _, e = s.manager(ctx, "enable", target); e != nil {
			return e
		}
		if e = s.fault("control_after_manager_effect"); e != nil {
			return e
		}
		if !loaded {
			_, e = s.manager(ctx, "bootstrap", domain, d.Path)
		}
		return e
	}
	if _, e = s.manager(ctx, "disable", target); e != nil {
		return e
	}
	if e = s.fault("control_after_disabled"); e != nil {
		return e
	}
	r, e := s.manager(ctx, "print-disabled", domain)
	if e != nil {
		return e
	}
	if !disabledEvidence(r.Stdout, d.ServiceID) {
		return issue("manager")
	}
	if loaded {
		if _, e = s.manager(ctx, "bootout", target); e != nil {
			return e
		}
	}
	loaded, e = s.managerLoaded(ctx, d.ServiceID)
	if e != nil {
		return e
	}
	if loaded {
		return issue("manager")
	}
	if e = s.stopInstance(ctx); e != nil {
		return e
	}
	if action == "uninstall" {
		return s.removeDefinition(d)
	}
	return nil
}
func (s *Service) stopInstance(ctx context.Context) error {
	live, e := s.instanceLive()
	if e != nil {
		return e
	}
	if live {
		if e = sendNotification(ctx, s.options.StatePath, "stop"); e != nil {
			return e
		}
	}
	for live {
		select {
		case <-ctx.Done():
			return issue("manager")
		case <-time.After(10 * time.Millisecond):
		}
		live, e = s.instanceLive()
		if e != nil {
			return e
		}
	}
	return nil
}

func (s *Service) instanceLive() (bool, error) {
	root, _, e := privatefs.OpenDirectory(filepath.Dir(s.options.StatePath), false)
	if errors.Is(e, os.ErrNotExist) {
		return false, nil
	}
	if e != nil {
		return false, issue("state_corrupt")
	}
	defer root.Close()
	f, e := privatefs.OpenNoFollow(root, filepath.Base(s.options.StatePath)+".worker.lock", os.O_RDONLY, 0)
	if errors.Is(e, os.ErrNotExist) {
		return false, nil
	}
	if e != nil {
		return false, issue("state_corrupt")
	}
	defer f.Close()
	fi, e := f.Stat()
	if e != nil || !privatefs.PrivateInfo(fi, false) {
		return false, issue("state_corrupt")
	}
	free, e := privatefs.TryLock(f)
	if e != nil {
		return false, issue("state_corrupt")
	}
	if free {
		privatefs.Unlock(f)
	}
	return !free, nil
}
func (s *Service) removeDefinition(d definition) error {
	if e := s.verifyDefinition(d); e != nil {
		return e
	}
	root, info, e := privatefs.OpenServiceDirectory(s.options.ServiceDir, false)
	if e != nil {
		return issue("revision_conflict")
	}
	defer root.Close()
	now, e := os.Lstat(s.options.ServiceDir)
	if e != nil || !os.SameFile(info, now) {
		return issue("revision_conflict")
	}
	if e = s.verifyDefinition(d); e != nil {
		return e
	}
	if e = root.Remove(filepath.Base(d.Path)); e != nil {
		return issue("local_write_unknown")
	}
	dir, e := root.Open(".")
	if e != nil {
		return issue("local_write_unknown")
	}
	defer dir.Close()
	if e = dir.Sync(); e != nil {
		return issue("local_write_unknown")
	}
	return nil
}

// An exact pending uninstall can finish after its file removal was made visible.
// A different request or any replacement file remains a revision conflict.
func (s *Service) recoverRemoval(ctx context.Context, d definition) error {
	if e := s.requireUnpublished(d); e != nil {
		return e
	}
	if s.options.Platform == "linux" {
		state, e := s.inspectUnit(ctx, d)
		if e != nil {
			return e
		}
		if state.active || state.enabled {
			return issue("manager")
		}
		if _, e = s.manager(ctx, "daemon-reload"); e != nil {
			return e
		}
		state, e = s.inspectUnit(ctx, d)
		if e != nil {
			return e
		}
		if state.loaded {
			return issue("manager")
		}
	} else {
		loaded, e := s.managerLoaded(ctx, d.ServiceID)
		if e != nil {
			return e
		}
		if loaded {
			return issue("manager")
		}
		result, e := s.manager(ctx, "print-disabled", fmt.Sprintf("gui/%d", s.options.UID))
		if e != nil {
			return e
		}
		if !disabledEvidence(result.Stdout, d.ServiceID) {
			return issue("manager")
		}
	}
	live, e := s.instanceLive()
	if e != nil {
		return e
	}
	if live {
		return issue("manager")
	}
	root, info, e := privatefs.OpenServiceDirectory(s.options.ServiceDir, false)
	if e != nil {
		return issue("revision_conflict")
	}
	defer root.Close()
	now, e := os.Lstat(s.options.ServiceDir)
	if e != nil || !os.SameFile(info, now) {
		return issue("revision_conflict")
	}
	if e = s.requireUnpublished(d); e != nil {
		return e
	}
	dir, e := root.Open(".")
	if e != nil {
		return issue("local_write_unknown")
	}
	defer dir.Close()
	if e = dir.Sync(); e != nil {
		return issue("local_write_unknown")
	}
	return nil
}
