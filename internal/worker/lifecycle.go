package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	root, dirInfo, e := privatefs.OpenDirectory(filepath.Dir(s.options.StatePath), false)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return issue("state_corrupt")
	}
	defer root.Close()
	name := filepath.Base(s.options.StatePath) + ".worker"
	lock, e := privatefs.OpenNoFollow(root, name+".lock", os.O_RDONLY, 0)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return issue("state_corrupt")
	}
	defer lock.Close()
	// Retain the observed lock and directory identities. Reopening the namespace
	// between retries could select a replacement worker instead of this owner.
	verifyOwner := func() error {
		dir, e := os.Lstat(filepath.Dir(s.options.StatePath))
		fi, e2 := lock.Stat()
		named, e3 := root.Lstat(name + ".lock")
		if e != nil || e2 != nil || e3 != nil || !privatefs.PrivateInfo(dir, true) || !os.SameFile(dirInfo, dir) || !privatefs.PrivateInfo(fi, false) || !os.SameFile(fi, named) {
			return issue("state_corrupt")
		}
		return nil
	}
	var socketInfo os.FileInfo
	var conn net.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	sent := false
	for {
		if e = verifyOwner(); e != nil {
			return e
		}
		free, e := privatefs.TryLock(lock)
		if e != nil {
			return issue("state_corrupt")
		}
		if free {
			privatefs.Unlock(lock)
			return verifyOwner()
		}
		if ctx.Err() != nil {
			return issue("manager")
		}
		fi, e := root.Lstat(name + ".sock")
		present := e == nil
		if socketInfo == nil {
			if e != nil || !privatefs.SocketInfo(fi) {
				return issue("manager")
			}
			socketInfo = fi
		} else if present {
			if !privatefs.SocketInfo(fi) || !os.SameFile(socketInfo, fi) {
				return issue("state_corrupt")
			}
		} else if !os.IsNotExist(e) {
			return issue("state_corrupt")
		}
		// Endpoint removal can precede release of the instance lock during
		// cleanup. Wait for release without selecting another endpoint.
		if !sent && present {
			attempt, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
			if conn == nil {
				d := net.Dialer{}
				conn, e = d.DialContext(attempt, "unixgram", s.options.StatePath+".worker.sock")
			}
			if e == nil && attempt.Err() == nil {
				if e = verifyOwner(); e != nil {
					cancel()
					return e
				}
				again, statErr := root.Lstat(name + ".sock")
				if statErr != nil || !privatefs.SocketInfo(again) || !os.SameFile(socketInfo, again) {
					cancel()
					return issue("state_corrupt")
				}
				deadline, _ := attempt.Deadline()
				if e = conn.SetWriteDeadline(deadline); e == nil {
					_, e = conn.Write([]byte("stop"))
				}
				if e == nil {
					sent = true
				}
			}
			cancel()
		}
		// Stop is an idempotent local control. Unlike Wake/Recheck hints, a
		// failed send is retried only against the pinned owner and endpoint,
		// within the existing lifecycle/caller budget. A successful send is
		// never repeated; completion still requires the owner lock to end.
		select {
		case <-ctx.Done():
			return issue("manager")
		case <-time.After(10 * time.Millisecond):
		}
	}
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
