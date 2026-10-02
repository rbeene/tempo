package activity

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

type syncRunLock struct{ held *lockedStore }

func (s *Service) acquireSyncLock(ctx context.Context) (*syncRunLock, error) {
	p, e := s.store.location()
	if e != nil {
		return nil, e
	}
	root, info, e := secureDirectory(filepath.Dir(p), false)
	if os.IsNotExist(e) {
		return nil, syncRequired("binding")
	}
	if e != nil {
		return nil, failureFrom(e)
	}
	fi, e := root.Lstat(filepath.Base(p))
	if e != nil || !privateInfo(fi, false) {
		root.Close()
		return nil, failure("state_corrupt")
	}
	name := filepath.Base(p) + ".sync"
	f, e := openNoFollow(root, name+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		root.Close()
		return nil, failure("state_corrupt")
	}
	l := &lockedStore{root: root, lock: f, dir: filepath.Dir(p), dirInfo: info, name: name}
	if e = l.verify(); e != nil {
		l.close()
		return nil, e
	}
	timeout := s.store.timeout
	if timeout == 0 {
		timeout = 250 * time.Millisecond
	}
	if timeout < 0 || timeout > time.Second {
		l.close()
		return nil, failure("validation")
	}
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if bounded.Err() != nil {
			l.close()
			return nil, failure("state_busy")
		}
		ok, e := tryLockFile(f)
		if e != nil {
			l.close()
			return nil, failure("state_corrupt")
		}
		if ok {
			if e = l.verify(); e != nil {
				l.close()
				return nil, e
			}
			return &syncRunLock{held: l}, nil
		}
		select {
		case <-bounded.Done():
		case <-time.After(5 * time.Millisecond):
		}
	}
}
func (l *syncRunLock) close() {
	if l != nil && l.held != nil {
		l.held.close()
		l.held = nil
	}
}
func (l *syncRunLock) verify() error {
	if l == nil || l.held == nil {
		return failure("state_corrupt")
	}
	return l.held.verify()
}
