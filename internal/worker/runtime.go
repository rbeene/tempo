package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rbeene/tempo/internal/activity"
)

type wallClock struct{}
type wallTimer struct{ t *time.Timer }

func (wallClock) Now() time.Time                 { return time.Now() }
func (wallClock) NewTimer(d time.Duration) Timer { return wallTimer{time.NewTimer(d)} }
func (t wallTimer) C() <-chan time.Time          { return t.t.C }
func (t wallTimer) Stop() bool                   { return t.t.Stop() }
func randomID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", issue("state_corrupt")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func validRuntime(r runtimeRecord) bool {
	return r.Version == 1 && (r.InstanceMode == "foreground" || r.InstanceMode == "managed") && r.BackoffNS >= 0 && r.BackoffNS <= int64(5*time.Minute) && (r.Pending == nil || (uuidPattern.MatchString(r.Pending.RequestID) && r.Pending.Limit == 20))
}
func (s *Service) Run(ctx context.Context) (err error) {
	ctx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	defer func() {
		if errors.Is(err, errWorkerStopped) {
			err = nil
		}
	}()
	h, e := acquire(ctx, s.options.StatePath+".worker")
	if e != nil {
		return e
	}
	defer h.close()
	ep, e := s.listen(h, cancelRun)
	if e != nil {
		return e
	}
	if ep != nil {
		defer ep.close()
	}
	name := filepath.Base(s.options.StatePath) + ".worker-runtime.json"
	raw, e := readPrivate(h.root, name, 65536)
	r := runtimeRecord{Version: 1, InstanceMode: s.options.InstanceMode}
	if e == nil {
		if decode(raw, &r) != nil || !validRuntime(r) {
			return issue("state_corrupt")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return issue("state_corrupt")
	}
	r.InstanceMode = s.options.InstanceMode
	recovering := r.Pending != nil
	save := func() error {
		if e := s.fault("runtime_before_save"); e != nil {
			return e
		}
		b, e := json.Marshal(r)
		if e != nil || len(b) > 65536 {
			return issue("state_corrupt")
		}
		verify := func() error {
			if e := h.verify(); e != nil {
				return e
			}
			current, e := readPrivate(h.root, name, 65536)
			if errors.Is(e, os.ErrNotExist) && raw == nil {
				return nil
			}
			if e != nil || !bytes.Equal(current, raw) {
				return issue("state_corrupt")
			}
			return nil
		}
		if e = atomicWrite(h.root, name, b, verify, s.fault); e != nil {
			return e
		}
		raw = b
		return nil
	}

	if e = save(); e != nil {
		return e
	}
	var lastRecheck time.Time
	wait := func(delay time.Duration, mode string) error {
		timer := s.options.Clock.NewTimer(delay)
		defer func() { timer.Stop() }()
		start := s.options.Clock.Now()
		var wakes <-chan struct{}
		if ep != nil {
			wakes = ep.wake
		}
		for {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-timer.C():
				return nil
			case <-wakes:
				recheck := ep.recheck.Swap(false)
				now := s.options.Clock.Now()
				if mode == "progress" {
					continue
				}
				if recheck {
					if lastRecheck.IsZero() || now.Sub(lastRecheck) >= 5*time.Second {
						lastRecheck = now
						return nil
					}
					continue
				}
				if mode == "backoff" {
					continue
				}
				// Ordinary capture can accelerate idle polling, but never create a hot loop.
				remaining := time.Second - now.Sub(start)
				if remaining <= 0 {
					return nil
				}
				timer.Stop()
				timer = s.options.Clock.NewTimer(remaining)
			}
		}
	}
	backoff := func(err error) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		category := runtimeCategory(err)
		r.FailureCategory = &category
		delay := time.Duration(r.BackoffNS)
		if category == "auth" || category == "keychain" || category == "unsupported" {
			delay = 5 * time.Minute
		} else if delay < 5*time.Second {
			delay = 5 * time.Second
		} else {
			delay *= 2
		}
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		r.BackoffNS = int64(delay)
		actual := delay
		if s.options.Jitter != nil && delay < 5*time.Minute {
			actual = s.options.Jitter(delay)
			if actual < delay*4/5 {
				actual = delay * 4 / 5
			}
			if actual > delay*6/5 {
				actual = delay * 6 / 5
			}
			if actual > 5*time.Minute {
				actual = 5 * time.Minute
			}
		}
		until := s.options.Clock.Now().Add(actual).UTC()
		r.BackoffUntil = &until
		if e := save(); e != nil {
			return e
		}
		return wait(actual, "backoff")
	}
	checkRestartDeadline := r.Pending == nil
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if r.Pending == nil {
			st, e := s.options.Sync.SyncStatus(ctx)
			if e != nil {
				if e = backoff(e); e != nil {
					return e
				}
				continue
			}
			eligible, submitting := false, false
			for _, item := range st.Items {
				eligible = eligible || item.State == "queued"
				submitting = submitting || item.State == "submitting"
			}
			if checkRestartDeadline {
				checkRestartDeadline = false
				// Required pending/orphan recovery takes precedence over ordinary scheduling.
				if !submitting && r.BackoffUntil != nil {
					delay := r.BackoffUntil.Sub(s.options.Clock.Now())
					if delay > 5*time.Minute {
						delay = 5 * time.Minute
						until := s.options.Clock.Now().Add(delay).UTC()
						r.BackoffUntil = &until
						if e = save(); e != nil {
							return e
						}
					}
					if delay > 0 {
						if e = wait(delay, "backoff"); e != nil {
							return e
						}
						continue
					}
				}
			}
			if !(st.Enabled && eligible) && !submitting {
				if e = wait(30*time.Second, "idle"); e != nil {
					return e
				}
				continue
			}
			id, e := s.options.NewRequestID()
			if e != nil || !uuidPattern.MatchString(id) {
				return issue("validation")
			}
			r.Pending = &activity.SyncRunInput{RequestID: id, Limit: 20}
			if e = save(); e != nil {
				return e
			}
			if e = s.fault("runtime_after_pending_saved"); e != nil {
				return e
			}
		}
		pass, cancel := context.WithTimeout(ctx, 2*time.Minute)
		result, e := s.options.Sync.SyncNow(pass, *r.Pending, s.options.SyncDependencies)
		cancel()
		if e != nil {
			recovering = true
			if e = backoff(e); e != nil {
				return e
			}
			continue
		}
		if e = s.fault("runtime_after_sync_return"); e != nil {
			return e
		}
		if result.RequestID != r.Pending.RequestID || result.ContractVersion != 1 || (result.State != "complete" && result.State != "interrupted") {
			return issue("state_corrupt")
		}
		if !recovering && len(result.ResolvedIDs) > 0 {
			now := s.options.Clock.Now().UTC()
			r.LastSuccess = &now
		}
		r.Pending = nil
		recovering = false
		if len(result.ResolvedIDs) > 0 {
			r.BackoffNS = 0
			r.BackoffUntil = nil
			r.FailureCategory = nil
		}
		if e = save(); e != nil {
			return e
		}
		if e = s.fault("runtime_after_result_saved"); e != nil {
			return e
		}
		if len(result.BlockedIDs) > 0 {
			st, readErr := s.options.Sync.SyncStatus(ctx)
			if readErr == nil {
				blocked := make(map[string]bool, len(result.BlockedIDs))
				for _, id := range result.BlockedIDs {
					blocked[id] = true
				}
				for _, item := range st.Items {
					if !blocked[item.ID] || item.State != "queued" || item.FailureCategory == nil {
						continue
					}
					category := runtimeCategory(&Error{Code: *item.FailureCategory})
					if readErr == nil || category == "auth" || category == "keychain" {
						readErr = &Error{Code: category}
					}
					if category == "auth" || category == "keychain" {
						break
					}
				}
			}
			if readErr != nil {
				if e = backoff(readErr); e != nil {
					return e
				}
				continue
			}
		}
		delay, mode := 30*time.Second, "idle"
		if len(result.ResolvedIDs) > 0 {
			delay, mode = time.Second, "progress"
		}
		if e = wait(delay, mode); e != nil {
			return e
		}
	}
}
func runtimeCategory(err error) string {
	var a *activity.Error
	var w *Error
	code := "network"
	if errors.As(err, &a) {
		code = a.Code
	} else if errors.As(err, &w) {
		code = w.Code
	}
	switch code {
	case "auth", "keychain", "unsupported", "state_corrupt", "state_busy", "local_write_unknown", "network", "api", "rate_limit":
		return code
	}
	return "network"
}
