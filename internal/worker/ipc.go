package worker

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/privatefs"
)

var errWorkerStopped = errors.New("worker stopped")

type endpoint struct {
	conn    *net.UnixConn
	wake    chan struct{}
	recheck atomic.Bool
	done    chan struct{}
	close   func()
}

func (s *Service) listen(h *heldFile, cancel context.CancelCauseFunc) (*endpoint, error) {
	path := s.options.StatePath + ".worker.sock"
	name := filepath.Base(path)
	if e := h.verify(); e != nil {
		return nil, e
	}
	if old, e := h.root.Lstat(name); e == nil {
		if !privatefs.SocketInfo(old) {
			return nil, issue("state_corrupt")
		}
		if h.root.Remove(name) != nil {
			return nil, issue("state_corrupt")
		}
	} else if !os.IsNotExist(e) {
		return nil, issue("state_corrupt")
	}
	conn, e := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if e != nil {
		return nil, nil
	} // Polling remains available when the local socket cannot bind.
	info, e := h.root.Lstat(name)
	if e != nil || !privatefs.SocketInfo(info) || h.verify() != nil {
		conn.Close()
		return nil, issue("state_corrupt")
	}
	_ = conn.SetReadBuffer(1024)
	ep := &endpoint{conn: conn, wake: make(chan struct{}, 1), done: make(chan struct{})}
	ep.close = func() {
		conn.Close()
		<-ep.done
		if current, e := h.root.Lstat(name); e == nil && os.SameFile(current, info) {
			_ = h.root.Remove(name)
		}
	}
	go func() {
		defer close(ep.done)
		var buf [16]byte
		for {
			n, _, e := conn.ReadFromUnix(buf[:])
			if e != nil {
				return
			}
			switch string(buf[:n]) {
			case "stop":
				cancel(errWorkerStopped)
				continue
			case "recheck":
				ep.recheck.Store(true)
			case "wake":
			default:
				continue
			}
			select {
			case ep.wake <- struct{}{}:
			default:
			}
		}
	}()
	return ep, nil
}
func Notify(ctx context.Context, path string, kind NotifyKind) error {
	payload := "wake"
	if kind == Recheck {
		payload = "recheck"
	} else if kind != Wake {
		return issue("validation")
	}
	return sendNotification(ctx, path, payload)
}
func sendNotification(ctx context.Context, path, payload string) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	path, e := activity.ResolveStatePath(path)
	if e != nil {
		return issue("validation")
	}
	root, info, e := privatefs.OpenDirectory(filepath.Dir(path), false)
	if e != nil {
		return issue("manager")
	}
	defer root.Close()
	socket := filepath.Base(path) + ".worker.sock"
	fi, e := root.Lstat(socket)
	if e != nil || !privatefs.SocketInfo(fi) {
		return issue("manager")
	}
	d := net.Dialer{}
	conn, e := d.DialContext(ctx, "unixgram", path+".worker.sock")
	if e != nil {
		return issue("manager")
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	_ = conn.SetWriteDeadline(deadline)
	now, e := os.Lstat(filepath.Dir(path))
	again, e2 := root.Lstat(socket)
	if e != nil || e2 != nil || !os.SameFile(info, now) || !os.SameFile(fi, again) {
		return issue("state_corrupt")
	}
	if _, e = conn.Write([]byte(payload)); e != nil {
		return issue("manager")
	}
	return nil
}
