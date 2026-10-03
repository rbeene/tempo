//go:build darwin || linux

package worker

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Linux AF_UNIX ignores SO_RCVBUF. Its unconnected receiver backlog is a
// packet count copied from max_dgram_qlen when the socket is created. The
// kernel considers the queue full only above that count. After the first
// admitted Wake, qlen+1 additional attempts include the first blocked send.
func qaStopAdmitPressure(t *testing.T, statePath string) int {
	t.Helper()
	limit := 40
	expectedQueued := 0
	if runtime.GOOS == "linux" {
		raw, err := os.ReadFile("/proc/sys/net/unix/max_dgram_qlen")
		if err != nil {
			t.Fatal("Linux Unix datagram queue threshold unavailable for pressure fixture")
		}
		qlen, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil || qlen < 0 || qlen > 4096 {
			t.Fatal("Linux Unix datagram queue threshold outside bounded fixture range 0..4096")
		}
		limit = qlen + 1
		expectedQueued = qlen + 1
		t.Logf("Linux Unix datagram queue threshold=%d; additional pressure attempt cap=%d", qlen, limit)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accepted := 1 // The caller already admitted one Wake on this receiver.
	for i := 0; i < limit; i++ {
		err := Notify(ctx, statePath, Wake)
		if ctx.Err() != nil {
			t.Fatal("bounded fixture pressure setup deadline expired")
		}
		if err != nil {
			qaWorkerCode(t, err, "manager")
			if expectedQueued != 0 && accepted != expectedQueued {
				t.Fatal("Linux fixture send failed before its declared receiver queue filled")
			}
			t.Logf("owned paused receiver accepted %d notifications before bounded pressure", accepted)
			return accepted
		}
		accepted++
	}
	t.Fatal("bounded fixture did not establish socket pressure")
	return 0
}

// An independently controlled synthetic foreground owner holds the same real
// worker lock and socket namespace as Run. No product seam or manager is used.
func TestQAStopControlWaitsForOwnedSocketDrain(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		name := "unpressured"
		if pressure {
			name = "receiver-pressure"
		}
		t.Run(name, func(t *testing.T) {
			o, _ := qaWorkerOptions(t)
			short, err := os.MkdirTemp("/tmp", "tw-stop-")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(short)
			o.StatePath = filepath.Join(short, "activity.json")
			normalized := qaWorkerNew(t, o)
			o = normalized.options
			owner, err := acquire(context.Background(), o.StatePath+".worker")
			if err != nil {
				t.Fatal(err)
			}
			var ownerOnce sync.Once
			releaseOwner := func() { ownerOnce.Do(owner.close) }
			defer releaseOwner()
			qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, InstanceMode: "foreground"})
			conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: o.StatePath + ".worker.sock", Net: "unixgram"})
			if err != nil {
				releaseOwner()
				t.Fatal("owned socket fixture cannot bind")
			}
			if err = conn.SetReadBuffer(1024); err != nil {
				conn.Close()
				releaseOwner()
				t.Fatal("owned socket buffer setup failed")
			}
			drain := make(chan struct{})
			var drainOnce sync.Once
			releaseDrain := func() { drainOnce.Do(func() { close(drain) }) }
			ownerDone := make(chan struct{})
			ownerReadError := make(chan error, 1)
			go func() {
				defer close(ownerDone)
				defer releaseOwner()
				defer conn.Close()
				<-drain
				var buf [16]byte
				for {
					n, _, readErr := conn.ReadFromUnix(buf[:])
					if readErr != nil {
						ownerReadError <- readErr
						return
					}
					if string(buf[:n]) == "stop" {
						return
					}
				}
			}()
			defer func() {
				releaseDrain()
				conn.Close()
				select {
				case <-ownerDone:
				case <-time.After(time.Second):
					t.Error("synthetic owner cleanup leaked")
				}
			}()
			if !pressure {
				releaseDrain()
			}
			if err = Notify(context.Background(), o.StatePath, Wake); err != nil {
				t.Fatal("initial owned notification failed before pressure")
			}
			if pressure {
				qaStopAdmitPressure(t, o.StatePath)
			}
			controller := qaWorkerNew(t, o)
			reserved := make(chan struct{})
			controller.fail = func(stage string) error {
				if stage == "control_after_reserved" {
					close(reserved)
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			type outcome struct {
				result Result
				err    error
			}
			stopped := make(chan outcome, 1)
			stopJoined := make(chan struct{})
			go func() {
				defer close(stopJoined)
				result, stopErr := controller.Stop(ctx, ControlRequest{RequestID: qaWorkerID(801)})
				stopped <- outcome{result, stopErr}
			}()
			defer func() {
				cancel()
				select {
				case <-stopJoined:
				case <-time.After(time.Second):
					t.Error("synthetic Stop controller leaked")
				}
			}()
			select {
			case <-reserved:
			case <-ctx.Done():
				t.Fatal("Stop did not durably reserve its exact identity")
			}
			var got outcome
			returned := false
			if pressure {
				select {
				case got = <-stopped:
					returned = true
					if got.err != nil {
						var safe *Error
						if errors.As(got.err, &safe) {
							t.Errorf("Stop abandoned live owned receiver pressure: code=%s", safe.Code)
						} else {
							t.Error("Stop abandoned live owner with untyped error")
						}
					} else {
						t.Error("Stop completed while synthetic owner still holds its lock")
					}
				case <-time.After(100 * time.Millisecond):
				}
				releaseDrain()
			}
			if !returned {
				select {
				case got = <-stopped:
				case <-ctx.Done():
					t.Fatal("Stop did not finish inside original parent bound after drain")
				}
			}
			if got.err != nil {
				if !pressure {
					t.Error("unpressured Stop failed")
				}
				// A failed original implementation must retain its first exact intent.
				control := qaWorkerReadControl(t, o)
				if control.Pending == nil || control.Pending.Request.RequestID != qaWorkerID(801) || control.Receipts[qaWorkerID(801)].Result != nil {
					t.Error("failed Stop lost pending exact intent or invented completion")
				}
				t.Fatal("Stop failed expected successful recovery after original owned drain")
			}
			select {
			case <-ownerDone:
			case <-time.After(time.Second):
				t.Fatal("Stop did not join released foreground owner")
			}
			select {
			case <-ownerReadError:
				t.Error("synthetic owner exited without receiving Stop")
			default:
			}
			if got.result.Status.State == "running" {
				t.Error("Stop returned running owner")
			}
			control := qaWorkerReadControl(t, o)
			if control.Pending != nil || control.Receipts[qaWorkerID(801)].Result == nil {
				t.Fatal("completed Stop missing durable receipt")
			}
			before, err := os.ReadFile(o.StatePath + ".worker-control.json")
			if err != nil {
				t.Fatal(err)
			}
			replay, err := controller.Stop(ctx, ControlRequest{RequestID: qaWorkerID(801)})
			after, readErr := os.ReadFile(o.StatePath + ".worker-control.json")
			if err != nil || readErr != nil || !reflect.DeepEqual(replay, got.result) || !reflect.DeepEqual(before, after) {
				t.Error("completed exact Stop replay changed outcome/history")
			}
			if _, err = os.Stat(o.StatePath); !os.IsNotExist(err) {
				t.Error("foreground Stop minted activity state")
			}
		})
	}
}
