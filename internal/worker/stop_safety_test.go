//go:build darwin || linux

package worker

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// These independent controls supplement, and never modify, the admitted
// pressure helper. Every owner, endpoint, control ID, and path is synthetic.
type qaStopSafetyFixture struct {
	o          Options
	controller *Service
	owner      *heldFile
	conn       *net.UnixConn
	closeOwner sync.Once
	queuedWake int
}

func qaStopSafetyNew(t *testing.T) *qaStopSafetyFixture {
	t.Helper()
	o, syncer := qaWorkerOptions(t)
	short, err := os.MkdirTemp("/tmp", "tw-safe-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(short) })
	o.StatePath = filepath.Join(short, "activity.json")
	o = qaWorkerNew(t, o).options
	owner, err := acquire(context.Background(), o.StatePath+".worker")
	if err != nil {
		t.Fatal(err)
	}
	f := &qaStopSafetyFixture{o: o, controller: qaWorkerNew(t, o), owner: owner}
	t.Cleanup(func() {
		f.unlock()
		if f.conn != nil {
			f.conn.Close()
		}
		if syncer.calls != 0 {
			t.Error("control invoked provider sync")
		}
		if _, err := os.Stat(f.o.StatePath); !os.IsNotExist(err) {
			t.Error("control minted activity state")
		}
	})
	qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, InstanceMode: "foreground"})
	f.conn, err = net.ListenUnixgram("unixgram", &net.UnixAddr{Name: o.StatePath + ".worker.sock", Net: "unixgram"})
	if err != nil {
		t.Fatal("owned safety receiver bind failed")
	}
	if err = f.conn.SetReadBuffer(1024); err != nil {
		t.Fatal("owned safety receiver buffer setup failed")
	}
	return f
}
func (f *qaStopSafetyFixture) unlock() { f.closeOwner.Do(func() { f.owner.close() }) }
func (f *qaStopSafetyFixture) pressure(t *testing.T) {
	t.Helper()
	if err := Notify(context.Background(), f.o.StatePath, Wake); err != nil {
		t.Fatal("initial owned Wake failed")
	}
	f.queuedWake = qaStopAdmitPressure(t, f.o.StatePath)
}

type qaStopSafetyOutcome struct {
	result Result
	err    error
}

func (f *qaStopSafetyFixture) start(t *testing.T, ctx context.Context, cancel context.CancelFunc, id string) <-chan qaStopSafetyOutcome {
	t.Helper()
	reserved := make(chan struct{})
	f.controller.fail = func(stage string) error {
		if stage == "control_after_reserved" {
			close(reserved)
		}
		return nil
	}
	out := make(chan qaStopSafetyOutcome, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		result, err := f.controller.Stop(ctx, ControlRequest{RequestID: id})
		out <- qaStopSafetyOutcome{result, err}
	}()
	// Registered last: controller joins before the owned owner/endpoint close.
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("safety Stop controller cleanup leaked")
		}
	})
	select {
	case <-reserved:
	case <-ctx.Done():
		t.Fatal("safety Stop did not reserve exact request")
	}
	return out
}
func qaStopSafetyHeld(t *testing.T, out <-chan qaStopSafetyOutcome) {
	t.Helper()
	// This is an observation window, not a production hook or queue warm-up.
	// An early error/success fails admission. Namespace controls below require
	// the eventual typed failure and independently inspect both endpoints.
	select {
	case got := <-out:
		var safe *Error
		if errors.As(got.err, &safe) {
			t.Fatalf("control left continuously held owner before mutation: code=%s", safe.Code)
		}
		t.Fatal("control left continuously held owner before mutation")
	case <-time.After(100 * time.Millisecond):
	}
}
func qaStopSafetyGet(t *testing.T, ctx context.Context, out <-chan qaStopSafetyOutcome) qaStopSafetyOutcome {
	t.Helper()
	select {
	case got := <-out:
		return got
	case <-ctx.Done():
		// A deadline result may race the caller's Done notification.
		select {
		case got := <-out:
			return got
		case <-time.After(time.Second):
			t.Fatal("safety Stop did not join its expired caller")
		}
	}
	return qaStopSafetyOutcome{}
}
func qaStopSafetyPending(t *testing.T, o Options, id string) {
	t.Helper()
	control := qaWorkerReadControl(t, o)
	if control.Pending == nil || control.Pending.Request.RequestID != id || control.Receipts[id].Result != nil {
		t.Error("safety failure lost exact pending identity or invented completion")
	}
}
func qaStopSafetyNoStop(t *testing.T, conn *net.UnixConn, queuedWake int) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal("owned receiver deadline failed")
	}
	var buf [16]byte
	// Inspect every admitted Wake and one more slot for an unexpected Stop.
	for i := 0; i <= queuedWake; i++ {
		if i == queuedWake {
			if err := conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
				t.Fatal("owned receiver final inspection deadline failed")
			}
		}
		n, _, err := conn.ReadFromUnix(buf[:])
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() && i == queuedWake {
				return
			}
			t.Fatal("owned receiver inspection failed")
		}
		if string(buf[:n]) == "stop" {
			t.Error("Stop reached an endpoint after rejected ownership/cancellation")
		}
	}
	t.Fatal("bounded owned receiver inspection did not drain")
}

func TestQAStopPinnedOwnershipAndCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "socket-replacement", "lock-replacement", "root-unsafe", "root-replacement"} {
		t.Run(mode, func(t *testing.T) {
			f := qaStopSafetyNew(t)
			f.pressure(t)
			budget := 3 * time.Second
			if mode == "deadline" {
				budget = 500 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			id := qaWorkerID(820)
			out := f.start(t, ctx, cancel, id)
			qaStopSafetyHeld(t, out)
			want := "state_corrupt"
			journalOptions := f.o
			var replacement *net.UnixConn
			socket := f.o.StatePath + ".worker.sock"
			switch mode {
			case "cancel":
				want = "manager"
				cancel()
			case "deadline":
				want = "manager"
			case "socket-replacement":
				if err := os.Rename(socket, socket+".retired"); err != nil {
					t.Fatal("owned socket replacement rename failed")
				}
				var err error
				replacement, err = net.ListenUnixgram("unixgram", &net.UnixAddr{Name: socket, Net: "unixgram"})
				if err != nil {
					t.Fatal("owned replacement socket bind failed")
				}
				defer replacement.Close()
			case "lock-replacement":
				lock := f.o.StatePath + ".worker.lock"
				if err := os.Rename(lock, lock+".retired"); err != nil {
					t.Fatal("owned lock replacement rename failed")
				}
				other, err := acquire(context.Background(), f.o.StatePath+".worker")
				if err != nil {
					t.Fatal("owned replacement lock acquisition failed")
				}
				defer other.close()
			case "root-unsafe":
				root := filepath.Dir(f.o.StatePath)
				if err := os.Chmod(root, 0755); err != nil {
					t.Fatal("owned root mode mutation failed")
				}
				defer os.Chmod(root, 0700)
			case "root-replacement":
				root := filepath.Dir(f.o.StatePath)
				retired := root + ".retired"
				if err := os.Rename(root, retired); err != nil {
					t.Fatal("owned root replacement rename failed")
				}
				defer os.RemoveAll(retired)
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal("owned replacement root creation failed")
				}
				journalOptions.StatePath = filepath.Join(retired, filepath.Base(f.o.StatePath))
				var err error
				replacement, err = net.ListenUnixgram("unixgram", &net.UnixAddr{Name: socket, Net: "unixgram"})
				if err != nil {
					t.Fatal("owned replacement root receiver bind failed")
				}
				defer replacement.Close()
			}
			got := qaStopSafetyGet(t, ctx, out)
			if mode == "root-unsafe" {
				if err := os.Chmod(filepath.Dir(f.o.StatePath), 0700); err != nil {
					t.Fatal("owned root mode restoration failed")
				}
			}
			// Endpoint delivery inspection runs even when the typed result is
			// unexpected, so a replacement delivery cannot hide behind Fatal.
			qaStopSafetyNoStop(t, f.conn, f.queuedWake)
			if replacement != nil {
				qaStopSafetyNoStop(t, replacement, 0)
			}
			qaWorkerCode(t, got.err, want)
			qaStopSafetyPending(t, journalOptions, id)
		})
	}
}

func TestQAStopSuccessfulSendWaitsForOwnerRelease(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "receiver-retained"
		if remove {
			name = "endpoint-removed-before-unlock"
		}
		t.Run(name, func(t *testing.T) {
			f := qaStopSafetyNew(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			id := qaWorkerID(821)
			out := f.start(t, ctx, cancel, id)
			if err := f.conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal("owned receiver first-delivery deadline failed")
			}
			var buf [16]byte
			n, _, err := f.conn.ReadFromUnix(buf[:])
			if err != nil || string(buf[:n]) != "stop" {
				t.Fatal("owned receiver did not observe exact Stop delivery")
			}
			if remove {
				if err = os.Remove(f.o.StatePath + ".worker.sock"); err != nil {
					t.Fatal("owned endpoint removal failed")
				}
			}
			qaStopSafetyHeld(t, out)
			qaStopSafetyPending(t, f.o, id)
			// Inspect the still-connected original receiver after successful
			// delivery, including when its pathname was removed. No resend.
			qaStopSafetyNoStop(t, f.conn, f.queuedWake)
			f.unlock()
			got := qaStopSafetyGet(t, ctx, out)
			if got.err != nil {
				t.Fatal("Stop did not finish after original owner release")
			}
			if got.result.Status.State == "running" {
				t.Error("released owner returned running")
			}
			control := qaWorkerReadControl(t, f.o)
			if control.Pending != nil || control.Receipts[id].Result == nil {
				t.Fatal("successful Stop missing exact durable receipt")
			}
			before, err := os.ReadFile(f.o.StatePath + ".worker-control.json")
			if err != nil {
				t.Fatal(err)
			}
			replay, err := f.controller.Stop(ctx, ControlRequest{RequestID: id})
			after, readErr := os.ReadFile(f.o.StatePath + ".worker-control.json")
			if err != nil || readErr != nil || !reflect.DeepEqual(replay, got.result) || !reflect.DeepEqual(before, after) {
				t.Error("successful exact Stop replay changed outcome/history")
			}
		})
	}
}
