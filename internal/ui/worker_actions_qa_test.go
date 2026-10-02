package ui

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/worker"
)

func qaWorkerStart(t *testing.T, c *workerController, a *WorkerActions) *qaHWFlow {
	return qaHWStart(t, func(ctx context.Context, p *promptBridge) error { return c.run(ctx, p, a) })
}
func qaWorkerResult() worker.Result {
	return worker.Result{ContractVersion: 1, Status: activity.WorkerStatus{State: "stopped", QueuedCount: 3, UnknownCount: 2, SyncEnabled: false}}
}
func qaWorkerActions(t *testing.T) *WorkerActions {
	return &WorkerActions{Status: func(ctx context.Context) (worker.Result, error) {
		qaHWBound(t, ctx, 250*time.Millisecond)
		return qaWorkerResult(), nil
	}}
}
func qaWorkerSet(a *WorkerActions, op string, fn func(context.Context, worker.ControlRequest) (worker.Result, error)) {
	switch op {
	case "install":
		a.Install = fn
	case "start":
		a.Start = fn
	case "stop":
		a.Stop = fn
	case "uninstall":
		a.Uninstall = fn
	}
}
func TestQAWorkerControllerLazyMenuAndBack(t *testing.T) {
	a := qaWorkerActions(t)
	a.Status = func(context.Context) (worker.Result, error) {
		t.Error("opening menu read status")
		return worker.Result{}, nil
	}
	f := qaWorkerStart(t, &workerController{}, a)
	r := f.next(t, "choose")
	for _, id := range []string{"status", "install", "start", "stop", "uninstall", "back"} {
		found := false
		for _, v := range r.choices {
			found = found || v.ID == id
		}
		if !found {
			t.Errorf("missing menu %s", id)
		}
	}
	qaHWPick(t, r, "back")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAWorkerControllerStatusBoundedAndNullable(t *testing.T) {
	a := qaWorkerActions(t)
	f := qaWorkerStart(t, &workerController{}, a)
	qaHWPick(t, f.next(t, "choose"), "status")
	qaHWView(t, f, "stopped", "unknown", "queued", "3", "2", "false")
	if e := f.finish(t); e != nil {
		t.Fatal(e)
	}
}
func TestQAWorkerControllerExplicitControlsAndConsent(t *testing.T) {
	for _, op := range []string{"install", "start", "stop", "uninstall"} {
		for _, yes := range []bool{false, true} {
			if (op == "start" || op == "stop") && !yes {
				continue
			}
			t.Run(op+"/"+map[bool]string{false: "decline", true: "accept"}[yes], func(t *testing.T) {
				a := qaWorkerActions(t)
				calls := 0
				qaWorkerSet(a, op, func(ctx context.Context, r worker.ControlRequest) (worker.Result, error) {
					calls++
					qaHWBound(t, ctx, 2*time.Minute)
					if !qaHWUUID.MatchString(r.RequestID) || r.Confirmed != (op == "install" || op == "uninstall") {
						t.Errorf("worker request replaced consent: %+v", r)
					}
					return qaWorkerResult(), nil
				})
				f := qaWorkerStart(t, &workerController{}, a)
				qaHWPick(t, f.next(t, "choose"), op)
				if op == "install" || op == "uninstall" {
					r := f.next(t, "confirm")
					qaHWContains(t, r.title, op, "service", "history", "upload")
					if op == "install" {
						qaHWContains(t, r.title, "start")
					}
					if calls != 0 {
						t.Error("manager effect preceded consent")
					}
					r.reply <- promptReply{confirmed: yes}
				}
				if yes {
					qaHWView(t, f, "request")
				}
				_ = f.finish(t)
				if calls != map[bool]int{false: 0, true: 1}[yes] {
					t.Errorf("mutations=%d", calls)
				}
			})
		}
	}
}
func TestQAWorkerControllerUnknownReopenExactReplayAndReadOnlyStatus(t *testing.T) {
	for _, op := range []string{"install", "start", "stop", "uninstall"} {
		t.Run(op, func(t *testing.T) {
			a := qaWorkerActions(t)
			var requests []worker.ControlRequest
			uncertain := &worker.Error{Code: "local_write_unknown", Message: "RAW-MANAGER-CANARY", Uncertain: true}
			qaWorkerSet(a, op, func(ctx context.Context, r worker.ControlRequest) (worker.Result, error) {
				qaHWBound(t, ctx, 2*time.Minute)
				requests = append(requests, r)
				if len(requests) == 1 {
					return worker.Result{}, uncertain
				}
				return qaWorkerResult(), nil
			})
			c := &workerController{}
			f := qaWorkerStart(t, c, a)
			qaHWPick(t, f.next(t, "choose"), op)
			if op == "install" || op == "uninstall" {
				f.next(t, "confirm").reply <- promptReply{confirmed: true}
			}
			r := f.next(t, "view")
			qaHWContains(t, r.body, "local_write_unknown", "request", "same", "nonapplication")
			if strings.Contains(r.body+r.title, "RAW-MANAGER-CANARY") {
				t.Error("raw manager error disclosed")
			}
			r.reply <- promptReply{}
			qaHWPick(t, f.next(t, "choose"), "back")
			if !errors.Is(f.finish(t), uncertain) || c.pending == nil {
				t.Fatal("unknown outcome lost on back")
			}
			g := qaWorkerStart(t, c, a)
			qaHWPick(t, g.next(t, "choose"), "status")
			qaHWView(t, g, "request", "read")
			if len(requests) != 1 {
				t.Error("status retried manager")
			}
			qaHWPick(t, g.next(t, "choose"), "replay")
			qaHWView(t, g, "request")
			if e := g.finish(t); e != nil {
				t.Fatal(e)
			}
			if c.pending != nil || len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) {
				t.Errorf("reopen changed exact replay: %+v", requests)
			}
		})
	}
}
func TestQAWorkerControllerDefiniteFailureRequiresFreshConsent(t *testing.T) {
	for _, code := range []string{"revision_conflict", "request_conflict", "manager", "RAW-MANAGER-CODE"} {
		t.Run(code, func(t *testing.T) {
			a := qaWorkerActions(t)
			calls := 0
			var ids []string
			a.Install = func(context.Context, worker.ControlRequest) (worker.Result, error) {
				return worker.Result{}, errors.New("unexpected")
			}
			qaWorkerSet(a, "install", func(ctx context.Context, r worker.ControlRequest) (worker.Result, error) {
				calls++
				ids = append(ids, r.RequestID)
				return worker.Result{}, &worker.Error{Code: code, Message: "RAW-MANAGER-CANARY"}
			})
			c := &workerController{}
			for i := 0; i < 2; i++ {
				f := qaWorkerStart(t, c, a)
				qaHWPick(t, f.next(t, "choose"), "install")
				f.next(t, "confirm").reply <- promptReply{confirmed: true}
				r := f.next(t, "view")
				qaHWContains(t, r.body, "review")
				if strings.Contains(r.body+r.title, "RAW-MANAGER") {
					t.Error("raw manager details disclosed")
				}
				r.reply <- promptReply{}
				_ = f.finish(t)
				if c.pending != nil {
					t.Fatal("definite failure retained unknown recovery")
				}
			}
			if calls != 2 || ids[0] == ids[1] {
				t.Errorf("new review reused failed request: %+v", ids)
			}
		})
	}
}
func TestQAWorkerControllerCanceledDispatchAndUnknownRemainDistinct(t *testing.T) {
	a := qaWorkerActions(t)
	var calls atomic.Int32
	started := make(chan struct{})
	a.Start = func(ctx context.Context, r worker.ControlRequest) (worker.Result, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return worker.Result{}, &worker.Error{Code: "local_write_unknown", Message: "RAW-CANCELED-CANARY", Uncertain: true}
	}
	c := &workerController{}
	f := qaWorkerStart(t, c, a)
	qaHWPick(t, f.next(t, "choose"), "start")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("explicit start never dispatched")
	}
	f.cancel()
	if !unknownOutcome(f.finish(t)) || c.pending == nil || calls.Load() != 1 {
		t.Fatal("cancel after dispatch erased unknown outcome")
	}
}
func TestQAWorkerControllerCanceledConfirmationHasNoEffects(t *testing.T) {
	a := qaWorkerActions(t)
	var effects atomic.Int32
	a.Install = func(context.Context, worker.ControlRequest) (worker.Result, error) {
		effects.Add(1)
		return worker.Result{}, nil
	}
	f := qaWorkerStart(t, &workerController{}, a)
	qaHWPick(t, f.next(t, "choose"), "install")
	_ = f.next(t, "confirm")
	f.cancel()
	if e := f.finish(t); !errors.Is(e, context.Canceled) {
		t.Errorf("cancel result=%v", e)
	}
	if effects.Load() != 0 {
		t.Fatal("cancel submitted service install")
	}
}
func TestQAWorkerControllerUnavailableCallbackIsSafe(t *testing.T) {
	f := qaWorkerStart(t, &workerController{}, &WorkerActions{})
	qaHWPick(t, f.next(t, "choose"), "install")
	r := f.next(t, "view")
	qaHWContains(t, r.body, "unavailable")
	r.reply <- promptReply{}
	_ = f.finish(t)
}
