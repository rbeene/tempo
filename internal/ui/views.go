package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
)

// ReadViews supplies shared ancillary local observations. The runner invokes
// them only for an explicit view request, never during timer refresh.
type ReadViews struct {
	Links func(context.Context) (activity.BindingList, error)
	Sync  func(context.Context) (activity.SyncStatus, error)
	Setup func(context.Context) (setup.Status, error)
}

type localView uint8

const (
	timerDetails localView = iota
	linksView
	syncView
	setupView
	helpView
)

func observeView[T any](ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	ctx, end := context.WithTimeout(ctx, 250*time.Millisecond)
	defer end()
	return read(ctx)
}

func showView(ctx context.Context, view localView, snapshot activity.ActivitySnapshot, key TimerKey, selected bool, p *promptBridge, views *ReadViews) error {
	switch view {
	case timerDetails:
		return p.View(ctx, "Timer details", timerDetail(snapshot, key, selected))
	case helpView:
		return p.View(ctx, "Help", "Enter: inspect selected timer and immutable attribution\n2 Links: search local mappings and inspect full scope\nl Links actions: create, repair or unlink a reviewed mapping\nx Activity: inspect actors, review timing or capture warnings\na Accounts and auth: inspect, verify or explicitly change credentials/account\n3 Sync: inspect upload queue and exact accounting\n, Setup: inspect local readiness\nUp/Down: select timer or scroll details\nr: refresh authoritative local activity\nEscape: close the current modal or quit from the dashboard\nq: quit only from dashboard navigation; search text inside pickers\nPaste never submits or confirms a form\nResize to at least 40x8 to use forms\nJob suspension is unavailable; quit normally instead.")
	case linksView:
		if views == nil || views.Links == nil {
			return p.View(ctx, "Links", "Local links unavailable.")
		}
		result, err := observeView(ctx, views.Links)
		if err != nil {
			return p.View(ctx, "Links", "Local links unavailable; retry after returning to the dashboard.")
		}
		if len(result.Bindings) == 0 {
			return p.View(ctx, "Links", "No local links. Explicit linking initializes project capture.")
		}
		choices := make([]terminal.Choice, 0, len(result.Bindings))
		for _, b := range result.Bindings {
			choices = append(choices, terminal.Choice{ID: b.ID, Label: b.Kind + " " + b.Locator + " · Project " + b.Attribution.ProjectID})
		}
		id, err := p.Choose(ctx, "Links", choices)
		if err != nil {
			return err
		}
		for _, b := range result.Bindings {
			if b.ID == id {
				return p.View(ctx, "Links · Details", fmt.Sprintf("Binding %s\nRevision %s · Observation %s\nScope %s\nFull locator %s\n%s\nAttached actors %d\nHistorical attribution is retained after permitted link changes.", b.ID, b.Revision, result.SnapshotRevision, b.Kind, b.Locator, attributionDetail(b.Attribution), len(b.AttachedActors)))
			}
		}
	case syncView:
		if views == nil || views.Sync == nil {
			return p.View(ctx, "Sync", "Local sync status unavailable.")
		}
		result, err := observeView(ctx, views.Sync)
		if err != nil {
			return p.View(ctx, "Sync", "Local sync status unavailable; retry after returning to the dashboard.")
		}
		body := fmt.Sprintf("Uploads enabled: %t\nObservation %s\nWorker %s\nExact captured %s (%s ns)\nPlanned upload %s (%s ns)\nConfirmed returned %s (%s ns)\nPlanned residual %s ns\nReturned residual %s ns\nQueued roots %d\nUnknown writes require read-only reconciliation; absence never authorizes a replacement.", result.Enabled, result.SnapshotRevision, result.Worker.State, duration(result.Totals.ExactDurationNS), result.Totals.ExactDurationNS, optionalDuration(result.Totals.PlannedDurationNS), optionalCounter(result.Totals.PlannedDurationNS), optionalDuration(result.Totals.ConfirmedDurationNS), optionalCounter(result.Totals.ConfirmedDurationNS), optionalCounter(result.Totals.PlannedResidualNS), optionalCounter(result.Totals.TotalResidualNS), len(result.Items))
		if len(result.Items) == 0 {
			return p.View(ctx, "Sync", body)
		}
		choices := make([]terminal.Choice, 0, len(result.Items))
		for _, item := range result.Items {
			choices = append(choices, terminal.Choice{ID: item.ID, Label: item.State + " · Project " + item.Interval.Attribution.ProjectID})
		}
		id, err := p.Choose(ctx, "Sync · Queue", choices)
		if err != nil {
			return err
		}
		for _, item := range result.Items {
			if item.ID == id {
				return p.View(ctx, "Sync · Details", body+fmt.Sprintf("\n\nOutbox %s\nRevision %s\nState %s\nComputer %s\n%s\nCaptured %s\nEntry %s", item.ID, item.Revision, item.State, item.Interval.ComputerID, attributionDetail(item.Interval.Attribution), duration(item.Interval.DurationNS), optionalCounter(item.EntryID)))
			}
		}
	case setupView:
		if views == nil || views.Setup == nil {
			return p.View(ctx, "Setup", "Local setup readiness unavailable.")
		}
		result, err := observeView(ctx, views.Setup)
		if err != nil {
			return p.View(ctx, "Setup", "Local setup readiness unavailable; inspect configuration before checking again.")
		}
		var body strings.Builder
		for _, step := range result.Steps {
			fmt.Fprintf(&body, "%s · %s\n%s\n\n", step.Action, step.State, step.SafeMessage)
		}
		body.WriteString("Capture mapping, host trust/delivery, upload consent and worker ownership are separate.")
		return p.View(ctx, "Setup", body.String())
	}
	return nil
}

func attributionDetail(a activity.Attribution) string {
	return fmt.Sprintf("Account %s · Project %s\nTask %s · User %s · Timezone %s", a.AccountID, a.ProjectID, a.TaskID, a.UserID, a.Timezone)
}
func optionalDuration(ns *string) string {
	if ns == nil {
		return "unavailable"
	}
	return duration(*ns)
}
func optionalCounter(value *string) string {
	if value == nil {
		return "unavailable"
	}
	return *value
}

func timerDetail(snapshot activity.ActivitySnapshot, key TimerKey, selected bool) string {
	if !selected {
		return "No activity yet."
	}
	var body strings.Builder
	fmt.Fprintf(&body, "Computer %s\nAccount %s · Project %s\nObservation %s · %s\n", key.ComputerID, key.AccountID, key.ProjectID, snapshot.SnapshotRevision, snapshot.ObservedAt.UTC().Format(time.RFC3339Nano))
	for _, timer := range snapshot.ProjectTimers {
		if timerIdentity(timer) == key {
			fmt.Fprintf(&body, "Provisional union %s\nConfirmed closed %s\n%d active · %d waiting · %d queued · %d need attention\n", duration(timer.ProvisionalUnionNS), duration(timer.ConfirmedClosedNS), len(timer.ActiveActorRefs), len(timer.WaitingActorRefs), timer.QueuedCount, timer.NeedsAttentionCount)
		}
	}
	for _, project := range snapshot.Projects {
		if project.ComputerID == key.ComputerID && project.Attribution.AccountID == key.AccountID && project.Attribution.ProjectID == key.ProjectID {
			fmt.Fprintf(&body, "\nAttribution epoch\n%s\nProvisional union %s · Confirmed closed %s\n", attributionDetail(project.Attribution), duration(project.ProvisionalUnionNS), duration(project.ConfirmedClosedNS))
		}
	}
	for _, actor := range snapshot.Actors {
		if actor.Ref.Key.ComputerID == key.ComputerID && actor.Attribution.AccountID == key.AccountID && actor.Attribution.ProjectID == key.ProjectID {
			fmt.Fprintf(&body, "\nActor %s · %s/%s\nGeneration %s · Revision %s\nState %s · Health %s\n", actor.ID, actor.Ref.Key.Source, actor.Ref.Key.AgentID, actor.Ref.Generation, actor.Revision, actor.State, actor.Health)
		}
	}
	return body.String()
}
