package ui

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
	"github.com/rbeene/tempo/internal/terminal"
)

// SyncActions delegates immutable upload consent and recovery to the shared
// engine. Remote ambiguity never authorizes another POST.
type SyncActions struct {
	Accounts  func(context.Context) ([]harvest.Object, error)
	Identity  func(context.Context, string) (activity.SyncAccountIdentity, error)
	Status    func(context.Context) (activity.SyncStatus, error)
	Configure func(context.Context, activity.SyncConfigureInput) (activity.SyncConfigurationResult, error)
	Now       func(context.Context, activity.SyncRunInput) (activity.SyncRun, error)
	Reconcile func(context.Context, activity.SyncReconcileInput) (activity.SyncRun, error)
	Resolve   func(context.Context, activity.SyncResolveInput) (activity.MutationResult, error)
	Pause     func(context.Context, string) (activity.MutationResult, error)
	Resume    func(context.Context, string) (activity.MutationResult, error)
}

type syncPending struct {
	operation string
	configure activity.SyncConfigureInput
	run       activity.SyncRunInput
	reconcile activity.SyncReconcileInput
	resolve   activity.SyncResolveInput
	requestID string
	err       error
	details   string
	remote    bool
	// A GET-only reconciliation may have its own uncertain local receipt.
	// It cannot replace or acknowledge the original ambiguous POST.
	recovery *syncPending
}

type syncController struct{ pending *syncPending }

func (c *syncController) run(ctx context.Context, p *promptBridge, actions *SyncActions) error {
	if c.pending != nil {
		return c.recover(ctx, p, actions)
	}
	if actions == nil {
		return syncUnavailable(ctx, p)
	}
	op, err := p.Choose(ctx, "Sync actions", []terminal.Choice{{ID: "status", Label: "Read-only sync status"}, {ID: "configure", Label: "Review account upload consent"}, {ID: "now", Label: "Submit one bounded upload pass"}, {ID: "reconcile", Label: "Reconcile existing entries without POST"}, {ID: "resolve", Label: "Attach an entry or authorize rejected retry"}, {ID: "pause", Label: "Pause future upload claims"}, {ID: "resume", Label: "Resume future upload claims"}, {ID: "back", Label: "Back"}})
	if err != nil || op == "back" {
		return err
	}
	if op == "status" {
		if actions.Status == nil {
			return syncUnavailable(ctx, p)
		}
		status, err := syncRead(ctx, actions)
		if err != nil {
			return syncFailure(ctx, p, err)
		}
		return p.View(ctx, "Sync · Read-only status", syncStatusDetails(status))
	}
	if !syncAvailable(actions, op) {
		return syncUnavailable(ctx, p)
	}
	intent, err := syncPrepare(ctx, p, actions, op)
	if err != nil {
		return syncFailure(ctx, p, err)
	}
	if intent == nil {
		return nil
	}
	c.pending = intent
	body, err := intent.dispatch(ctx, actions)
	if err == nil {
		c.pending = nil
		return p.View(ctx, "Sync · Local operation complete", body)
	}
	if !unknownOutcome(err) {
		c.pending = nil
		return syncFailure(ctx, p, err)
	}
	intent.err = err
	var remote *harvest.Error
	intent.remote = errors.As(err, &remote) && remote.Uncertain
	if p.View(ctx, "Sync · Outcome unknown", intent.unknownDetails()) != nil {
		return intent.err
	}
	return c.recover(ctx, p, actions)
}

func syncAvailable(a *SyncActions, op string) bool {
	if a == nil {
		return false
	}
	switch op {
	case "configure":
		return a.Configure != nil && a.Accounts != nil && a.Identity != nil && a.Status != nil
	case "now":
		return a.Now != nil
	case "reconcile":
		return a.Reconcile != nil && a.Status != nil
	case "resolve":
		return a.Resolve != nil && a.Status != nil
	case "pause":
		return a.Pause != nil
	case "resume":
		return a.Resume != nil
	}
	return false
}

func syncPrepare(ctx context.Context, p *promptBridge, a *SyncActions, op string) (*syncPending, error) {
	intent := &syncPending{operation: op}
	switch op {
	case "configure":
		accounts, err := authCall(ctx, 2*time.Minute, a.Accounts)
		if err != nil {
			return nil, err
		}
		account, err := chooseAuthAccount(ctx, p, accounts)
		if err != nil {
			return nil, err
		}
		verified, err := authCall(ctx, 2*time.Minute, func(ctx context.Context) (activity.SyncAccountIdentity, error) { return a.Identity(ctx, account) })
		if err != nil {
			return nil, err
		}
		if verified.AccountID != account || !identity.Valid(verified.UserID) {
			return nil, &activity.Error{Code: "identity_conflict"}
		}
		status, err := syncRead(ctx, a)
		if err != nil {
			return nil, err
		}
		revision := "0"
		found := false
		for _, configuration := range status.Configurations {
			if configuration.AccountID == verified.AccountID && configuration.UserID == verified.UserID {
				if _, valid := canonicalCounter(configuration.Revision); !valid || found {
					return nil, &activity.Error{Code: "state_corrupt"}
				}
				revision, found = configuration.Revision, true
			}
		}
		mode, err := p.Choose(ctx, "Sync · Tracking mode", []terminal.Choice{{ID: "duration", Label: "Duration"}, {ID: "timestamp", Label: "Timestamp"}})
		if err != nil {
			return nil, err
		}
		policies := []terminal.Choice{{ID: "exact", Label: "Exact captured duration"}}
		if mode == "duration" {
			policies = append(policies, terminal.Choice{ID: "nearest-hundredth-hour", Label: "Round to nearest hundredth hour"})
		} else if mode != "timestamp" {
			return nil, &activity.Error{Code: "validation"}
		}
		policy, err := p.Choose(ctx, "Sync · Duration policy", policies)
		if err != nil {
			return nil, err
		}
		if policy != "exact" && !(mode == "duration" && policy == "nearest-hundredth-hour") {
			return nil, &activity.Error{Code: "validation"}
		}
		clock := ""
		if mode == "timestamp" {
			clock, err = p.Choose(ctx, "Sync · Timestamp clock", []terminal.Choice{{ID: "12h", Label: "12-hour clock"}, {ID: "24h", Label: "24-hour clock"}})
			if err != nil {
				return nil, err
			}
			if clock != "12h" && clock != "24h" {
				return nil, &activity.Error{Code: "validation"}
			}
		}
		intent.configure = activity.SyncConfigureInput{AccountID: verified.AccountID, UserID: verified.UserID, Mode: mode, DurationPolicy: policy, Clock: clock, IfRevision: revision, RequestID: requestID(), Confirmed: true}
		intent.details = fmt.Sprintf("Configure upload consent\nAccount %s · Verified current user %s\nObserved configuration revision %s\nMode %s · Duration policy %s · Clock %s", verified.AccountID, verified.UserID, revision, mode, policy, clock)
		warning := "\nThis declares upload representation consent for this exact account and user. Captured history and historical attribution remain retained. It does not enable or resume uploads, run a pass, change the saved default account, or control the worker. A current-user change or revision conflict requires fresh review. Exact duration preserves captured integer nanoseconds without promising provider precision."
		if policy == "nearest-hundredth-hour" {
			warning += " Nearest-hundredth-hour rounding uses 36-second units, with 18-second ties rounding up. Below 18 seconds blocks the root before POST. Planned and returned signed residuals remain visible; capture is not rounded."
		}
		if mode == "timestamp" {
			warning += " Timestamp requires exact whole-minute endpoints, one local date, the verified user timezone, and no ambiguous clock fold or midnight crossing; no rounding is authorized."
		}
		intent.details += warning
	case "now":
		limit, err := syncLimit(ctx, p)
		if err != nil {
			return nil, err
		}
		intent.run = activity.SyncRunInput{RequestID: requestID(), Limit: limit}
		intent.details = fmt.Sprintf("Submit one bounded upload pass · Limit %d\nExisting queue attribution, saved account/current-user consent and frozen configuration remain authoritative. The shared service reserves eligible targets durably; this preview does not retarget historical account, user, project or task attribution. Unknown Harvest writes cannot be retried; use read-only reconciliation. Only never-attempted queued parts or explicitly authorized conclusively rejected parts may POST. No automatic retry, worker launch or consent change follows. Capture and history remain retained.", limit)
	case "reconcile":
		status, err := syncRead(ctx, a)
		if err != nil {
			return nil, err
		}
		target, err := syncTarget(ctx, p, status, true)
		if err != nil {
			return nil, err
		}
		limit, err := syncLimit(ctx, p)
		if err != nil {
			return nil, err
		}
		intent.reconcile = activity.SyncReconcileInput{RequestID: requestID(), OutboxID: target.id, Limit: limit}
		intent.details = fmt.Sprintf("Read-only remote reconciliation · Limit %d\n%s\nComplete GET-only matching may save a local resolution receipt. No Harvest POST or replacement write is authorized; absence does not prove nonapplication.", limit, target.details)
		return intent, nil
	case "resolve":
		status, err := syncRead(ctx, a)
		if err != nil {
			return nil, err
		}
		target, err := syncTarget(ctx, p, status, false)
		if err != nil {
			return nil, err
		}
		choices := []terminal.Choice{{ID: "attach", Label: "Attach an existing stopped Harvest entry"}}
		if target.retry {
			choices = append(choices, terminal.Choice{ID: "retry-rejected", Label: "Authorize retry of conclusively rejected parts"})
		}
		mode, err := p.Choose(ctx, "Sync · Resolve outbox", choices)
		if err != nil {
			return nil, err
		}
		in := activity.SyncResolveInput{OutboxID: target.id, IfRevision: target.revision, RequestID: requestID(), Confirmed: true}
		switch mode {
		case "attach":
			in.EntryID, err = p.Text(ctx, "Existing stopped Harvest entry ID", "")
			if err != nil {
				return nil, err
			}
			if !identity.Valid(in.EntryID) {
				return nil, &activity.Error{Code: "validation"}
			}
		case "retry-rejected":
			if !target.retry {
				return nil, &activity.Error{Code: "invalid_transition"}
			}
			in.RetryRejected = true
		default:
			return nil, &activity.Error{Code: "validation"}
		}
		intent.resolve = in
		intent.details = "Resolve " + mode + "\n" + target.details + "\nExisting stopped entry " + in.EntryID + "\nAttachment verifies the exact saved account/current user, stopped entry, immutable marker and complete collision scan; it creates no Harvest entry. Rejected retry changes only local authorization for conclusively rejected parts. Unknown or submitting parts cannot authorize retry. A later separate manual Now pass is required for any authorized POST; this operation sends no POST. Historical attribution and attempt audit remain retained."
	case "pause", "resume":
		intent.requestID = requestID()
		intent.details = "Review " + op + " future upload claims. Capture and historical activity continue; upload consent remains separate. This does not start, stop, install or remove the worker, run a pass, or change account configuration. An already dispatched Harvest write may remain uncertain; no automatic retry follows."
	default:
		return nil, &activity.Error{Code: "validation"}
	}
	yes, err := p.Confirm(ctx, intent.details+"\nRequest ID "+intent.id())
	if err != nil || !yes {
		return nil, err
	}
	return intent, nil
}

func syncLimit(ctx context.Context, p *promptBridge) (int, error) {
	text, err := p.Text(ctx, "Bounded batch limit (1–100)", "20")
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(text)
	if err != nil || n < 1 || n > 100 || strconv.Itoa(n) != text {
		return 0, &activity.Error{Code: "validation"}
	}
	return n, nil
}

type syncObservedTarget struct {
	id, revision, details string
	retry                 bool
}

func syncTarget(ctx context.Context, p *promptBridge, status activity.SyncStatus, all bool) (syncObservedTarget, error) {
	// Project display and scalar request fields before prompting, so callers
	// cannot mutate a borrowed snapshot while the operator reviews it.
	targets := []syncObservedTarget{}
	choices := []terminal.Choice{}
	if all {
		targets = append(targets, syncObservedTarget{details: "All eligible observed outbox roots"})
		choices = append(choices, terminal.Choice{ID: "all", Label: "All eligible outbox roots"})
	}
	for _, item := range status.Items {
		targets = append(targets, syncObservedTarget{id: item.ID, revision: item.Revision, details: syncItemDetails(item), retry: syncRejectedRetry(item)})
		choices = append(choices, terminal.Choice{ID: item.ID, Label: item.State + " · Outbox " + item.ID + " · Account " + item.Interval.Attribution.AccountID + " · User " + item.Interval.Attribution.UserID})
	}
	if len(choices) == 0 {
		return syncObservedTarget{}, &activity.Error{Code: "input_required"}
	}
	id, err := p.Choose(ctx, "Sync · Observed outbox target", choices)
	if err != nil {
		return syncObservedTarget{}, err
	}
	for _, target := range targets {
		if target.id == id || all && id == "all" && target.id == "" {
			return target, nil
		}
	}
	return syncObservedTarget{}, &activity.Error{Code: "not_found"}
}

func syncRejectedRetry(item activity.OutboxItem) bool {
	if item.Plan == nil || item.State == "unknown" || item.State == "submitting" {
		return false
	}
	rejected := false
	for _, part := range item.Plan.Parts {
		switch part.State {
		case "rejected":
			rejected = true
		case "synced", "queued":
		default:
			return false
		}
	}
	return rejected
}

func syncRead(ctx context.Context, a *SyncActions) (activity.SyncStatus, error) {
	if ctx.Err() != nil {
		return activity.SyncStatus{}, context.Cause(ctx)
	}
	if a == nil || a.Status == nil {
		return activity.SyncStatus{}, &activity.Error{Code: "unsupported_contract"}
	}
	return observeView(ctx, a.Status)
}

func (in *syncPending) id() string {
	switch in.operation {
	case "configure":
		return in.configure.RequestID
	case "now":
		return in.run.RequestID
	case "reconcile":
		return in.reconcile.RequestID
	case "resolve":
		return in.resolve.RequestID
	}
	return in.requestID
}

func (in *syncPending) dispatch(ctx context.Context, a *SyncActions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	if a == nil {
		return "", &activity.Error{Code: "unsupported_contract"}
	}
	body := "Request ID " + in.id() + "\n"
	switch in.operation {
	case "configure":
		if a.Configure != nil {
			result, err := a.Configure(ctx, in.configure)
			return body + fmt.Sprintf("Configuration saved locally · Returned snapshot revision %s\nAccount %s · User %s · Configuration revision %s\nMode %s · Policy %s · Clock %s\nUpload enablement, capture, worker ownership and historical attribution remain separate.", result.SnapshotRevision, result.Configuration.AccountID, result.Configuration.UserID, result.Configuration.Revision, result.Configuration.Mode, result.Configuration.DurationPolicy, optionalCounter(result.Configuration.Clock)), err
		}
	case "now":
		if a.Now != nil {
			result, err := a.Now(ctx, in.run)
			if err != nil {
				return "", err
			}
			return syncRunDetails(ctx, a, result, body), nil
		}
	case "reconcile":
		if a.Reconcile != nil {
			result, err := a.Reconcile(ctx, in.reconcile)
			if err != nil {
				return "", err
			}
			return syncRunDetails(ctx, a, result, body), nil
		}
	case "resolve", "pause", "resume":
		var result activity.MutationResult
		var err error
		switch {
		case in.operation == "resolve" && a.Resolve != nil:
			result, err = a.Resolve(ctx, in.resolve)
		case in.operation == "pause" && a.Pause != nil:
			result, err = a.Pause(ctx, in.requestID)
		case in.operation == "resume" && a.Resume != nil:
			result, err = a.Resume(ctx, in.requestID)
		default:
			return "", &activity.Error{Code: "unsupported_contract"}
		}
		return body + fmt.Sprintf("Local %s receipt · Snapshot revision %s\nChanged %t · Affected IDs %s\nCapture, historical attribution, upload consent and worker ownership remain separate. No POST pass was launched.", in.operation, result.SnapshotRevision, result.Changed, strings.Join(result.AffectedIDs, ", ")), err
	}
	return "", &activity.Error{Code: "unsupported_contract"}
}

func syncRunDetails(ctx context.Context, a *SyncActions, result activity.SyncRun, body string) string {
	body += fmt.Sprintf("Shared run state %s · Snapshot revision %s\nAttempted IDs %s\nResolved IDs %s\nBlocked IDs %s\nRemaining count %d\nA completed local pass is not proof every upload succeeded. Unknown Harvest writes require read-only reconciliation; absence never authorizes a replacement POST.\n", result.State, result.SnapshotRevision, strings.Join(result.AttemptedIDs, ", "), strings.Join(result.ResolvedIDs, ", "), strings.Join(result.BlockedIDs, ", "), result.RemainingCount)
	if status, err := syncRead(ctx, a); err == nil {
		body += "\nFresh read-only observation:\n" + syncStatusDetails(status)
	} else {
		body += "\nFresh read-only status unavailable. The shared run receipt remains authoritative; no automatic retry follows."
	}
	return body
}

func (in *syncPending) unknownDetails() string {
	body := "local_write_unknown: submitted local outcome or durability remains unknown. Explicit same-intent ledger replay preserves the exact request; read-only status cannot establish nonapplication."
	if in.remote {
		body = "uncertain_write: Harvest POST outcome remains unknown. Read-only inspection and reconciliation only; absence cannot authorize another POST. No Now replay or automatic retry is permitted."
	}
	body += "\nOperation " + in.operation + "\nRequest ID " + in.id() + "\n" + in.details
	if in.recovery != nil {
		body += "\n\nGET-only reconciliation local_write_unknown\nReconciliation request ID " + in.recovery.id() + "\n" + in.recovery.details + "\nOnly explicit replay of this exact reconciliation receipt is permitted; original POST uncertainty remains retained."
	}
	return body
}

func (c *syncController) recover(ctx context.Context, p *promptBridge, a *SyncActions) error {
	for {
		choices := []terminal.Choice{{ID: "replay", Label: c.pending.id() + " · Replay exact local ledger input"}, {ID: "status", Label: "Read-only status"}, {ID: "back", Label: "Back; retain pending outcome"}}
		if c.pending.remote {
			choices = []terminal.Choice{{ID: "status", Label: "Read-only status"}, {ID: "reconcile", Label: "GET-only reconciliation; no POST"}, {ID: "back", Label: "Back; retain original POST ambiguity"}}
		}
		id, err := p.Choose(ctx, "Sync · Recover submitted intent "+c.pending.id(), choices)
		if err != nil || id == "back" {
			return c.pending.err
		}
		switch id {
		case "status":
			body := c.pending.unknownDetails() + "\nRead-only status unavailable."
			if status, err := syncRead(ctx, a); err == nil {
				body = c.pending.unknownDetails() + "\nRead-only observation:\n" + syncStatusDetails(status)
			}
			if p.View(ctx, "Sync · Read-only status", body) != nil {
				return c.pending.err
			}
		case "replay":
			if c.pending.remote {
				return c.pending.err
			}
			body, err := c.pending.dispatch(ctx, a)
			if err == nil {
				c.pending = nil
				return p.View(ctx, "Sync · Exact local receipt complete", body)
			}
			if p.View(ctx, "Sync · Outcome unknown", c.pending.unknownDetails()+"\nExact replay did not establish completion. The callback may be unavailable; no replacement input or request was sent.") != nil {
				return c.pending.err
			}
		case "reconcile":
			if !c.pending.remote {
				return c.pending.err
			}
			if c.pending.recovery == nil {
				if !syncAvailable(a, "reconcile") {
					if p.View(ctx, "Sync · Recovery unavailable", c.pending.unknownDetails()+"\nGET-only reconciliation unavailable.") != nil {
						return c.pending.err
					}
					continue
				}
				intent, err := syncPrepare(ctx, p, a, "reconcile")
				if err != nil {
					if syncFailure(ctx, p, err) != nil {
						return c.pending.err
					}
					continue
				}
				c.pending.recovery = intent
			}
			hadUnknown := c.pending.recovery.err != nil
			body, err := c.pending.recovery.dispatch(ctx, a)
			if err == nil {
				c.pending.recovery = nil
				body += "\n\n" + c.pending.unknownDetails()
			} else {
				// Preserve the GET-only input for an explicit exact receipt replay.
				// A definite failure ends that input, never the original POST.
				if !hadUnknown {
					c.pending.recovery.err = err
				}
				if !unknownOutcome(err) && !hadUnknown {
					c.pending.recovery = nil
				}
				body = c.pending.unknownDetails() + "\nReconciliation did not establish a durable local result; no POST followed."
			}
			if p.View(ctx, "Sync · Read-only reconciliation", body) != nil {
				return c.pending.err
			}
		default:
			return c.pending.err
		}
	}
}

func syncStatusDetails(status activity.SyncStatus) string {
	var body strings.Builder
	fmt.Fprintf(&body, "Uploads enabled %t · Observation %s\nWorker state %s · Queued %d · Submitting %d · Unknown %d\n%s\n", status.Enabled, status.SnapshotRevision, status.Worker.State, status.Worker.QueuedCount, status.Worker.SubmittingCount, status.Worker.UnknownCount, syncTotalsDetails(status.Totals))
	for _, configuration := range status.Configurations {
		fmt.Fprintf(&body, "\nSaved consent · Account %s · User %s · Configuration revision %s\nMode %s · Policy %s · Clock %s · Declared %t\n", configuration.AccountID, configuration.UserID, configuration.Revision, configuration.Mode, configuration.DurationPolicy, optionalCounter(configuration.Clock), configuration.Declared)
	}
	for _, item := range status.Items {
		body.WriteString("\n" + syncItemDetails(item) + "\n")
	}
	for _, accounting := range status.Accounting {
		fmt.Fprintf(&body, "\nShared accounting · Scope %s · Date %s\n%s\n%s\n", accounting.Scope, optionalCounter(accounting.Date), attributionDetail(accounting.Attribution), syncTotalsDetails(accounting.Totals))
	}
	body.WriteString("\nThese are shared integer accounting values, without UI arithmetic. Unavailable confirmed values are not zero. Unknown Harvest writes require read-only reconciliation; absence is not proof of nonapplication and never authorizes a new POST. Capture, upload consent and worker ownership remain separate.")
	return body.String()
}

func syncTotalsDetails(t activity.SyncTotals) string {
	return fmt.Sprintf("Exact captured %s ns\nPlanned upload %s ns\nConfirmed returned %s ns\nPlanned signed residual %s ns\nReturned signed residual %s ns", t.ExactDurationNS, optionalCounter(t.PlannedDurationNS), optionalCounter(t.ConfirmedDurationNS), optionalCounter(t.PlannedResidualNS), optionalCounter(t.TotalResidualNS))
}

func syncItemDetails(item activity.OutboxItem) string {
	var body strings.Builder
	fmt.Fprintf(&body, "Outbox %s · Revision %s · State %s\nComputer %s\n%s\nExact captured %s ns · Entry %s\n", item.ID, item.Revision, item.State, item.Interval.ComputerID, attributionDetail(item.Interval.Attribution), item.Interval.DurationNS, optionalCounter(item.EntryID))
	if item.Plan != nil {
		configuration := item.Plan.Configuration
		fmt.Fprintf(&body, "Frozen plan consent · Account %s · User %s · Configuration revision %s\nMode %s · Policy %s · Clock %s\n", configuration.AccountID, configuration.UserID, configuration.Revision, configuration.Mode, configuration.DurationPolicy, optionalCounter(configuration.Clock))
		for _, part := range item.Plan.Parts {
			fmt.Fprintf(&body, "Part %s · State %s · Date %s\nExact %s ns · Planned %s ns · Planned hours %s\nPlanned signed residual %s ns · Confirmed returned %s ns · Returned signed residual %s ns\nEntry %s · Attempt count %d\n", part.ID, part.State, part.SpentDate, part.DurationNS, part.PlannedDurationNS, part.PlannedHours, part.PlannedResidualNS, optionalCounter(part.ConfirmedDurationNS), optionalCounter(part.TotalResidualNS), optionalCounter(part.EntryID), len(part.Attempts))
		}
	}
	return body.String()
}

func syncUnavailable(ctx context.Context, p *promptBridge) error {
	return p.View(ctx, "Sync actions", "Requested Sync action unavailable.")
}

func syncFailure(ctx context.Context, p *promptBridge, err error) error {
	var ended *terminal.ExitError
	if errors.As(err, &ended) {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	code := "operation_failed"
	var domain *activity.Error
	if errors.As(err, &domain) {
		switch domain.Code {
		case "validation", "confirmation_required", "input_required", "identity_conflict", "revision_conflict", "request_conflict", "invalid_transition", "not_found", "state_busy", "state_corrupt", "unsupported_contract", "auth", "forbidden", "response", "sync_busy", "sync_paused":
			code = domain.Code
		}
	}
	return p.View(ctx, "Sync · Failed", code+"\nReview fresh read-only status and account/current-user identity before a new intent. Renew full consent and use a new request ID after a definite conflict. No automatic retry, stale revision substitution or replacement Harvest POST follows this failure.")
}
