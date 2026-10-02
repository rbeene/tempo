package cli

import (
	"context"
	"strconv"
	"strings"

	"github.com/rbeene/tempo/internal/activity"
)

func syncCommand(name string) bool { return strings.HasPrefix(name, "sync ") }
func validateSyncCLI(p *parsed) error {
	f := p.flags
	if p.command.Name != "sync configure" && f["account"] != "" {
		return problem("usage", "sync uses the saved account; --account is only valid for configure")
	}
	if value, ok := f["limit"]; ok {
		n, e := strconv.Atoi(value)
		if e != nil || n < 1 || n > 100 {
			return problem("validation", "limit must be between 1 and 100")
		}
	}
	if p.command.Name == "sync resolve" && f["entry"] != "" && f["retry-rejected"] == "true" {
		return problem("validation", "choose entry attachment or rejected retry")
	}
	return nil
}
func executeSync(ctx context.Context, p parsed, d Dependencies) (any, error) {
	s := d.Activity
	if s == nil {
		s = activity.New(activity.Options{Path: d.Getenv("TEMPO_STATE")})
	}
	f := p.flags
	id := f["request-id"]
	if id == "" && p.command.Mutation {
		id = linkRequestID()
	}
	limit := 0
	if f["limit"] != "" {
		limit, _ = strconv.Atoi(f["limit"])
	}
	// authService creation does not access credentials. Provider resolves them only
	// when the service reaches an account-bound remote operation after replay.
	deps := activity.SyncDependencies{NewProvider: authService(d).Provider}
	switch p.command.Name {
	case "sync status":
		return s.SyncStatus(ctx)
	case "sync pause":
		return s.SyncPause(ctx, id)
	case "sync resume":
		return s.SyncResume(ctx, id)
	case "sync configure":
		return s.SyncConfigure(ctx, activity.SyncConfigureInput{AccountID: f["account"], Mode: f["mode"], DurationPolicy: f["duration-policy"], Clock: f["clock"], IfRevision: f["if-revision"], RequestID: id, Confirmed: f["yes"] == "true"}, deps)
	case "sync now":
		return s.SyncNow(ctx, activity.SyncRunInput{RequestID: id, Limit: limit}, deps)
	case "sync reconcile":
		root := ""
		if len(p.args) > 0 {
			root = p.args[0]
		}
		return s.SyncReconcile(ctx, activity.SyncReconcileInput{RequestID: id, OutboxID: root, Limit: limit}, deps)
	case "sync resolve":
		return s.SyncResolve(ctx, activity.SyncResolveInput{RequestID: id, OutboxID: p.args[0], EntryID: f["entry"], IfRevision: f["if-revision"], RetryRejected: f["retry-rejected"] == "true", Confirmed: f["yes"] == "true"}, deps)
	}
	return nil, problem("usage", "unknown sync command")
}
