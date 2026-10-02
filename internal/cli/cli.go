// Package cli exposes Tempo's command boundary for isolated tests and embedding.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/ui"
	"github.com/rbeene/tempo/internal/worker"
)

var Version = "dev"

type Dependencies struct {
	Hooks            *hookstate.Service
	Worker           *worker.Service
	Auth             *auth.Service
	Prompter         terminal.Prompter
	TerminalEligible func(io.Reader, io.Writer) bool
	Activity         *activity.Service
	Store            auth.Store
	ConfigPath       string
	Getenv           func(string) string
	NewProvider      func(token, account string) harvest.Provider
	Now              func() time.Time
	SaveConfig       func(string, auth.Config) error
}
type cliError struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Uncertain bool           `json:"uncertain"`
	Details   map[string]any `json:"details,omitempty"`
}

func (e *cliError) Error() string    { return e.Message }
func problem(code, msg string) error { return &cliError{Code: code, Message: msg} }
func safeError(err error) *cliError {
	var ce *cliError
	if errors.As(err, &ce) {
		return ce
	}
	var ae *activity.Error
	if errors.As(err, &ae) {
		return &cliError{Code: ae.Code, Message: ae.Message, Retryable: ae.Retryable, Uncertain: ae.Uncertain, Details: ae.Details}
	}
	var hookErr *hookstate.Error
	if errors.As(err, &hookErr) {
		details := map[string]any{}
		if hookErr.RequestID != "" {
			details["request_id"] = hookErr.RequestID
		}
		return &cliError{Code: hookErr.Code, Message: "hook lifecycle operation failed; inspect hooks status and preserve the request ID", Retryable: hookErr.Retryable, Uncertain: hookErr.Uncertain, Details: details}
	}
	var we *worker.Error
	if errors.As(err, &we) {
		return &cliError{Code: we.Code, Message: we.Message, Retryable: we.Retryable, Uncertain: we.Uncertain}
	}
	var authErr *auth.Error
	if errors.As(err, &authErr) {
		details := map[string]any{"effects": authErr.Effects}
		if len(authErr.RequiredFields) > 0 {
			details["required_fields"] = authErr.RequiredFields
		}
		return &cliError{Code: authErr.Code, Message: authErr.Message, Retryable: authErr.Retryable, Uncertain: authErr.Uncertain, Details: details}
	}
	if errors.Is(err, auth.ErrNotFound) {
		return &cliError{Code: "auth", Message: "no token configured; connect securely or use HARVEST_TOKEN"}
	}
	var he *harvest.Error
	if errors.As(err, &he) {
		return &cliError{Code: he.Code, Message: apiMessage(he.Code), Retryable: he.Retryable, Uncertain: he.Uncertain}
	}
	return &cliError{Code: "internal", Message: "operation failed; check configuration and credential storage"}
}
func exitCode(code string) int {
	switch code {
	case "usage", "validation", "input_required", "invalid_transition", "recovery_bounds", "unsupported_contract":
		return 2
	case "auth":
		return 3
	case "forbidden":
		return 4
	case "not_found", "binding_unavailable", "actor_not_found", "uncertainty_not_found":
		return 5
	case "conflict", "confirmation_required", "attribution_conflict", "binding_in_use", "revision_conflict", "request_conflict", "event_conflict", "event_gap", "ordering_unavailable", "clock_conflict", "state_busy":
		return 6
	case "network", "api", "rate_limit", "response":
		return 7
	case "uncertain_write", "local_write_unknown", "credential_write_unknown":
		return 8
	default:
		return 1
	}
}

// Run never exits the process. Dependencies permit tests without personal config,
// OS credential access or real API requests. Writes are explicit commands only.
func Run(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer, d Dependencies) int {
	eligible := terminal.Eligible
	if d.TerminalEligible != nil {
		eligible = d.TerminalEligible
	}
	jsonMode := wantsJSON(args) || wantsLocalJSON(args, !eligible(in, out))
	p, err := parse(args)
	if (strings.HasPrefix(p.command.Name, "activity ") || linkCommand(p.command.Name)) && p.flags["non-interactive"] == "true" {
		jsonMode = true
	}
	var data any
	if err == nil {
		if d.Now == nil {
			d.Now = time.Now
		}
		if d.Getenv == nil {
			d.Getenv = os.Getenv
		}
		err = validate(&p, d.Now())
		if err == nil {
			if p.command.Name == "hook codex" || p.command.Name == "hook claude" {
				return runHook(ctx, strings.TrimPrefix(p.command.Name, "hook "), in, out, errOut, d)
			}
			if p.command.Name == "ui" || p.command.Name == "activity status" && p.flags["watch"] == "true" {
				if eligible(in, out) && p.flags["json"] != "true" && p.flags["non-interactive"] != "true" {
					session, openErr := terminal.Open(ctx, in, out)
					if openErr != nil {
						err = openErr
					} else {
						errOut = &uiDiagnosticWriter{writer: errOut}
						a := activityService(d)
						views := &ui.ReadViews{Links: a.ListBindings, Sync: a.SyncStatus, Setup: func(readCtx context.Context) (setup.Status, error) {
							service, err := setupService(d)
							if err != nil {
								return setup.Status{}, err
							}
							return service.Run(readCtx, setup.Input{}, nil)
						}}
						links := &ui.LinkActions{Prepare: func(actionCtx context.Context, input activity.LinkInput, prompt terminal.Prompter) (activity.LinkInput, error) {
							service := setup.New(setup.Options{Auth: authService(d), Activity: a})
							return service.PrepareLink(actionCtx, input, prompt)
						}, Commit: func(actionCtx context.Context, input activity.LinkInput) (activity.BindingResult, error) {
							service := setup.New(setup.Options{Auth: authService(d), Activity: a})
							return service.CommitLink(actionCtx, input)
						}, Unlink: a.Unlink, Repair: a.RepairBinding}
						authActions := uiAuthActions(d)
						capture := &ui.ActivityActions{Status: a.Status, Review: a.Review, Preview: a.Preview, Resolve: a.Resolve, Interrupt: a.Interrupt}
						err = uiResultError(ui.Run(session.Context(), session, a, ui.Options{Views: views, Links: links, Activity: capture, Auth: authActions, Hooks: uiHookActions(d), Worker: uiWorkerActions(d), Diagnostics: func(readCtx context.Context, check bool) (setup.Diagnostics, error) {
							service, err := setupService(d)
							if err != nil {
								return setup.Diagnostics{}, err
							}
							return service.Doctor(readCtx, check)
						}, OnAuthResult: func(operation string, result auth.Result, outcome error) {
							uiAuthReport(errOut, operation, result, outcome)
						}, OnRetainedOutcome: func(family, requestID string, outcome error) {
							uiRetainedReport(errOut, family, requestID, outcome)
						}, OnRestorationFailure: func() { uiRestorationReport(errOut) }}))
						if err == nil {
							return 0
						}
					}
				} else {
					jsonMode = true
					data, err = activityService(d).Status(ctx)
				}
			} else if guidedCommand(p.command.Name) {
				interactive := eligible(in, out) && p.flags["json"] != "true" && p.flags["non-interactive"] != "true"
				if !interactive {
					jsonMode = true
				}
				data, err = executeGuided(ctx, p, in, out, d, interactive)
			} else if sharedAuthCommand(p.command.Name) {
				data, err = executeAuth(ctx, p, in, d)
			} else {
				data, err = execute(ctx, p, in, d)
			}
		}
	}
	if err != nil {
		completed := []setup.Step{}
		if status, ok := data.(setup.Status); ok {
			for _, step := range status.Steps {
				if step.State == "complete" {
					completed = append(completed, step)
				}
			}
		}
		if len(completed) > 0 && !jsonMode {
			for _, step := range completed {
				fmt.Fprintf(errOut, "tempo: completed %s: %s\n", step.Action, step.SafeMessage)
			}
		}
		var ended *terminal.ExitError
		if errors.As(err, &ended) {
			if ended.Code == 1 {
				fmt.Fprintln(errOut, "tempo: terminal: terminal input or output failed")
			}
			return ended.Code
		}
		e := safeError(err)
		if len(completed) > 0 {
			if e.Details == nil {
				e.Details = map[string]any{}
			}
			e.Details["completed_steps"] = completed
		}
		if errors.Is(err, context.Canceled) || e.Code == "network" {
			var cause *terminal.ExitError
			if errors.As(context.Cause(ctx), &cause) {
				return cause.Code
			}
		}
		if jsonMode {
			_ = json.NewEncoder(errOut).Encode(map[string]any{"schema_version": 1, "error": e})
		} else {
			fmt.Fprintf(errOut, "tempo: %s: %s\n", e.Code, e.Message)
		}
		return exitCode(e.Code)
	}
	if p.command.Name == "worker run" {
		return 0
	}
	if jsonMode || p.command.Name == "schema" {
		if err := json.NewEncoder(out).Encode(map[string]any{"schema_version": 1, "data": data}); err != nil {
			return 1
		}
		return 0
	}
	if p.command.Name == "help" {
		printHelp(out)
		return 0
	}
	if p.command.Name == "version" {
		fmt.Fprintln(out, "tempo "+Version)
		return 0
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if enc.Encode(data) != nil {
		return 1
	}
	return 0
}
func printHelp(w io.Writer) {
	fmt.Fprint(w, "Tempo — personal Harvest time tracking\n\nUsage: tempo <command> [options]\n\nGlobal options: --json  --account ID  --yes  --non-interactive\n")
	for _, c := range commands {
		fmt.Fprintf(w, "  %-20s %-5s %s\n", c.Name, c.Positionals, c.Summary)
		keys := []string{}
		for k := range c.Flags {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if len(keys) > 0 {
			fmt.Fprint(w, "      ")
			for _, k := range keys {
				fmt.Fprintf(w, "--%s (%s)  ", k, c.Flags[k])
			}
			fmt.Fprintln(w)
		}
	}
	fmt.Fprintln(w, "\nUse tempo schema for types, result envelopes, exit codes and environment inputs.\nDestructive commands require --yes. Eligible interactive setup offers hidden secret input.\nUse --json or --non-interactive to disable prompts.\nLogin: obtain a personal token at https://id.getharvest.com/developers and pass it via stdin.\nSee docs/commands.md for examples and safe secret input.")
}

func validate(p *parsed, now time.Time) error {
	f := p.flags
	n := p.command.Name
	if hooksCommand(n) {
		return validateHooksCLI(p)
	}
	if workerCommand(n) {
		return validateWorkerCLI(p)
	}
	if syncCommand(n) {
		return validateSyncCLI(p)
	}
	if recoveryCommand(n) {
		return validateRecoveryCLI(p)
	}
	for _, k := range []string{"date", "from", "to"} {
		if v, ok := f[k]; ok {
			s, e := date(v, now)
			if e != nil {
				return e
			}
			f[k] = s
		}
	}
	if f["from"] != "" && f["to"] != "" && f["from"] > f["to"] {
		return problem("validation", "--from must not follow --to")
	}
	for _, k := range []string{"start", "end"} {
		if v, ok := f[k]; ok {
			if _, e := clockTime(v); e != nil {
				return e
			}
		}
	}
	if f["start"] != "" && f["end"] != "" && f["end"] <= f["start"] {
		return problem("validation", "--end must follow --start on the same day; split overnight time")
	}
	_, dur := f["duration"]
	_, hrs := f["hours"]
	_, start := f["start"]
	_, end := f["end"]
	if dur && hrs || (dur || hrs) && (start || end) {
		return problem("validation", "use exactly one duration format or start/end times")
	}
	for _, k := range []string{"duration", "hours"} {
		if v, ok := f[k]; ok {
			if _, e := hours(v); e != nil {
				return e
			}
		}
	}
	switch n {
	case "hook codex", "hook claude":
		if f["input-stdin"] != "true" {
			return problem("usage", n+" requires --input-stdin")
		}
		if _, ok := f["account"]; ok {
			return problem("usage", "hook capture does not accept account overrides")
		}
		if _, ok := f["yes"]; ok {
			return problem("usage", "hook capture does not accept confirmation flags")
		}
	case "links show":
		if len(p.args) > 0 && f["path"] != "" {
			return problem("validation", "use binding ID or --path, not both")
		}
	case "links unlink", "links repair":
		if f["if-revision"] == "" {
			return problem("validation", "binding mutation requires --if-revision")
		}
		if f["yes"] != "true" {
			return problem("confirmation_required", "this command requires explicit --yes")
		}
		if n == "links repair" && f["path"] == "" {
			return problem("validation", "repair requires --path")
		}
	case "activity event":
		if f["input-stdin"] != "true" {
			return problem("validation", "activity event requires --input-stdin")
		}
	case "time create":
		if f["project"] == "" || f["task"] == "" || f["date"] == "" {
			return problem("validation", "create requires --project, --task and --date")
		}
		if !dur && !hrs && !(start && end) {
			return problem("validation", "create requires --duration, --hours or both --start and --end")
		}
	case "time update":
		count := 0
		for k := range entryFlags() {
			if _, ok := f[k]; ok {
				count++
			}
		}
		if count == 0 {
			return problem("validation", "update requires at least one changed field")
		}
		// A complete timestamp pair prevents accidentally restarting a stopped entry.
		if start != end {
			return problem("validation", "timestamp updates require both --start and --end")
		}
	case "timer start":
		if len(p.args) == 1 {
			for k := range p.command.Flags {
				if _, ok := f[k]; ok {
					return problem("validation", "restart by ID does not accept new-entry options")
				}
			}
		} else {
			if f["project"] == "" || f["task"] == "" {
				return problem("validation", "new timer requires --project and --task")
			}
			if f["date"] == "" {
				f["date"] = now.Format("2006-01-02")
			}
		}
	case "time delete", "auth logout":
		if f["yes"] != "true" {
			return problem("confirmation_required", "this command requires explicit --yes")
		}
	case "auth login":
		if f["token-stdin"] != "true" {
			return problem("validation", "use --token-stdin with a token from https://id.getharvest.com/developers")
		}
	case "tasks list":
		if f["all"] == "true" && f["project"] != "" {
			return problem("validation", "use --project or --all")
		}
	}
	return nil
}

type session struct {
	ctx     context.Context
	p       parsed
	d       Dependencies
	account string
	api     harvest.Provider
	user    string
}

func execute(ctx context.Context, p parsed, in io.Reader, d Dependencies) (any, error) {
	if hooksCommand(p.command.Name) {
		return executeHooks(ctx, p, d)
	}
	if workerCommand(p.command.Name) {
		return executeWorker(ctx, p, d)
	}
	if syncCommand(p.command.Name) {
		return executeSync(ctx, p, d)
	}
	switch p.command.Name {
	case "help":
		return schema(), nil
	case "schema":
		return schema(), nil
	case "version":
		return map[string]string{"version": Version}, nil
	}

	if linkCommand(p.command.Name) {
		return executeLinks(ctx, p, d)
	}
	if strings.HasPrefix(p.command.Name, "activity ") {
		service := activityService(d)
		if recoveryCommand(p.command.Name) {
			return executeRecovery(ctx, p, service)
		}
		if p.command.Name == "activity status" {
			return service.Status(ctx)
		}
		eventCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if f, ok := in.(*os.File); ok {
			deadline, _ := eventCtx.Deadline()
			reader, cleanup, err := deadlineActivityFile(f, deadline)
			if err != nil {
				return nil, problem("validation", "cannot safely apply activity input deadline")
			}
			defer cleanup()
			in = reader
		}
		event, err := activity.DecodeEvent(in)
		if err != nil {
			return nil, err
		}
		if eventCtx.Err() != nil {
			return nil, problem("validation", "activity event input deadline exceeded")
		}
		result, err := service.Ingest(eventCtx, event)
		if err == nil && (result.Disposition == "applied" || result.Disposition == "duplicate") {
			notifyWorker(eventCtx, d, worker.Wake)
		}
		return result, err
	}
	if p.command.Name == "config show" {
		return authService(d).ConfigShow(ctx, p.flags["account"])
	}
	path := d.ConfigPath
	if path == "" {
		path = d.Getenv("TEMPO_CONFIG")
		if path == "" {
			var e error
			path, e = auth.ConfigPath()
			if e != nil {
				return nil, problem("config", "cannot locate configuration")
			}
		}
	}
	cfg, err := auth.Load(path)
	if err != nil {
		return nil, problem("config", "cannot read configuration; inspect TEMPO_CONFIG and account_id")
	}
	s := &session{ctx: ctx, p: p, d: d}
	s.account = p.flags["account"]
	if s.account == "" {
		s.account = d.Getenv("HARVEST_ACCOUNT_ID")
	}
	if s.account == "" {
		s.account = cfg.Account
	}
	if s.account != "" && !validID(s.account) {
		return nil, problem("validation", "selected account ID must be a positive integer")
	}
	s.api, err = authService(d).Provider(ctx, s.account)
	if err != nil {
		return nil, err
	}
	if s.account == "" {
		return nil, problem("auth", "select an account with accounts use ID, --account ID or HARVEST_ACCOUNT_ID")
	}
	return s.dispatch()
}
func idOf(o harvest.Object) string {
	if o == nil {
		return ""
	}
	switch v := o["id"].(type) {
	case json.Number:
		return string(v)
	case string:
		return v
	case int:
		return fmt.Sprint(v)
	case int64:
		return fmt.Sprint(v)
	case float64:
		return fmt.Sprintf("%.0f", v)
	}
	return ""
}
func nested(o harvest.Object, key string) harvest.Object { v, _ := o[key].(map[string]any); return v }
func (s *session) me() (string, error) {
	if s.user != "" {
		return s.user, nil
	}
	o, e := s.api.Get(s.ctx, "/users/me")
	if e != nil {
		return "", e
	}
	id := idOf(o)
	if !validID(id) {
		return "", problem("response", "Harvest did not return a valid user ID")
	}
	s.user = id
	return id, nil
}
func (s *session) own(id string) (harvest.Object, error) {
	user, e := s.me()
	if e != nil {
		return nil, e
	}
	o, e := s.api.Get(s.ctx, "/time_entries/"+id)
	if e != nil {
		return nil, e
	}
	if idOf(o) != id {
		return nil, problem("response", "Harvest returned a different time entry")
	}
	if idOf(nested(o, "user")) != user {
		return nil, problem("forbidden", "time entry belongs to a different user")
	}
	return o, nil
}

// Keep a second redaction boundary when a future provider is introduced.
func apiMessage(code string) string {
	switch code {
	case "auth":
		return "Harvest authentication failed; check the configured token"
	case "forbidden":
		return "Harvest denied access to this operation"
	case "not_found", "binding_unavailable", "actor_not_found", "uncertainty_not_found":
		return "Harvest could not find the requested resource"
	case "validation":
		return "Harvest rejected the request; check project/task access, company mode and supplied fields"
	case "rate_limit":
		return "Harvest rate limit reached; try again later"
	case "network":
		return "Harvest request failed or was canceled"
	case "response":
		return "Harvest returned an invalid or incomplete response"
	case "uncertain_write", "local_write_unknown", "credential_write_unknown":
		return "write outcome is uncertain; inspect time list/show or timer status before retrying manually"
	default:
		return "Harvest could not complete the request"
	}
}

// readToken returns promptly on cancellation. Closing a pipe/file input also
// releases its blocked read; the executable then exits without retaining input.
func readToken(ctx context.Context, in io.Reader) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	type result struct {
		token string
		err   error
	}
	done := make(chan result, 1)
	go func() { token, err := auth.ReadToken(in); done <- result{token, err} }()
	select {
	case r := <-done:
		return r.token, r.err
	case <-ctx.Done():
		if closer, ok := in.(io.Closer); ok {
			_ = closer.Close()
		}
		return "", ctx.Err()
	}
}
