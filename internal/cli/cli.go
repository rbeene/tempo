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
	"github.com/rbeene/tempo/internal/terminal"
)

var Version = "dev"

type Dependencies struct {
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
	jsonMode := wantsJSON(args) || wantsLocalJSON(args)
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
			if guidedCommand(p.command.Name) {
				eligible := terminal.Eligible
				if d.TerminalEligible != nil {
					eligible = d.TerminalEligible
				}
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
		var ended *terminal.ExitError
		if errors.As(err, &ended) {
			return ended.Code
		}
		e := safeError(err)
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
	fmt.Fprintln(w, "\nUse tempo schema for types, result envelopes, exit codes and environment inputs.\nDestructive commands require --yes. No command prompts for secrets.\nLogin: obtain a personal token at https://id.getharvest.com/developers and pass it via stdin.\nSee docs/commands.md for examples and safe secret input.")
}

func validate(p *parsed, now time.Time) error {
	f := p.flags
	n := p.command.Name
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
	case "link":
		if len(p.args) == 0 {
			return &activity.Error{Code: "input_required", Message: "an explicit project ID is required", Details: map[string]any{"required_fields": []string{"project_id"}}}
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
	ctx                    context.Context
	p                      parsed
	d                      Dependencies
	config                 auth.Config
	path                   string
	token, source, account string
	api                    harvest.Provider
	user                   string
}

func execute(ctx context.Context, p parsed, in io.Reader, d Dependencies) (any, error) {
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
		service := d.Activity
		if service == nil {
			service = activity.New(activity.Options{Path: d.Getenv("TEMPO_STATE")})
		}
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
		return service.Ingest(eventCtx, event)
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
	if d.SaveConfig == nil {
		d.SaveConfig = auth.Save
	}
	if p.command.Name == "auth logout" {
		if d.Store == nil {
			d.Store = auth.NewStore()
		}
		if e := d.Store.Delete(); e != nil && !errors.Is(e, auth.ErrNotFound) {
			return nil, problem("keychain", "could not remove saved token")
		}
		if e := d.SaveConfig(path, auth.Config{}); e != nil {
			return nil, problem("config", "saved token removed, but account configuration could not be cleared")
		}
		return map[string]any{"logged_out": true, "environment_token_present": d.Getenv("HARVEST_TOKEN") != "", "note": "logout removes local storage only; unset HARVEST_TOKEN separately and revoke tokens in Harvest if needed"}, nil
	}
	cfg, err := auth.Load(path)
	if err != nil {
		return nil, problem("config", "cannot read configuration; inspect TEMPO_CONFIG and account_id")
	}
	s := &session{ctx: ctx, p: p, d: d, config: cfg, path: path}
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
	if p.command.Name == "config show" {
		return map[string]any{"path": path, "saved_account_id": cfg.Account, "account_id": s.account, "token_stored_in_config": false}, nil
	}
	if s.d.Store == nil {
		s.d.Store = auth.NewStore()
	}
	if s.d.NewProvider == nil {
		s.d.NewProvider = func(token, account string) harvest.Provider { return harvest.New(token, account) }
	}
	if p.command.Name == "auth login" {
		return s.login(in)
	}

	s.token, s.source, err = auth.ResolveToken(s.d.Store, d.Getenv)
	if err != nil {
		if p.command.Name == "auth status" && errors.Is(err, auth.ErrNotFound) {
			return map[string]any{"authenticated": false, "account_id": s.account, "source": "none"}, nil
		}
		if errors.Is(err, auth.ErrNotFound) {
			return nil, problem("auth", "no token configured; use auth login --token-stdin or HARVEST_TOKEN")
		}
		return nil, problem("keychain", "cannot access token; unlock Keychain or use HARVEST_TOKEN")
	}
	if p.command.Name == "auth status" && p.flags["check"] != "true" {
		return map[string]any{"authenticated": true, "verified": false, "account_id": s.account, "source": s.source}, nil
	}
	s.api = s.d.NewProvider(s.token, s.account)
	switch p.command.Name {
	case "auth status":
		accounts, e := s.api.Accounts(ctx)
		if e != nil {
			return nil, e
		}
		if s.account != "" && !hasAccount(accounts, s.account) {
			return nil, problem("forbidden", "selected account is not accessible with this token")
		}
		return map[string]any{"authenticated": true, "verified": true, "source": s.source, "account_id": s.account, "accounts": accounts}, nil
	case "accounts list":
		return s.api.Accounts(ctx)
	case "accounts use", "config set-account":
		accounts, e := s.api.Accounts(ctx)
		if e != nil {
			return nil, e
		}
		id := p.args[0]
		if !hasAccount(accounts, id) {
			return nil, problem("forbidden", "requested Harvest account is not accessible")
		}
		if e = s.d.SaveConfig(path, auth.Config{Account: id}); e != nil {
			return nil, problem("config", "could not save selected account")
		}
		return map[string]string{"account_id": id}, nil
	}
	if s.account == "" {
		return nil, problem("auth", "select an account with accounts use ID, --account ID or HARVEST_ACCOUNT_ID")
	}
	return s.dispatch()
}
func (s *session) login(in io.Reader) (any, error) {
	token, e := readToken(s.ctx, in)
	if e != nil {
		if s.ctx.Err() != nil {
			return nil, problem("network", "token input canceled")
		}
		return nil, problem("validation", "stdin must contain one nonempty token (maximum 16 KiB)")
	}
	accounts, e := s.d.NewProvider(token, "").Accounts(s.ctx)
	if e != nil {
		return nil, e
	}
	selected := s.account
	if selected == "" {
		if len(accounts) != 1 {
			return nil, problem("conflict", "multiple or no Harvest accounts; login again with --account ID")
		}
		selected = idOf(accounts[0])
	}
	if !hasAccount(accounts, selected) {
		return nil, problem("forbidden", "selected Harvest account is not accessible")
	}
	// Prepare nonsecret configuration first; replacing the token is the final
	// fallible step. Failed token storage restores the previous account choice.
	if e = s.d.SaveConfig(s.path, auth.Config{Account: selected}); e != nil {
		var saveErr *auth.SaveError
		if errors.As(e, &saveErr) && saveErr.Replaced {
			if restore := s.d.SaveConfig(s.path, s.config); restore != nil {
				return nil, problem("config", "account configuration changed but could not be durably saved or restored; saved token was not changed; inspect config show")
			}
			return nil, problem("config", "account persistence failed; previous account restored and saved token unchanged")
		}
		return nil, problem("config", "could not save selected account; saved token was not changed")
	}
	if e = s.d.Store.Set(token); e != nil {
		if restore := s.d.SaveConfig(s.path, s.config); restore != nil {
			return nil, problem("config", "token storage failed and the previous account could not be restored; select the account again")
		}
		return nil, problem("keychain", "could not save token in secure OS storage; previous account restored")
	}

	return map[string]any{"authenticated": true, "account_id": selected, "source": "keychain", "environment_override": s.d.Getenv("HARVEST_TOKEN") != ""}, nil
}
func hasAccount(accounts []harvest.Object, id string) bool {
	for _, a := range accounts {
		if idOf(a) == id && a["product"] == "harvest" {
			return true
		}
	}
	return false
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
