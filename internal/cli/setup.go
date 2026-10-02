package cli

import (
	"context"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/setup"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/worker"
	"io"
)

func guidedCommand(n string) bool { return n == "setup" || n == "doctor" || n == "link" }
func authService(d Dependencies) *auth.Service {
	if d.Auth != nil {
		return d.Auth
	}
	return auth.NewService(auth.Options{ConfigPath: d.ConfigPath, Getenv: d.Getenv, NewProvider: d.NewProvider})
}
func executeGuided(ctx context.Context, p parsed, in io.Reader, out io.Writer, d Dependencies, interactive bool) (result any, err error) {
	a := activityService(d)
	service := setup.New(setup.Options{Auth: authService(d), Activity: a})
	if p.command.Name == "doctor" {
		return service.Doctor(ctx, p.flags["check"] == "true")
	}
	var prompt terminal.Prompter
	if interactive {
		prompt = d.Prompter
		if prompt == nil {
			session, e := terminal.Open(ctx, in, out)
			if e != nil {
				return nil, e
			}
			defer func() {
				if err != nil && (errors.Is(err, context.Canceled) || safeError(err).Code == "network") {
					var cause *terminal.ExitError
					if errors.As(context.Cause(session.Context()), &cause) {
						err = cause
					}
				}
				if e := session.Close(); e != nil && err == nil {
					err = &terminal.ExitError{Code: 1}
				}
			}()
			prompt = session
			ctx = session.Context()
		}
	}
	if p.command.Name == "setup" {
		if host := p.flags["host"]; host != "" && host != "codex" && host != "claude" && host != "both" {
			return nil, problem("validation", "host must be codex, claude or both")
		}
		if scope := p.flags["scope"]; scope != "" && scope != "user" && scope != "project" {
			return nil, problem("validation", "scope must be user or project")
		}
		status, err := service.Run(ctx, setup.Input{Host: p.flags["host"], Scope: p.flags["scope"], Path: p.flags["path"], AccountID: p.flags["account"]}, prompt)
		for _, step := range status.Steps {
			if step.Action == "auth.login" && step.State == "complete" {
				// A later link failure does not undo a completed credential commit.
				notifyWorker(ctx, d, worker.Recheck)
				break
			}
		}
		return status, err
	}
	id := ""
	if len(p.args) > 0 {
		id = p.args[0]
	}
	return service.Link(ctx, activity.LinkInput{ProjectID: id, TaskID: p.flags["task"], Path: p.flags["path"], AccountID: p.flags["account"], Timezone: p.flags["timezone"], IfRevision: p.flags["if-revision"], RequestID: p.flags["request-id"]}, prompt)
}
func sharedAuthCommand(n string) bool {
	switch n {
	case "auth login", "auth logout", "auth status", "accounts use", "config set-account", "accounts list":
		return true
	}
	return false
}
func executeAuth(ctx context.Context, p parsed, in io.Reader, d Dependencies) (any, error) {
	s := authService(d)
	switch p.command.Name {
	case "auth login":
		token, e := readToken(ctx, in)
		if e != nil {
			if ctx.Err() != nil {
				return nil, problem("network", "token input canceled")
			}
			return nil, problem("validation", "stdin must contain one nonempty token (maximum 16 KiB)")
		}
		attempt, e := s.PrepareLogin(ctx, []byte(token))
		if e != nil {
			return nil, e
		}
		defer attempt.Close()
		result, e := s.CommitLogin(ctx, attempt, p.flags["account"])
		if e != nil {
			var ae *auth.Error
			if errors.As(e, &ae) && ae.Code == "input_required" {
				return nil, problem("conflict", "multiple or no Harvest accounts; login again with --account ID")
			}
			return nil, authCompatibilityError(e)
		}
		notifyWorker(ctx, d, worker.Recheck)
		return map[string]any{"authenticated": true, "account_id": result.AccountID, "source": result.Source, "environment_override": result.EnvironmentOverride, "effects": result.Effects}, nil
	case "auth logout":
		r, e := s.Logout(ctx, p.flags["yes"] == "true")
		if e != nil {
			return nil, authCompatibilityError(e)
		}
		return map[string]any{"logged_out": r.LoggedOut, "environment_token_present": r.EnvironmentTokenPresent, "note": r.Note, "effects": r.Effects}, nil
	case "auth status":
		r, e := s.Status(ctx, p.flags["check"] == "true", p.flags["account"])
		if e != nil {
			return nil, e
		}
		if !r.Authenticated {
			return map[string]any{"authenticated": false, "account_id": r.AccountID, "source": "none"}, nil
		}
		out := map[string]any{"authenticated": true, "verified": r.Verified, "account_id": r.AccountID, "source": r.Source}
		if r.Verified {
			out["accounts"] = r.Accounts
		}
		return out, nil
	case "accounts use", "config set-account":
		r, e := s.UseAccount(ctx, p.args[0])
		if e != nil {
			return nil, e
		}
		notifyWorker(ctx, d, worker.Recheck)
		return map[string]string{"account_id": r.AccountID}, nil
	case "accounts list":
		return s.Accounts(ctx)
	}
	return nil, problem("usage", "unknown authentication action")
}
func authCompatibilityError(e error) error {
	var ae *auth.Error
	if !errors.As(e, &ae) {
		return e
	}
	copy := *ae
	if ae.Code == "config" {
		switch {
		case ae.Effects.Credential == "applied":
			copy.Message = "saved token removed, but account configuration could not be cleared"
		case ae.Effects.Config == "restored":
			copy.Message = "account persistence failed; previous account restored and saved token unchanged"
		case ae.Effects.Config == "unknown":
			copy.Message = "account configuration changed but could not be durably saved or restored; saved token was not changed; inspect config show"
		default:
			copy.Message = "could not save selected account; saved token was not changed"
		}
	}
	if ae.Code == "keychain" && ae.Effects.Config == "restored" {
		copy.Message = "could not save token in secure OS storage; previous account restored"
	}
	return &copy
}

// InteractiveInvocation lets main choose lifetime before applying a finite
// deadline. Parsing still rejects invalid commands before opening a terminal.
func InteractiveInvocation(args []string, in io.Reader, out io.Writer) bool {
	p, e := parse(args)
	return e == nil && (p.command.Name == "setup" || p.command.Name == "link") && p.flags["json"] != "true" && p.flags["non-interactive"] != "true" && terminal.Eligible(in, out)
}
