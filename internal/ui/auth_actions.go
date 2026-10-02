package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
	"github.com/rbeene/tempo/internal/terminal"
)

// AuthActions delegates credential and account operations to the shared service.
// Credential writes have no request ledger or same-ID replay guarantee.
type AuthActions struct {
	CanPersist   func() bool
	Status       func(context.Context, bool, string) (auth.Result, error)
	Accounts     func(context.Context) ([]harvest.Object, error)
	PrepareLogin func(context.Context, []byte) (*auth.LoginAttempt, error)
	CommitLogin  func(context.Context, *auth.LoginAttempt, string) (auth.Result, error)
	Logout       func(context.Context, bool) (auth.Result, error)
	UseAccount   func(context.Context, string) (auth.Result, error)
	ConfigShow   func(context.Context, string) (auth.ConfigStatus, error)
}

type authCompletion struct {
	operation string
	result    auth.Result
	err       error
}

type authController struct {
	pending            error
	operation, account string
	completed          *authCompletion
}

func (c *authController) run(ctx context.Context, p *promptBridge, actions *AuthActions) error {
	if c.pending != nil {
		return c.inspect(ctx, p, actions)
	}
	if actions == nil {
		return p.View(ctx, "Accounts and auth", "Authentication actions unavailable.")
	}
	id, err := p.Choose(ctx, "Accounts and auth", []terminal.Choice{
		{ID: "status", Label: "Inspect credential status"},
		{ID: "check", Label: "Verify authentication with Harvest"},
		{ID: "config", Label: "Inspect account configuration"},
		{ID: "accounts", Label: "Choose default Harvest account"},
		{ID: "login", Label: "Save a credential securely"},
		{ID: "logout", Label: "Remove saved credential and account"},
		{ID: "back", Label: "Back"},
	})
	if err != nil {
		return err
	}
	switch id {
	case "back":
		return nil
	case "status", "check", "config":
		return authRead(ctx, p, actions, id)
	case "login":
		return c.login(ctx, p, actions)
	case "accounts":
		if actions.Accounts == nil {
			return authUnavailable(ctx, p)
		}
		accounts, err := authCall(ctx, 2*time.Minute, actions.Accounts)
		if err != nil {
			return authFailure(ctx, p, err)
		}
		account, err := chooseAuthAccount(ctx, p, accounts)
		if err != nil {
			return authFailure(ctx, p, err)
		}
		yes, err := p.Confirm(ctx, "Save default account "+account+" in local config? Environment and explicit account overrides still take precedence. This does not change the saved credential.")
		if err != nil || !yes {
			return err
		}
		if actions.UseAccount == nil {
			return authUnavailable(ctx, p)
		}
		result, err := authCall(ctx, 2*time.Minute, func(ctx context.Context) (auth.Result, error) { return actions.UseAccount(ctx, account) })
		return c.finish(ctx, p, actions, "account selection", account, result, err)
	case "logout":
		yes, err := p.Confirm(ctx, "Remove the saved credential and clear account config? This removes only local storage; it does not unset environment authentication or revoke a token in Harvest. HARVEST_TOKEN must be unset separately.")
		if err != nil || !yes {
			return err
		}
		if actions.Logout == nil {
			return authUnavailable(ctx, p)
		}
		result, err := authCall(ctx, 2*time.Minute, func(ctx context.Context) (auth.Result, error) { return actions.Logout(ctx, true) })
		return c.finish(ctx, p, actions, "logout", "", result, err)
	default:
		return authFailure(ctx, p, &auth.Error{Code: "validation"})
	}
}

func (c *authController) login(ctx context.Context, p *promptBridge, actions *AuthActions) error {
	if actions.CanPersist == nil || !actions.CanPersist() {
		return p.View(ctx, "Accounts and auth", "Secure credential storage is unavailable. Supply HARVEST_TOKEN securely in the environment; no token will be collected here.")
	}
	if actions.PrepareLogin == nil {
		return authUnavailable(ctx, p)
	}
	token, err := p.Secret(ctx, "Harvest personal access token (hidden)")
	if err != nil {
		clear(token[:cap(token)])
		return err
	}
	attempt, err := authCall(ctx, 2*time.Minute, func(ctx context.Context) (*auth.LoginAttempt, error) { return actions.PrepareLogin(ctx, token) })
	// The shared attempt owns its own secret. The prompt's bytes must not live
	// across account selection, confirmation, or credential dispatch.
	clear(token[:cap(token)])
	defer attempt.Close()
	if err != nil {
		attempt.Close()
		return authFailure(ctx, p, err)
	}
	if attempt == nil {
		return authFailure(ctx, p, &auth.Error{Code: "response"})
	}
	account, err := chooseAuthAccount(ctx, p, attempt.Accounts)
	if err != nil {
		attempt.Close()
		return authFailure(ctx, p, err)
	}
	yes, err := p.Confirm(ctx, "Replace the saved credential and save account "+account+" in local config? HARVEST_TOKEN in the environment still overrides the saved credential. Each credential operation is submitted once; an unknown outcome requires inspection before an explicit replacement.")
	if err != nil || !yes {
		return err
	}
	if actions.CommitLogin == nil {
		attempt.Close()
		return authUnavailable(ctx, p)
	}
	result, err := authCall(ctx, 2*time.Minute, func(ctx context.Context) (auth.Result, error) { return actions.CommitLogin(ctx, attempt, account) })
	// No secret needs to remain while a result or uncertainty is inspected.
	attempt.Close()
	return c.finish(ctx, p, actions, "login", account, result, err)
}

func authCall[T any](ctx context.Context, budget time.Duration, call func(context.Context) (T, error)) (T, error) {
	bounded, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	if bounded.Err() != nil {
		var zero T
		return zero, context.Cause(bounded)
	}
	return call(bounded)
}

func chooseAuthAccount(ctx context.Context, p *promptBridge, accounts []harvest.Object) (string, error) {
	choices := []terminal.Choice{}
	for _, account := range accounts {
		if account["product"] != "harvest" {
			continue
		}
		var id string
		switch value := account["id"].(type) {
		case string:
			id = value
		case json.Number:
			id = string(value)
		}
		if !identity.Valid(id) {
			continue
		}
		name, _ := account["name"].(string)
		choices = append(choices, terminal.Choice{ID: id, Label: terminal.Sanitize(name) + " · account " + id})
	}
	if len(choices) == 0 {
		return "", &auth.Error{Code: "input_required"}
	}
	id, err := p.Choose(ctx, "Choose a Harvest account", choices)
	if err != nil {
		return "", err
	}
	for _, choice := range choices {
		if choice.ID == id {
			return id, nil
		}
	}
	return "", &auth.Error{Code: "validation"}
}

func authRead(ctx context.Context, p *promptBridge, actions *AuthActions, id string) error {
	if actions == nil {
		return authUnavailable(ctx, p)
	}
	if id == "config" {
		if actions.ConfigShow == nil {
			return authUnavailable(ctx, p)
		}
		result, err := authCall(ctx, 250*time.Millisecond, func(ctx context.Context) (auth.ConfigStatus, error) { return actions.ConfigShow(ctx, "") })
		if err != nil {
			return authFailure(ctx, p, err)
		}
		return p.View(ctx, "Auth config", fmt.Sprintf("Config path %s\nSaved account %s\nEffective account %s\nToken stored in config: false\nExplicit account and environment selection override the saved account. No credentials were read.", result.Path, result.SavedAccountID, result.AccountID))
	}
	if actions.Status == nil {
		return authUnavailable(ctx, p)
	}
	budget := 250 * time.Millisecond
	if id == "check" {
		budget = 2 * time.Minute
	}
	result, err := authCall(ctx, budget, func(ctx context.Context) (auth.Result, error) { return actions.Status(ctx, id == "check", "") })
	if err != nil {
		return authFailure(ctx, p, err)
	}
	return p.View(ctx, "Auth status", authResult(result)+"\nRead-only observation; status does not establish nonapplication or causal certainty for an earlier write.")
}

func (c *authController) finish(ctx context.Context, p *promptBridge, actions *AuthActions, operation, account string, result auth.Result, err error) error {
	if err == nil {
		// Retain the shared reply before presenting it: terminal cancellation
		// may prevent the view, but cannot erase an acknowledged local effect.
		c.completed = &authCompletion{operation: operation, result: result}
		return p.View(ctx, "Auth complete", authResult(result))
	}
	if !unknownOutcome(err) {
		var domain *auth.Error
		if errors.As(err, &domain) {
			effects := domain.Effects
			credentialChanged := effects.Credential == "applied" || effects.Credential == "unknown"
			configChanged := effects.Config == "saved" || effects.Config == "cleared" || effects.Config == "restored" || effects.Config == "unknown"
			if credentialChanged || configChanged {
				c.completed = &authCompletion{operation: operation, result: result, err: err}
			}
		}
		return authFailure(ctx, p, err)
	}
	c.pending, c.operation, c.account = err, operation, account
	body := c.unknownDetails()
	if p.View(ctx, "Auth outcome unknown", body) != nil {
		return c.pending
	}
	return c.inspect(ctx, p, actions)
}

func (c *authController) unknownDetails() string {
	var domain *auth.Error
	effects := "Credential effect unknown\nConfig effect unknown"
	if errors.As(c.pending, &domain) {
		effects = authEffects(domain.Effects)
	}
	return "credential_write_unknown: the submitted operation may have applied.\nOperation " + c.operation + "\nAccount " + c.account + "\n" + effects + "\nInspect auth status and config show. Read-only observations do not prove nonapplication or causal certainty. No automatic retry or replay is available. An explicit replacement must be reviewed separately after inspection."
}

func (c *authController) inspect(ctx context.Context, p *promptBridge, actions *AuthActions) error {
	for {
		id, err := p.Choose(ctx, "Auth outcome unknown · Inspect", []terminal.Choice{{ID: "status", Label: "Read-only credential status"}, {ID: "check", Label: "Read-only Harvest verification"}, {ID: "config", Label: "Read-only account config"}, {ID: "back", Label: "Back; retain unknown outcome"}})
		if err != nil || id == "back" {
			return c.pending
		}
		if id != "status" && id != "check" && id != "config" {
			return c.pending
		}
		if authRead(ctx, p, actions, id) != nil {
			return c.pending
		}
	}
}

func authResult(result auth.Result) string {
	source := "none"
	if result.Source == "environment" || result.Source == "keychain" {
		source = result.Source
	}
	return fmt.Sprintf("Account %s\nAuthenticated %t · Verified %t · Logged out %t\nCredential source %s\nEnvironment override %t · Environment token present %t\n%s\nLogout cannot unset environment authentication or revoke a token in Harvest.", result.AccountID, result.Authenticated, result.Verified, result.LoggedOut, source, result.EnvironmentOverride, result.EnvironmentTokenPresent, authEffects(result.Effects))
}

func authEffects(effects auth.Effects) string {
	credential, config := "unknown", "unknown"
	switch effects.Credential {
	case "unchanged", "applied", "unknown":
		credential = effects.Credential
	}
	switch effects.Config {
	case "unchanged", "saved", "cleared", "restored", "unknown":
		config = effects.Config
	}
	return "Credential effect " + credential + "\nConfig effect " + config
}

func authUnavailable(ctx context.Context, p *promptBridge) error {
	return p.View(ctx, "Accounts and auth", "Requested authentication action unavailable.")
}

func authFailure(ctx context.Context, p *promptBridge, err error) error {
	var ended *terminal.ExitError
	if errors.As(err, &ended) {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	code := "operation_failed"
	var credential *auth.Error
	var remote *harvest.Error
	if errors.As(err, &credential) {
		code = credential.Code
	} else if errors.As(err, &remote) {
		code = remote.Code
	}
	switch code {
	case "auth", "keychain", "validation", "confirmation_required", "config", "forbidden", "input_required", "state_busy", "response", "network", "api", "rate_limit":
	default:
		code = "operation_failed"
	}
	return p.View(ctx, "Auth failed", code+"\nInspect credential status and account config. No automatic retry or mutation follows this failure.")
}
