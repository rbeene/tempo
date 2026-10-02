package auth

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/identity"
	"os"
)

func unchanged() Effects { return Effects{"unchanged", "unchanged"} }

// ErrPersistenceUnavailable identifies absent native storage after environment lookup.
// Its ordinary CLI classification remains keychain; unrelated provider errors differ.
var ErrPersistenceUnavailable = issue("keychain", unchanged())

func issue(code string, effects Effects) *Error {
	messages := map[string]string{"auth": "no valid token configured; connect securely or use HARVEST_TOKEN", "keychain": "secure credential storage is unavailable; unlock it locally or use HARVEST_TOKEN", "validation": "check the supplied token and account selection", "confirmation_required": "this action requires explicit confirmation", "config": "could not save account configuration; inspect config show", "forbidden": "selected Harvest account is not accessible", "input_required": "an explicit account selection is required", "credential_write_unknown": "credential operation may have applied; inspect auth status and config show before an explicit replacement; do not retry automatically", "state_busy": "another authentication operation is active; try again after it completes", "response": "credential helper returned an invalid response"}
	msg := messages[code]
	if msg == "" {
		code = "keychain"
		msg = messages[code]
	}
	return &Error{Code: code, Message: msg, Uncertain: code == "credential_write_unknown", Retryable: code == "state_busy", Effects: effects}
}
func accountID(o harvest.Object) string {
	switch x := o["id"].(type) {
	case string:
		return x
	case json.Number:
		return string(x)
	}
	return ""
}
func containsAccount(as []harvest.Object, id string) bool {
	for _, a := range as {
		if a["product"] == "harvest" && accountID(a) == id {
			return true
		}
	}
	return false
}
func NewService(o Options) *Service {
	if o.Getenv == nil {
		o.Getenv = os.Getenv
	}
	if o.Runner == nil {
		o.Runner = ProcessRunner{}
	}
	if o.NewProvider == nil {
		o.NewProvider = func(t, a string) harvest.Provider { return harvest.New(t, a) }
	}
	if o.PersistentAvailable == nil {
		o.PersistentAvailable = NativeSupported
	}
	return &Service{options: o}
}
func (s *Service) CanPersist() bool { return s.options.PersistentAvailable() }
func (s *Service) configPath() (string, error) {
	if s.options.ConfigPath != "" {
		return s.options.ConfigPath, nil
	}
	if p := s.options.Getenv("TEMPO_CONFIG"); p != "" {
		return p, nil
	}
	return ConfigPath()
}
func (s *Service) EffectiveAccount(explicit string) (string, error) {
	a := explicit
	if a == "" {
		a = s.options.Getenv("HARVEST_ACCOUNT_ID")
	}
	if a == "" {
		p, e := s.configPath()
		if e != nil {
			return "", issue("config", unchanged())
		}
		c, e := Load(p)
		if e != nil {
			return "", issue("config", unchanged())
		}
		a = c.Account
	}
	if a != "" && !identity.Valid(a) {
		return "", issue("validation", unchanged())
	}
	return a, nil
}
func (s *Service) resolve(ctx context.Context, lock *os.File) (string, string, error) {
	if e := ctx.Err(); e != nil {
		return "", "", e
	}
	if t := s.options.Getenv("HARVEST_TOKEN"); t != "" {
		v, e := validateToken(t)
		if e != nil {
			return "", "", issue("validation", unchanged())
		}
		return v, "environment", nil
	}
	if !s.CanPersist() {
		return "", "", ErrPersistenceUnavailable
	}
	r, e := s.options.Runner.Run(ctx, NativeRequest{Operation: "read"}, lock)
	if e != nil {
		return "", "", sanitizeRunnerError(e)
	}
	if r.Code == "not_found" {
		return "", "", ErrNotFound
	}
	if r.Code != "" {
		return "", "", issue(r.Code, r.Effects)
	}
	t, e := validateToken(string(r.Token))
	clear(r.Token)
	if e != nil {
		return "", "", issue("response", unchanged())
	}
	return t, "keychain", nil
}
func (s *Service) ResolveToken(ctx context.Context) (string, string, error) {
	return s.resolve(ctx, nil)
}
func (s *Service) Provider(ctx context.Context, account string) (harvest.Provider, error) {
	t, _, e := s.ResolveToken(ctx)
	if e != nil {
		return nil, e
	}
	return s.options.NewProvider(t, account), nil
}
func (s *Service) Accounts(ctx context.Context) ([]harvest.Object, error) {
	p, e := s.Provider(ctx, "")
	if e != nil {
		return nil, e
	}
	return p.Accounts(ctx)
}
func (a *LoginAttempt) Close() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	clear(a.token)
	a.token = nil
	a.consumed = true
}
func (s *Service) PrepareLogin(ctx context.Context, input []byte) (*LoginAttempt, error) {
	if !s.CanPersist() {
		return nil, issue("keychain", unchanged())
	}
	t, e := validateToken(string(input))
	if e != nil {
		return nil, issue("validation", unchanged())
	}
	as, e := s.options.NewProvider(t, "").Accounts(ctx)
	if e != nil {
		return nil, e
	}
	return &LoginAttempt{Accounts: as, token: []byte(t)}, nil
}
func (s *Service) CommitLogin(ctx context.Context, a *LoginAttempt, selected string) (Result, error) {
	if a == nil {
		return Result{}, issue("validation", unchanged())
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.consumed {
		return Result{}, issue("validation", unchanged())
	}
	a.consumed = true
	defer func() { clear(a.token); a.token = nil }()
	id, e := s.EffectiveAccount(selected)
	if e != nil {
		return Result{}, e
	}
	if id == "" && len(a.Accounts) == 1 {
		id = accountID(a.Accounts[0])
	}
	if id == "" {
		e := issue("input_required", unchanged())
		e.RequiredFields = []string{"account_id"}
		return Result{}, e
	}
	if !identity.Valid(id) || !containsAccount(a.Accounts, id) {
		return Result{}, issue("forbidden", unchanged())
	}
	r, e := s.mutate(ctx, NativeRequest{Operation: "login", AccountID: id, Token: a.token}, nil)
	if e != nil {
		return Result{}, e
	}
	return Result{Authenticated: true, AccountID: id, Source: "keychain", EnvironmentOverride: s.options.Getenv("HARVEST_TOKEN") != "", Effects: r.Effects}, nil
}
func sanitizeRunnerError(e error) error {
	var ae *Error
	if errors.As(e, &ae) {
		return issue(ae.Code, ae.Effects)
	}
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		return e
	}
	return issue("keychain", unchanged())
}
func (s *Service) mutate(ctx context.Context, req NativeRequest, held *os.File) (NativeReply, error) {
	if e := ctx.Err(); e != nil {
		return NativeReply{}, e
	}
	p, e := s.configPath()
	if e != nil {
		return NativeReply{}, issue("config", unchanged())
	}
	req.ConfigPath = p
	lock := held
	if lock == nil {
		lock, e = AcquireMutationLock(ctx, s.options.LockPath)
		if e != nil {
			return NativeReply{}, e
		}
		defer lock.Close()
	}
	r, e := s.options.Runner.Run(ctx, req, lock)
	if e != nil {
		return r, sanitizeRunnerError(e)
	}
	if r.Code != "" {
		return r, issue(r.Code, r.Effects)
	}
	return r, nil
}
func (s *Service) Logout(ctx context.Context, confirmed bool) (Result, error) {
	if !confirmed {
		return Result{}, issue("confirmation_required", unchanged())
	}
	if !s.CanPersist() {
		return Result{}, issue("keychain", unchanged())
	}
	r, e := s.mutate(ctx, NativeRequest{Operation: "logout"}, nil)
	if e != nil {
		return Result{}, e
	}
	return Result{LoggedOut: true, EnvironmentTokenPresent: s.options.Getenv("HARVEST_TOKEN") != "", Note: "logout removes local storage only; unset HARVEST_TOKEN separately and revoke tokens in Harvest if needed", Effects: r.Effects}, nil
}
func (s *Service) UseAccount(ctx context.Context, id string) (Result, error) {
	if !identity.Valid(id) {
		return Result{}, issue("validation", unchanged())
	}
	lock, e := AcquireMutationLock(ctx, s.options.LockPath)
	if e != nil {
		return Result{}, e
	}
	defer lock.Close()
	token, _, e := s.resolve(ctx, lock)
	if e != nil {
		return Result{}, e
	}
	as, e := s.options.NewProvider(token, "").Accounts(ctx)
	if e != nil {
		return Result{}, e
	}
	if !containsAccount(as, id) {
		return Result{}, issue("forbidden", unchanged())
	}
	r, e := s.mutate(ctx, NativeRequest{Operation: "account", AccountID: id}, lock)
	if e != nil {
		return Result{}, e
	}
	return Result{AccountID: id, Effects: r.Effects}, nil
}
func (s *Service) Status(ctx context.Context, check bool, explicit string) (Result, error) {
	id, e := s.EffectiveAccount(explicit)
	if e != nil {
		return Result{}, e
	}
	t, source, e := s.ResolveToken(ctx)
	if errors.Is(e, ErrNotFound) {
		return Result{AccountID: id, Source: "none", Effects: unchanged()}, nil
	}
	if e != nil {
		return Result{}, e
	}
	r := Result{Authenticated: true, AccountID: id, Source: source, Effects: unchanged()}
	if !check {
		return r, nil
	}
	as, e := s.options.NewProvider(t, id).Accounts(ctx)
	if e != nil {
		return Result{}, e
	}
	if id != "" && !containsAccount(as, id) {
		return Result{}, issue("forbidden", unchanged())
	}
	r.Verified = true
	r.Accounts = as
	return r, nil
}
