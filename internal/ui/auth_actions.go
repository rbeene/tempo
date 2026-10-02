package ui

import (
	"context"

	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
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

type authController struct{ pending error }

func (c *authController) run(ctx context.Context, p *promptBridge, actions *AuthActions) error {
	return nil
}
