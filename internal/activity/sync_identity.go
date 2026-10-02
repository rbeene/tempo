package activity

import (
	"context"
	"github.com/rbeene/tempo/internal/identity"
	"time"
)

// SyncIdentity exposes the same verified account/current-user scope used by
// configure so a presentation can retain that identity before confirmation.
func (s *Service) SyncIdentity(ctx context.Context, accountID string, dependencies SyncDependencies) (SyncAccountIdentity, error) {
	if ctx.Err() != nil {
		return SyncAccountIdentity{}, context.Cause(ctx)
	}
	if accountID == "" {
		return SyncAccountIdentity{}, syncRequired("account_id")
	}
	if !identity.Valid(accountID) {
		return SyncAccountIdentity{}, failure("validation")
	}
	ctx, end := context.WithTimeout(ctx, 2*time.Minute)
	defer end()
	provider, err := syncProvider(ctx, dependencies, accountID)
	if ctx.Err() != nil {
		return SyncAccountIdentity{}, context.Cause(ctx)
	}
	if err != nil {
		return SyncAccountIdentity{}, err
	}
	user, err := syncIdentity(ctx, provider, accountID)
	if ctx.Err() != nil {
		return SyncAccountIdentity{}, context.Cause(ctx)
	}
	if err != nil {
		return SyncAccountIdentity{}, err
	}
	return SyncAccountIdentity{AccountID: accountID, UserID: bindingID(user)}, nil
}
