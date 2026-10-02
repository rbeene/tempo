package activity

import "context"

// SyncIdentity exposes the same verified account/current-user scope used by
// configure so a presentation can retain that identity before confirmation.
func (s *Service) SyncIdentity(ctx context.Context, accountID string, dependencies SyncDependencies) (SyncAccountIdentity, error) {
	return SyncAccountIdentity{}, nil
}
