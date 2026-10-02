package auth

import (
	"context"

	"github.com/rbeene/tempo/internal/identity"
)

// ConfigStatus is the existing nonsecret config-show result shared by CLI/UI.
type ConfigStatus struct {
	Path                string `json:"path"`
	SavedAccountID      string `json:"saved_account_id"`
	AccountID           string `json:"account_id"`
	TokenStoredInConfig bool   `json:"token_stored_in_config"`
}

func (s *Service) ConfigShow(ctx context.Context, explicit string) (ConfigStatus, error) {
	if err := ctx.Err(); err != nil {
		return ConfigStatus{}, context.Cause(ctx)
	}
	path, err := s.configPath()
	if err != nil {
		return ConfigStatus{}, issue("config", unchanged())
	}
	cfg, err := Load(path)
	if err != nil {
		return ConfigStatus{}, issue("config", unchanged())
	}
	account := explicit
	if account == "" {
		account = s.options.Getenv("HARVEST_ACCOUNT_ID")
	}
	if account == "" {
		account = cfg.Account
	}
	if account != "" && !identity.Valid(account) {
		return ConfigStatus{}, issue("validation", unchanged())
	}
	if err := ctx.Err(); err != nil {
		return ConfigStatus{}, context.Cause(ctx)
	}
	return ConfigStatus{Path: path, SavedAccountID: cfg.Account, AccountID: account}, nil
}
