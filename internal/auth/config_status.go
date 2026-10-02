package auth

import "context"

// ConfigStatus is the existing nonsecret config-show result shared by CLI/UI.
type ConfigStatus struct {
	Path                string `json:"path"`
	SavedAccountID      string `json:"saved_account_id"`
	AccountID           string `json:"account_id"`
	TokenStoredInConfig bool   `json:"token_stored_in_config"`
}

func (s *Service) ConfigShow(ctx context.Context, explicit string) (ConfigStatus, error) {
	return ConfigStatus{}, nil
}
