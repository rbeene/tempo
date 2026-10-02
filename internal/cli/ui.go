package cli

import (
	"errors"
	"regexp"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/worker"
)

var uiRequestIdentity = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// uiResultError retains shared domain outcomes after terminal restoration and
// bounds unexpected presentation failures to the terminal I/O error contract.
func uiResultError(err error) error {
	if err == nil {
		return nil
	}
	var local *activity.Error
	if errors.As(err, &local) {
		if local.Uncertain {
			copy := *local
			copy.Message = "local write outcome or durability is unknown; preserve the exact intent and inspect local state"
			if id, ok := copy.Details["request_id"].(string); ok && uiRequestIdentity.MatchString(id) {
				copy.Message += "; request ID: " + id
			}
			return &copy
		}
		return err
	}
	var credential *auth.Error
	var remote *harvest.Error
	var hooks *hookstate.Error
	var service *worker.Error
	if errors.As(err, &credential) || errors.As(err, &remote) || errors.As(err, &hooks) || errors.As(err, &service) || errors.Is(err, auth.ErrNotFound) || errors.Is(err, auth.ErrPersistenceUnavailable) {
		return err
	}
	var ended *terminal.ExitError
	if errors.As(err, &ended) {
		return err
	}
	return &terminal.ExitError{Code: 1}
}
