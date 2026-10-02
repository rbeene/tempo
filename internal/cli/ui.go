package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/worker"
)

var uiRequestIdentity = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// The report budget begins after Close, at the first plain diagnostic. Every
// retained outcome and the final human error share that one bounded budget.
type uiDiagnosticWriter struct {
	writer   io.Writer
	deadline time.Time
}

func (w *uiDiagnosticWriter) Write(data []byte) (int, error) {
	if w.deadline.IsZero() {
		w.deadline = time.Now().Add(250 * time.Millisecond)
	}
	ctx, end := context.WithDeadline(context.Background(), w.deadline)
	defer end()
	return terminal.WriteDiagnostic(ctx, w.writer, data)
}

func uiAuthReport(w io.Writer, operation string, result auth.Result, err error) {
	switch operation {
	case "login", "logout", "account selection":
	default:
		operation = "credential operation"
	}
	state, effects := "complete", result.Effects
	if err != nil {
		state = "operation_failed"
		var domain *auth.Error
		if errors.As(err, &domain) {
			state, effects = uiAuthCode(domain.Code), domain.Effects
		}
	}
	account := "unavailable"
	if validID(result.AccountID) {
		account = result.AccountID
	}
	fmt.Fprintf(w, "tempo: auth: %s %s; account %s; %s\n", operation, state, account, uiAuthEffects(effects))
}

func uiRetainedReport(w io.Writer, family, requestID string, err error) {
	switch family {
	case "auth", "links", "activity", "sync", "hooks", "worker", "appearance":
	default:
		family = "operation"
	}
	code := "local_write_unknown"
	var credential *auth.Error
	var remote *harvest.Error
	if errors.As(err, &credential) {
		fmt.Fprintf(w, "tempo: %s: credential_write_unknown; %s; inspect auth status and config show\n", family, uiAuthEffects(credential.Effects))
		return
	}
	if errors.As(err, &remote) {
		code = "uncertain_write"
	}
	identity := ""
	if uiRequestIdentity.MatchString(requestID) {
		identity = "; request ID: " + requestID
	}
	fmt.Fprintf(w, "tempo: %s: %s%s; preserve the exact submitted intent and inspect shared status\n", family, code, identity)
}

func uiRestorationReport(w io.Writer) {
	fmt.Fprintln(w, "tempo: terminal: terminal restoration failed")
}

func uiAuthCode(code string) string {
	switch code {
	case "auth", "keychain", "validation", "confirmation_required", "config", "forbidden", "input_required", "credential_write_unknown", "state_busy", "response":
		return code
	}
	return "operation_failed"
}

func uiAuthEffects(effects auth.Effects) string {
	credential, config := "unknown", "unknown"
	switch effects.Credential {
	case "unchanged", "applied", "unknown":
		credential = effects.Credential
	}
	switch effects.Config {
	case "unchanged", "saved", "cleared", "restored", "unknown":
		config = effects.Config
	}
	return "credential " + credential + "; config " + config
}

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
	if errors.As(err, &credential) {
		copy := *credential
		copy.Message = "credential operation returned " + uiAuthCode(credential.Code) + "; " + uiAuthEffects(credential.Effects)
		if credential.Uncertain {
			copy.Message += "; inspect auth status and config show before an explicitly reviewed replacement"
		}
		return &copy
	}
	var remote *harvest.Error
	var hooks *hookstate.Error
	var service *worker.Error
	if errors.As(err, &service) {
		if service.Uncertain {
			copy := *service
			copy.Message = "local worker control outcome or durability is unknown; preserve the exact intent and inspect worker status"
			return &copy
		}
		return err
	}
	if errors.As(err, &remote) || errors.As(err, &hooks) || errors.Is(err, auth.ErrNotFound) || errors.Is(err, auth.ErrPersistenceUnavailable) {
		return err
	}
	var ended *terminal.ExitError
	if errors.As(err, &ended) {
		return err
	}
	return &terminal.ExitError{Code: 1}
}
