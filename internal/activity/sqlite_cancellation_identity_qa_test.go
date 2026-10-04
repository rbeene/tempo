//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

type cancellationQASecret struct{}

func (*cancellationQASecret) Error() string { return "private-cancellation-evidence-marker" }

const cancellationQABusyJSON = `{"code":"state_busy","message":"local activity state is busy","retryable":true,"uncertain":false}`
const cancellationQARequestID = "ad000000-0000-4000-8000-000000000001"
const cancellationQAUnknownJSON = `{"code":"local_write_unknown","message":"local write durability is unknown; retry with the same identity","retryable":false,"uncertain":true,"details":{"request_id":"ad000000-0000-4000-8000-000000000001"}}`

func cancellationQASafe(t *testing.T, err error, wantJSON string, secret *cancellationQASecret) {
	t.Helper()
	var public *Error
	if !errors.As(err, &public) || public == nil {
		t.Fatal("safe activity classification missing")
	}
	encoded, encodeErr := json.Marshal(public)
	if encodeErr != nil || string(encoded) != wantJSON {
		t.Fatal("public JSON classification changed", string(encoded), encodeErr)
	}
	if errors.Unwrap(err) != public || err.Error() != public.Message {
		t.Fatal("public-only unwrap or safe message changed")
	}
	var native *sqliteio.Error
	var private *cancellationQASecret
	if errors.As(err, &native) || errors.As(err, &private) || errors.Is(err, secret) {
		t.Fatal("private cause escaped through public error inspection")
	}
	outerJSON, encodeErr := json.Marshal(err)
	if encodeErr != nil || strings.Contains(string(outerJSON), secret.Error()) || strings.Contains(fmt.Sprintf("%v %+v", err, err), secret.Error()) {
		t.Fatal("private evidence escaped public rendering")
	}
}

func TestSQLiteCancellationIdentityCleanBusyPreservesSafePublicResult(t *testing.T) {
	for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, native := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/native=%t", target, native), func(t *testing.T) {
				secret := &cancellationQASecret{}
				cause := errors.Join(secret, target)
				if native {
					cause = errors.Join(secret, &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Code: 9, Cause: fmt.Errorf("private nested cause: %w", target)})
				}
				err := sqliteLinkFailure(cause, nil, nil, false, false, cancellationQARequestID, nil)
				cancellationQASafe(t, err, cancellationQABusyJSON, secret)
				if !errors.Is(err, target) || !errors.Is(fmt.Errorf("safe outer wrapper: %w", err), target) {
					t.Fatal("clean cancellation identity was hidden by the safe wrapper")
				}
				other := context.Canceled
				if target == context.Canceled {
					other = context.DeadlineExceeded
				}
				if errors.Is(err, other) || errors.Is(err, errors.New(target.Error())) || errors.Is(err, sqliteio.ErrBusy) {
					t.Fatal("nonmatching sentinel was inferred or private evidence exposed")
				}
				cancellationQASafe(t, err, cancellationQABusyJSON, secret)
			})
		}
	}
}

func TestSQLiteCancellationIdentityNeverOverridesUnknownOrRetainedOwnership(t *testing.T) {
	for _, reason := range []string{"known", "uncertain", "cleanup", "owner", "native-cleanup", "already-unknown", "nested-known"} {
		t.Run(reason, func(t *testing.T) {
			secret := &cancellationQASecret{}
			cause := errors.Join(secret, context.Canceled, context.DeadlineExceeded)
			var cleanup error
			var owner *sqliteio.Conn
			known, uncertain := false, false
			switch reason {
			case "known":
				known = true
			case "uncertain":
				uncertain = true
			case "cleanup":
				cleanup = secret
			case "owner":
				owner = &sqliteio.Conn{} // Identity-only token; no native handle is opened or closed.
			case "native-cleanup":
				cause = &sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Cause: cause, Cleanup: secret}
			case "already-unknown":
				cause = errors.Join(cause, failure("local_write_unknown"))
			case "nested-known":
				cause = sqliteLinkFailure(cause, nil, nil, true, false, cancellationQARequestID, nil)
			}
			err := sqliteLinkFailure(cause, cleanup, owner, known, uncertain, cancellationQARequestID, nil)
			cancellationQASafe(t, err, cancellationQAUnknownJSON, secret)
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("uncertain outcome was reclassified as cancellation")
			}
			if errors.Is(fmt.Errorf("safe outer wrapper: %w", err), context.Canceled) {
				t.Fatal("outer wrapper bypassed uncertainty priority")
			}
		})
	}
}

func TestSQLiteCancellationIdentityRejectsFalseEvidenceAndKeepsPublicPreparation(t *testing.T) {
	secret := &cancellationQASecret{}
	for _, cause := range []error{
		&sqliteio.Error{Phase: sqliteio.Admission, Category: sqliteio.Busy, Cause: sqliteio.ErrBusy},
		&sqliteio.Error{Phase: sqliteio.OpenPhase, Category: sqliteio.Canceled, Code: 9},
	} {
		err := sqliteLinkFailure(errors.Join(secret, cause), nil, nil, false, false, "", nil)
		cancellationQASafe(t, err, cancellationQABusyJSON, secret)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("busy/category alone fabricated caller context cancellation")
		}
	}
	for _, public := range []*Error{
		{Code: "state_busy", Message: "safe but uncertain", Uncertain: true},
		{Code: "state_corrupt", Message: "safe nonbusy outcome"},
	} {
		err := &sqliteLinkFailureError{public: public, evidence: errors.Join(secret, context.Canceled, context.DeadlineExceeded)}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Unwrap(err) != public {
			t.Fatal("ineligible public outcome revealed private context identity")
		}
	}
	// Exercise the owner gate independently from the factory's unknown mapping.
	owned := &sqliteLinkFailureError{public: failure("state_busy"), evidence: context.Canceled, owner: &sqliteio.Conn{}}
	if errors.Is(owned, context.Canceled) {
		t.Fatal("retained owner was treated as a clean cancellation")
	}
	lookalike := sqliteLinkFailure(errors.New(context.Canceled.Error()), nil, nil, false, false, "", nil)
	if errors.Is(lookalike, context.Canceled) {
		t.Fatal("matching error text fabricated sentinel identity")
	}
	preparation := errors.New("safe selected preparation outcome")
	prepared := sqliteLinkFailure(errors.Join(secret, preparation, context.Canceled), nil, nil, false, false, "", preparation)
	if errors.Unwrap(prepared) != preparation || !errors.Is(prepared, preparation) || errors.Is(prepared, context.Canceled) || errors.Is(prepared, secret) {
		t.Fatal("public preparation identity or precedence changed")
	}
}
