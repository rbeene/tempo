package ui

import (
	"errors"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/worker"
)

func unknownOutcome(err error) bool {
	var a *activity.Error
	var credential *auth.Error
	var remote *harvest.Error
	var hooks *hookstate.Error
	var service *worker.Error
	return errors.As(err, &a) && a.Uncertain || errors.As(err, &credential) && credential.Uncertain || errors.As(err, &remote) && remote.Uncertain || errors.As(err, &hooks) && hooks.Uncertain || errors.As(err, &service) && service.Uncertain
}
