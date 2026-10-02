package ui

import (
	"errors"
	"sort"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/harvest"
	"github.com/rbeene/tempo/internal/hookstate"
	"github.com/rbeene/tempo/internal/themes"
	"github.com/rbeene/tempo/internal/worker"
)

func unknownOutcome(err error) bool {
	var a *activity.Error
	var credential *auth.Error
	var remote *harvest.Error
	var hooks *hookstate.Error
	var service *worker.Error
	var appearance *themes.Error
	return errors.As(err, &appearance) && appearance.Uncertain || errors.As(err, &a) && a.Uncertain || errors.As(err, &credential) && credential.Uncertain || errors.As(err, &remote) && remote.Uncertain || errors.As(err, &hooks) && hooks.Uncertain || errors.As(err, &service) && service.Uncertain
}

func primaryOutcome(retained map[string]error) error {
	for _, name := range retainedNames(retained) {
		if retained[name] != nil {
			return retained[name]
		}
	}
	return nil
}

func retainedNames(retained map[string]error) []string {
	credential, other := []string{}, []string{}
	for name, err := range retained {
		var outcome *auth.Error
		if errors.As(err, &outcome) && outcome.Uncertain {
			credential = append(credential, name)
		} else {
			other = append(other, name)
		}
	}
	sort.Strings(credential)
	sort.Strings(other)
	return append(credential, other...)
}
