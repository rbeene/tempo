package cli

import (
	"context"
	"regexp"
	"time"

	"github.com/rbeene/tempo/internal/activity"
)

var recoveryUTCPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?Z$`)

func recoveryCommand(name string) bool {
	return name == "activity review" || name == "activity preview" || name == "activity resolve" || name == "activity interrupt"
}
func recoveryUTC(value string) (*time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || !recoveryUTCPattern.MatchString(value) || t.IsZero() {
		return nil, problem("validation", "end must be a UTC RFC3339 timestamp ending in Z")
	}
	return &t, nil
}
func validateRecoveryCLI(p *parsed) error {
	f := p.flags
	if p.command.Name != "activity review" && f["account"] != "" {
		return problem("usage", "--account is not valid for this recovery action")
	}
	if value, ok := f["end"]; ok {
		if _, err := recoveryUTC(value); err != nil {
			return err
		}
	}
	return nil
}
func executeRecovery(ctx context.Context, p parsed, service *activity.Service) (any, error) {
	f := p.flags
	if p.command.Name == "activity review" {
		return service.Review(ctx, activity.ReviewInput{AccountID: f["account"], ProjectID: f["project"]})
	}
	var end *time.Time
	if v, ok := f["end"]; ok {
		var err error
		end, err = recoveryUTC(v)
		if err != nil {
			return nil, err
		}
	}
	id := p.args[0]
	if p.command.Name == "activity preview" {
		return service.Preview(ctx, activity.RecoveryInput{UncertaintyID: id, End: end, DiscardTail: f["discard-tail"] == "true"})
	}
	request := f["request-id"]
	if request == "" {
		request = linkRequestID()
	}
	if p.command.Name == "activity interrupt" {
		return service.Interrupt(ctx, activity.InterruptInput{ActorID: id, Generation: f["generation"], IfRevision: f["if-revision"], RequestID: request, Confirmed: f["yes"] == "true"})
	}
	return service.Resolve(ctx, activity.ResolveInput{UncertaintyID: id, End: end, DiscardTail: f["discard-tail"] == "true", IfRevision: f["if-revision"], Reason: f["reason"], RequestID: request, Confirmed: f["yes"] == "true"})
}
