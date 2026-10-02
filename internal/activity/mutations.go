package activity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

// A finite typed receipt is committed with its effect. Future operations add
// their actual result type here rather than persisting opaque provider payloads.
type mutationRequest struct {
	SyncConfigurationResult *SyncConfigurationResult `json:"sync_configuration_result,omitempty"`
	SyncRun                 *SyncRun                 `json:"sync_run,omitempty"`
	PendingSync             *syncReservation         `json:"pending_sync,omitempty"`
	Error                   *Error                   `json:"error,omitempty"`
	Operation               string                   `json:"operation"`
	Fingerprint             string                   `json:"fingerprint"`
	BindingResult           *BindingResult           `json:"binding_result,omitempty"`
	MutationResult          *MutationResult          `json:"mutation_result,omitempty"`
}

func mutationFingerprint(operation string, input any) string {
	b, _ := json.Marshal(struct {
		Operation string
		Input     any
	}{operation, input})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func mutationLookup(st *state, id, operation, fingerprint string) (mutationRequest, bool, error) {
	old, ok := st.Requests[id]
	if ok && (old.Operation != operation || old.Fingerprint != fingerprint) {
		return mutationRequest{}, false, failure("request_conflict")
	}
	return old, ok, nil
}
func requestError(err error, id string) error {
	var ae *Error
	if errors.As(err, &ae) && ae.Code == "local_write_unknown" {
		copy := *ae
		copy.Details = map[string]any{"request_id": id}
		return &copy
	}
	return err
}
func (s *Service) replayMutation(ctx context.Context, id, operation, fingerprint string) (mutationRequest, bool, error) {
	st, _, err := s.store.read(ctx)
	if err != nil {
		return mutationRequest{}, false, err
	}
	old, ok, err := mutationLookup(st, id, operation, fingerprint)
	if err != nil || !ok {
		return old, ok, err
	}
	// Visible data after an uncertain directory fsync is not a durability ACK.
	err = s.store.update(ctx, func(st *state) (bool, error) {
		var e error
		old, ok, e = mutationLookup(st, id, operation, fingerprint)
		return false, e
	})
	return old, ok, requestError(err, id)
}
func saveMutation(st *state, id string, r mutationRequest) {
	if st.Requests == nil {
		st.Requests = map[string]mutationRequest{}
	}
	st.Requests[id] = r
}
