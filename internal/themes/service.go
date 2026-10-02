package themes

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/rbeene/tempo/internal/activity"
)

type ThemeList struct {
	ContractVersion    int         `json:"contract_version"`
	PreferenceRevision string      `json:"preference_revision"`
	Themes             []ThemeInfo `json:"themes"`
}

type ThemeResult struct {
	ContractVersion    int       `json:"contract_version"`
	PreferenceRevision string    `json:"preference_revision"`
	Theme              ThemeInfo `json:"theme"`
}

type SetInput struct{ Theme, IfRevision, RequestID string }
type ResetInput struct{ IfRevision, RequestID string }

type Options struct {
	Path        string
	Getenv      func(string) string
	LockTimeout time.Duration
	Fault       func(stage string) error
}

type Error struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Uncertain bool           `json:"uncertain"`
	Details   map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return e.Message }

type Service struct{ options Options }

// New is lazy: construction never opens files or reads environment variables.
func New(options Options) *Service { return &Service{options: options} }

func (s *Service) store() (*fileStore, error) {
	if s.options.LockTimeout < 0 || s.options.LockTimeout > time.Second {
		return nil, failure("validation")
	}
	path := s.options.Path
	if path == "" {
		getenv := s.options.Getenv
		if getenv == nil {
			getenv = os.Getenv
		}
		path = getenv("TEMPO_PREFERENCES")
	}
	return &fileStore{path: path, timeout: s.options.LockTimeout, fail: s.options.Fault}, nil
}

func (s *Service) List(ctx context.Context) (ThemeList, error) {
	store, err := s.store()
	if err != nil {
		return ThemeList{}, err
	}
	st, _, err := store.read(ctx)
	if err != nil {
		return ThemeList{}, err
	}
	themes := Catalog()
	for i := range themes {
		themes[i].Selected = themes[i].ID == st.Theme
	}
	return ThemeList{ContractVersion: 1, PreferenceRevision: st.PreferenceRevision, Themes: themes}, nil
}

func (s *Service) Show(ctx context.Context, id string) (ThemeResult, error) {
	if id != "" {
		if _, err := Lookup(id); err != nil {
			return ThemeResult{}, failure("validation")
		}
	}
	list, err := s.List(ctx)
	if err != nil {
		return ThemeResult{}, err
	}
	for _, theme := range list.Themes {
		if theme.ID == id || (id == "" && theme.Selected) {
			return ThemeResult{ContractVersion: 1, PreferenceRevision: list.PreferenceRevision, Theme: theme}, nil
		}
	}
	return ThemeResult{}, failure("state_corrupt")
}

func (s *Service) Set(ctx context.Context, input SetInput) (activity.MutationResult, error) {
	if _, err := Lookup(input.Theme); err != nil {
		return activity.MutationResult{}, failure("validation")
	}
	if _, valid := counter(input.IfRevision); input.IfRevision != "" && !valid {
		return activity.MutationResult{}, failure("validation")
	}
	if input.RequestID != "" && !validUUID(input.RequestID) {
		return activity.MutationResult{}, failure("validation")
	}
	store, err := s.store()
	if err != nil {
		return activity.MutationResult{}, err
	}
	if input.RequestID == "" {
		input.RequestID = newID()
	}
	intent := intent{Theme: input.Theme, IfRevision: input.IfRevision}
	fingerprint := fingerprint(intent)
	var result activity.MutationResult
	err = store.update(ctx, func(st *state) (bool, error) {
		if old, found := st.Requests[input.RequestID]; found {
			if old.Fingerprint != fingerprint {
				return false, failure("request_conflict")
			}
			result = old.Result
			return false, nil // update re-synchronizes visible durable evidence.
		}
		if input.IfRevision != "" && input.IfRevision != st.PreferenceRevision {
			err := failure("revision_conflict")
			err.Details = map[string]any{"current_revision": st.PreferenceRevision}
			return false, err
		}
		changed := st.Theme != input.Theme
		if changed {
			st.Theme, st.PreferenceRevision = input.Theme, bump(st.PreferenceRevision)
		}
		revision := st.PreferenceRevision
		result = activity.MutationResult{ContractVersion: 1, SnapshotRevision: bump(st.Revision),
			RequestID: input.RequestID, Changed: changed, AffectedIDs: []string{}, EntityRevision: &revision}
		st.Requests[input.RequestID] = receipt{Intent: intent, Fingerprint: fingerprint, Result: result}
		return true, nil
	})
	if err != nil {
		var safe *Error
		if errors.As(err, &safe) && safe.Uncertain {
			safe.Details = map[string]any{"request_id": input.RequestID}
		}
		return activity.MutationResult{}, err
	}
	return result, nil
}

func (s *Service) Reset(ctx context.Context, input ResetInput) (activity.MutationResult, error) {
	return s.Set(ctx, SetInput{Theme: "terminal-default", IfRevision: input.IfRevision, RequestID: input.RequestID})
}
