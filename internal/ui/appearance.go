package ui

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

// appearanceFlow belongs to one runner. Only the runner's single flow worker
// accesses it, retaining exact recovery identity between invocations.
type appearanceFlow struct {
	service *themes.Service
	caps    themes.Capabilities
	pending *themes.SetInput
	unknown *themes.Error
}

func newAppearanceFlow(service *themes.Service, caps themes.Capabilities) *appearanceFlow {
	return &appearanceFlow{service: service, caps: caps}
}

func (f *appearanceFlow) run(ctx context.Context, bridge *promptBridge) (terminal.Styler, error) {
	if f.pending != nil {
		style, err := themes.NewStyler(f.pending.Theme, f.caps)
		if err != nil {
			return nil, f.unknown
		}
		if !f.retry(ctx, bridge, style) {
			return nil, f.unknown
		}
		style, err, _ = f.apply(ctx, bridge, *f.pending, style)
		return style, err
	}
	return f.draft(ctx, bridge)
}

// showAppearance owns one draft and one immutable admitted intent at a time.
// The bridge carries requests to the runner; this helper never reads input or
// renders. Candidate styling belongs only to the modal until a durable result.
func showAppearance(ctx context.Context, bridge *promptBridge, service *themes.Service, caps themes.Capabilities) (terminal.Styler, error) {
	return newAppearanceFlow(service, caps).run(ctx, bridge)
}

func (f *appearanceFlow) draft(ctx context.Context, bridge *promptBridge) (terminal.Styler, error) {
	service, caps := f.service, f.caps
	for {
		list, err := service.List(ctx)
		if err != nil {
			return nil, err
		}
		styles := make(map[string]terminal.Styler, len(list.Themes))
		choices := make([]terminal.Choice, 0, len(list.Themes))
		// Start at the saved choice without changing the generic picker's API.
		for _, selected := range []bool{true, false} {
			for _, theme := range list.Themes {
				if theme.Selected != selected {
					continue
				}
				style, err := themes.NewStyler(theme.ID, caps)
				if err != nil {
					return nil, err
				}
				styles[theme.ID] = style
				label := theme.Name
				if selected {
					label += " [saved]"
				}
				choices = append(choices, terminal.Choice{ID: theme.ID, Label: label})
			}
		}
		candidate, err := bridge.chooseStyled(ctx, "Appearance · Preview a theme", choices, styles)
		if err != nil {
			if !appearanceCancelled(err) {
				return nil, err
			}
			return currentAppearance(ctx, service, caps)
		}
		info, err := themes.Lookup(candidate)
		if err != nil {
			return nil, &themes.Error{Code: "validation", Message: "appearance choice is unavailable"}
		}
		style := styles[candidate]
		confirmed, err := bridge.confirmStyled(ctx, "Apply "+info.Name+"? This changes appearance only. Escape or No keeps the current saved choice.", style)
		if err != nil || !confirmed {
			if err != nil && !appearanceCancelled(err) {
				return nil, err
			}
			return currentAppearance(ctx, service, caps)
		}
		input := themes.SetInput{Theme: candidate, IfRevision: list.PreferenceRevision, RequestID: appearanceRequestID()}
		result, err, refresh := f.apply(ctx, bridge, input, style)
		if !refresh {
			return result, err
		}
	}
}

// apply reports whether a fresh, deliberately confirmed draft is required.
func (f *appearanceFlow) apply(ctx context.Context, bridge *promptBridge, input themes.SetInput, style terminal.Styler) (terminal.Styler, error, bool) {
	for {
		_, err := f.service.Set(ctx, input)
		if err == nil {
			f.pending, f.unknown = nil, nil
			// Replays are historical. A later writer's selection is current.
			current, err := currentAppearance(ctx, f.service, f.caps)
			return current, err, false
		}
		var safe *themes.Error
		if !errors.As(err, &safe) {
			if f.unknown != nil {
				return nil, f.unknown, false
			}
			return nil, err, false
		}
		if safe.Code == "local_write_unknown" {
			copy := *safe
			copy.Details = map[string]any{"theme": input.Theme, "if_revision": input.IfRevision, "request_id": input.RequestID}
			f.pending, f.unknown = &input, &copy
			if !f.retry(ctx, bridge, style) {
				return nil, f.unknown, false
			}
			continue
		}
		// A cancelled/failed retry is no durability acknowledgement of the
		// original uncertain transaction. Preserve its exact replay inputs.
		if f.unknown != nil {
			return nil, f.unknown, false
		}
		if safe.Code != "revision_conflict" {
			return nil, err, false
		}
		if err := bridge.viewStyled(ctx, "Appearance changed", "revision_conflict: another writer changed the saved appearance. Review a fresh choice and confirm again before applying.", style); err != nil {
			if !appearanceCancelled(err) {
				return nil, err, false
			}
			current, err := currentAppearance(ctx, f.service, f.caps)
			return current, err, false
		}
		return nil, nil, true // Fresh read, preview, confirmation and identity.
	}
}

func (f *appearanceFlow) retry(ctx context.Context, bridge *promptBridge, style terminal.Styler) bool {
	input := *f.pending
	body := fmt.Sprintf("local_write_unknown: appearance durability is unknown.\nTheme: %s\nObserved preference revision: %s\nRequest ID: %s\nKeep this exact theme, revision and request ID for retry. Retry checks durability without replacing a later writer's choice.", input.Theme, input.IfRevision, input.RequestID)
	if bridge.viewStyled(ctx, "Appearance outcome unknown", body, style) != nil {
		return false
	}
	confirmed, err := bridge.confirmStyled(ctx, "Retry the exact appearance request "+input.RequestID+"? Keep theme "+input.Theme+" and observed revision "+input.IfRevision+" unchanged.", style)
	return err == nil && confirmed
}

func appearanceCancelled(err error) bool {
	var exit *terminal.ExitError
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.As(err, &exit) && (exit.Code == 0 || exit.Code == 130 || exit.Code == 143)
}

func currentAppearance(ctx context.Context, service *themes.Service, caps themes.Capabilities) (terminal.Styler, error) {
	// Escape cancels the draft context. A bounded pure reread still reflects an
	// external writer; it does not resurrect that draft or attempt another write.
	readCtx, end := context.WithTimeout(context.WithoutCancel(ctx), 250*time.Millisecond)
	defer end()
	current, err := service.Show(readCtx, "")
	if err != nil {
		return nil, err
	}
	return themes.NewStyler(current.Theme.ID, caps)
}

func appearanceRequestID() string {
	var bytes [16]byte
	_, _ = rand.Read(bytes[:]) // Go's default cryptographic reader fails closed.
	bytes[6], bytes[8] = bytes[6]&15|64, bytes[8]&63|128
	h := hex.EncodeToString(bytes[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
