package ui

import (
	"bytes"
	"errors"
	"testing"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

func TestQAAppearanceUnknownLeaveRereadsPaletteWithoutClearingUncertainty(t *testing.T) {
	path := qaAppearancePath(t)
	qaAppearanceSave(t, path, "tokyo-night", "0", 1)
	x := qaAppearanceStart(t, qaAppearanceService(path, func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("PRIVATE synthetic durability")
		}
		return nil
	}))
	qaAppearanceChoose(t, x.request(t, "choose"), "gruvbox")
	x.request(t, "confirm").reply <- promptReply{confirmed: true}
	r := x.request(t, "view")
	qaAppearanceSave(t, path, "catppuccin", "2", 2)
	external := qaAppearanceBytes(t, path)
	r.reply <- promptReply{err: &terminal.ExitError{Code: 0}}
	result := x.finish(t)
	var unknown *themes.Error
	if !errors.As(result.err, &unknown) || unknown.Code != "local_write_unknown" || !unknown.Uncertain || unknown.Details["theme"] != "gruvbox" || unknown.Details["if_revision"] != "1" {
		t.Fatalf("presentation reread erased original uncertain intent: %v", result.err)
	}
	qaAppearanceCurrentStyle(t, result.style, "catppuccin")
	if !bytes.Equal(external, qaAppearanceBytes(t, path)) {
		t.Fatal("uncertain leave rolled back or wrote external preference")
	}
}
