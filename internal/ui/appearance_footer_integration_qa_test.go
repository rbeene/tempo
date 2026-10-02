package ui_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/ui"
)

func TestQAUIAppearanceIntegratedFooterKeepsPrimaryActionsAtMediumWidths(t *testing.T) {
	for width := 60; width <= 80; width++ {
		t.Run(fmt.Sprintf("content-%d", width), func(t *testing.T) {
			for _, rows := range []int{8, 12, 24} {
				t.Run(fmt.Sprintf("rows-%d", rows), func(t *testing.T) {
					for _, state := range []string{"empty", "active"} {
						t.Run(state, func(t *testing.T) {
							model := ui.NewModel(width+1, rows)
							snapshot := qaUISnapshot("1")
							if state == "active" {
								snapshot = qaUISnapshot("1", "100")
							}
							qaUIApply(t, model, 1, snapshot)
							frame := model.Render(nil)
							qaUIFrame(t, frame, width+1, rows)
							text := strings.Join(frame, "\n")
							for _, hint := range []string{"a Auth", "l Links", "h Hooks", "s Sync", "w Worker", "q Quit", "? Help", "A Appearance"} {
								if !strings.Contains(text, hint) {
									t.Errorf("content width %d hides reachable primary action %q: %q", width, hint, text)
								}
							}
							want := []string{"Computer " + qaUIComputer, "No activity yet"}
							if state == "active" {
								want = []string{"Computer " + qaUIComputer, "Project 100", "Account 1", "Provisional union", "00:00:10", "Confirmed closed", "00:00:05"}
							}
							for _, information := range want {
								if !strings.Contains(text, information) {
									t.Errorf("footer displaced authoritative dashboard content %q: %q", information, text)
								}
							}
						})
					}
				})
			}
		})
	}
}
