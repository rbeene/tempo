package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

// prose is used only for human presentation. JSON encoders never receive it.
type prose struct {
	w      io.Writer
	styler terminal.Styler
	active bool
	err    error
}

func presentation(ctx context.Context, d Dependencies, destination, warnings io.Writer) prose {
	return themePresentation(ctx, d, destination, warnings, "")
}

func themePresentation(ctx context.Context, d Dependencies, destination, warnings io.Writer, id string) prose {
	p := prose{w: destination}
	caps := outputCapabilities(d, destination)
	if themes.ResolveColorMode(caps) == themes.ColorNone {
		return p
	}
	if id == "" {
		saved, err := themeService(d).Show(ctx, "")
		if err != nil {
			_, p.err = fmt.Fprintln(warnings, "tempo: appearance preferences unavailable; using terminal default")
			return p
		}
		id = saved.Theme.ID
	}
	p.styler, _ = themes.NewStyler(id, caps)
	p.active = p.styler != nil && p.styler.Paint(terminal.RoleText, "x") != "x"
	return p
}

// Output capability evidence is independent of input eligibility and shared by
// finite prose, the existing prompt owner, and the activity dashboard.
func outputCapabilities(d Dependencies, destination io.Writer) themes.Capabilities {
	getenv := d.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	tty := false
	if d.OutputTTY != nil {
		tty = d.OutputTTY(destination)
	} else if f, ok := destination.(*os.File); ok {
		tty = term.IsTerminal(int(f.Fd()))
	}
	return themes.Capabilities{OutputTTY: tty, Term: getenv("TERM"), ColorTerm: getenv("COLORTERM"), NoColor: getenv("NO_COLOR")}
}

func (p *prose) line(role terminal.Role, text string) {
	if p.err != nil {
		return
	}
	text = terminal.Sanitize(text)
	if p.styler != nil {
		text = p.styler.Paint(role, text)
	}
	_, p.err = fmt.Fprintln(p.w, text)
}
func (p *prose) close() error {
	if p.active {
		_, err := fmt.Fprint(p.w, "\x1b[0m")
		if p.err == nil {
			p.err = err
		}
	}
	return p.err
}

func printHumanHelp(p *prose) {
	var plain strings.Builder
	printHelp(&plain)
	for _, line := range strings.Split(strings.TrimSuffix(plain.String(), "\n"), "\n") {
		role := terminal.RoleText
		switch {
		case strings.HasPrefix(line, "Tempo"):
			role = terminal.RoleAccent
		case strings.HasPrefix(line, "Usage:"):
			role = terminal.RoleInfo
		case strings.HasPrefix(line, "Global options:"), strings.HasPrefix(line, "      --"):
			role = terminal.RoleKey
		case strings.HasPrefix(line, "Use "), strings.HasPrefix(line, "See "):
			role = terminal.RoleMuted
		}
		p.line(role, line)
	}
}

func printThemes(p *prose, data any) {
	switch result := data.(type) {
	case themes.ThemeList:
		p.line(terminal.RoleAccent, "Appearance")
		p.line(terminal.RoleInfo, "Preference revision: "+result.PreferenceRevision)
		for _, theme := range result.Themes {
			marker, role := "  ", terminal.RoleText
			if theme.Selected {
				marker, role = "> ", terminal.RoleSelection
			}
			label := marker + theme.Name + " (" + theme.ID + ")"
			if theme.Selected {
				label += " [saved]"
			}
			p.line(role, label)
		}
	case themes.ThemeResult:
		p.line(terminal.RoleAccent, result.Theme.Name+" ("+result.Theme.ID+")")
		state := "preview; not saved"
		if result.Theme.Selected {
			state = "saved"
		}
		p.line(terminal.RoleInfo, "Preference revision: "+result.PreferenceRevision+"; "+state)
		for _, sample := range []struct {
			role terminal.Role
			text string
		}{
			{terminal.RoleText, "Text: project timer"}, {terminal.RoleMuted, "Secondary: local activity"},
			{terminal.RoleBorder, "-----"}, {terminal.RoleSelection, "> Selected row [selected]"},
			{terminal.RoleSuccess, "Success: saved"}, {terminal.RoleWarning, "Warning: needs attention"},
			{terminal.RoleError, "Error: review required"}, {terminal.RoleKey, "Key hints: Enter / Escape"},
		} {
			p.line(sample.role, sample.text)
		}
		if result.Theme.Source != nil {
			p.line(terminal.RoleMuted, "Source: "+*result.Theme.Source+"; license: "+*result.Theme.License)
		}
	case activity.MutationResult:
		label := "Appearance saved"
		if !result.Changed {
			label = "Appearance already selected"
		}
		p.line(terminal.RoleSuccess, label)
		p.line(terminal.RoleInfo, "Preference revision: "+*result.EntityRevision+"; transaction: "+result.SnapshotRevision)
		p.line(terminal.RoleMuted, "Request: "+result.RequestID+"; reread themes show for current selection")
	}
}
