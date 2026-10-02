package cli

import (
	"context"
	"strings"

	"github.com/rbeene/tempo/internal/themes"
)

func themeCommand(name string) bool { return strings.HasPrefix(name, "themes ") }

func themeService(d Dependencies) *themes.Service {
	if d.Themes != nil {
		return d.Themes
	}
	return themes.New(themes.Options{Getenv: d.Getenv})
}

func validateThemeCLI(p *parsed) error {
	for _, flag := range []string{"account", "yes"} {
		if _, present := p.flags[flag]; present {
			return problem("usage", "--"+flag+" is not valid for appearance commands")
		}
	}
	return nil
}

func executeThemes(ctx context.Context, p parsed, d Dependencies) (any, error) {
	s := themeService(d)
	switch p.command.Name {
	case "themes list":
		return s.List(ctx)
	case "themes show":
		id := ""
		if len(p.args) != 0 {
			id = p.args[0]
		}
		return s.Show(ctx, id)
	case "themes set":
		return s.Set(ctx, themes.SetInput{Theme: p.args[0], IfRevision: p.flags["if-revision"], RequestID: p.flags["request-id"]})
	case "themes reset":
		return s.Reset(ctx, themes.ResetInput{IfRevision: p.flags["if-revision"], RequestID: p.flags["request-id"]})
	}
	return nil, problem("usage", "unknown appearance command")
}
