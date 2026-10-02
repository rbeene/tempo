package themes

import "errors"

// Catalog returns independent static metadata. Persisted selection belongs to
// the preference service, not this registry. See THIRD_PARTY_NOTICES.md for the
// pinned upstream sources, full licenses and Tempo's semantic role adaptations.
func Catalog() []ThemeInfo {
	return []ThemeInfo{
		{ID: "terminal-default", Name: "Terminal default"},
		namedTheme("tokyo-night", "Tokyo Night",
			"https://github.com/tokyo-night/tokyo-night-vscode-theme/blob/7c0f11eaef322f293621ca7befe462214b7ea468/README.md",
			Palette{
				Background: "#1a1b26", Text: "#c0caf5", Muted: "#a9b1d6", Border: "#414868", Selection: "#283457",
				Accent: "#7aa2f7", Success: "#9ece6a", Warning: "#e0af68", Error: "#f7768e", Info: "#7dcfff", Key: "#bb9af7",
			}),
		namedTheme("gruvbox", "Gruvbox (Material dark)",
			"https://github.com/omacom/omarchy/blob/821ae589059ffdadc970315f866c94b55d268af7/themes/gruvbox/colors.toml",
			Palette{
				Background: "#282828", Text: "#d4be98", Muted: "#a89984", Border: "#665c54", Selection: "#504945",
				Accent: "#7daea3", Success: "#a9b665", Warning: "#d8a657", Error: "#ea6962", Info: "#89b482", Key: "#d3869b",
			}),
		namedTheme("catppuccin", "Catppuccin Mocha",
			"https://github.com/omacom/omarchy/blob/821ae589059ffdadc970315f866c94b55d268af7/themes/catppuccin/colors.toml",
			Palette{
				Background: "#1e1e2e", Text: "#cdd6f4", Muted: "#a6adc8", Border: "#585b70", Selection: "#45475a",
				Accent: "#89b4fa", Success: "#a6e3a1", Warning: "#f9e2af", Error: "#f38ba8", Info: "#94e2d5", Key: "#f5c2e7",
			}),
	}
}

func namedTheme(id, name, source string, palette Palette) ThemeInfo {
	license := "MIT"
	return ThemeInfo{ID: id, Name: name, Palette: palette, Source: &source, License: &license}
}

// Lookup accepts only the stable canonical IDs. It performs no preference or
// environment reads and returns caller-owned metadata, including source pointers.
func Lookup(id string) (ThemeInfo, error) {
	for _, theme := range Catalog() {
		if theme.ID == id {
			return theme, nil
		}
	}
	return ThemeInfo{}, errors.New("unknown theme")
}
