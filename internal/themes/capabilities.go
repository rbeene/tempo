package themes

import "strings"

// ResolveColorMode uses only caller-supplied evidence. Exact recognized TERM
// names prevent an arbitrary suffix from opting an unknown terminal into color.
// NO_COLOR and redirected output take precedence over any capability hint.
func ResolveColorMode(c Capabilities) ColorMode {
	if !c.OutputTTY || c.NoColor != "" {
		return ColorNone
	}
	var mode ColorMode
	switch c.Term {
	case "ansi", "linux", "xterm", "xterm-color", "screen", "tmux", "rxvt", "rxvt-unicode":
		mode = ColorANSI16
	case "xterm-256color", "screen-256color", "tmux-256color", "rxvt-unicode-256color",
		"alacritty", "xterm-kitty", "xterm-ghostty", "foot", "foot-extra", "wezterm":
		mode = ColorANSI256
	default:
		return ColorNone
	}
	if strings.EqualFold(c.ColorTerm, "truecolor") || strings.EqualFold(c.ColorTerm, "24bit") {
		return ColorTrueColor
	}
	return mode
}
