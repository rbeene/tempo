package themes

import (
	"fmt"
	"strconv"

	"github.com/rbeene/tempo/internal/terminal"
)

// spanStyler contains only immutable encodings. Each flat span clears inherited
// attributes before setting its role and restores the palette base afterward.
type spanStyler struct {
	roles [10]string
	base  string
}

func roleIndex(role terminal.Role) int {
	switch role {
	case terminal.RoleMuted:
		return 1
	case terminal.RoleBorder:
		return 2
	case terminal.RoleSelection:
		return 3
	case terminal.RoleAccent:
		return 4
	case terminal.RoleSuccess:
		return 5
	case terminal.RoleWarning:
		return 6
	case terminal.RoleError:
		return 7
	case terminal.RoleInfo:
		return 8
	case terminal.RoleKey:
		return 9
	default:
		return 0
	}
}

func (s spanStyler) Paint(role terminal.Role, text string) string {
	if text == "" || s.base == "" {
		return text
	}
	return s.roles[roleIndex(role)] + text + s.base
}

// NewStyler receives capabilities as values; it never reads preferences or
// environment, queries the terminal, or changes the emulator's palette.
func NewStyler(id string, caps Capabilities) (terminal.Styler, error) {
	info, err := Lookup(id)
	if err != nil {
		return nil, err
	}
	mode := ResolveColorMode(caps)
	var s spanStyler
	if id == "terminal-default" || mode == ColorNone {
		return s, nil
	}
	if mode == ColorANSI16 {
		s.base = "\x1b[0m"
		for i, code := range [...]string{"0", "90", "90", "7", "94", "92", "93", "91", "96", "95"} {
			s.roles[i] = "\x1b[0;" + code + "m"
		}
		return s, nil
	}
	p := info.Palette
	foreground := [...]string{p.Text, p.Muted, p.Border, p.Text, p.Accent, p.Success, p.Warning, p.Error, p.Info, p.Key}
	background, selection := "", ""
	if mode == ColorTrueColor {
		background, selection = rgbSGR(48, p.Background), rgbSGR(48, p.Selection)
		for i, color := range foreground {
			foreground[i] = rgbSGR(38, color)
		}
	} else {
		// Fixed xterm cube/grayscale mappings avoid user-defined ANSI slots.
		// Gruvbox selection/error are adjusted to retain readable contrast.
		indices := map[string][11]int{
			"tokyo-night": {234, 153, 146, 239, 237, 111, 149, 179, 210, 117, 141},
			"gruvbox":     {235, 180, 138, 59, 238, 109, 143, 179, 203, 108, 174},
			"catppuccin":  {235, 189, 146, 241, 239, 111, 151, 223, 211, 116, 218},
		}[id]
		background, selection = indexedSGR(48, indices[0]), indexedSGR(48, indices[4])
		for i := range foreground {
			index := i + 1
			if i == 3 {
				index = 1
			}
			foreground[i] = indexedSGR(38, indices[index])
		}
	}
	s.base = "\x1b[0;" + foreground[0] + ";" + background + "m"
	for i, fg := range foreground {
		bg := background
		if i == 3 {
			bg = selection
		}
		s.roles[i] = "\x1b[0;" + fg + ";" + bg + "m"
	}
	return s, nil
}

func indexedSGR(channel, index int) string { return fmt.Sprintf("%d;5;%d", channel, index) }
func rgbSGR(channel int, color string) string {
	n, _ := strconv.ParseUint(color[1:], 16, 24) // Only the validated built-in registry reaches here.
	return fmt.Sprintf("%d;2;%d;%d;%d", channel, n>>16, n>>8&255, n&255)
}
