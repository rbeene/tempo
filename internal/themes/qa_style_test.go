package themes_test

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rbeene/tempo/internal/themes"
)

type qaSGRState struct {
	fg, bg                                string
	reverse, bold, dim, italic, underline bool
}
type qaStyledRune struct {
	r     rune
	state qaSGRState
}

// Small independent terminal-state interpreter: only SGR is allowed. A color
// string that accidentally carries OSC, movement or malformed controls fails.
func qaParseSGR(t *testing.T, text string) ([]qaStyledRune, qaSGRState) {
	t.Helper()
	state := qaSGRState{fg: "default", bg: "default"}
	var visible []qaStyledRune
	for len(text) > 0 {
		if text[0] != 27 {
			r, n := utf8.DecodeRuneInString(text)
			if r < 32 || r == 127 {
				t.Fatalf("unexpected non-SGR control U+%04X", r)
			}
			visible = append(visible, qaStyledRune{r, state})
			text = text[n:]
			continue
		}
		if !strings.HasPrefix(text, "\x1b[") {
			t.Fatalf("forbidden terminal sequence %q", text)
		}
		end := strings.IndexByte(text, 'm')
		if end < 0 {
			t.Fatalf("unterminated SGR %q", text)
		}
		payload := text[2:end]
		text = text[end+1:]
		parts := strings.Split(payload, ";")
		params := make([]int, len(parts))
		for i, p := range parts {
			if p == "" {
				params[i] = 0
				continue
			}
			v, err := strconv.Atoi(p)
			if err != nil || v < 0 {
				t.Fatalf("not plain SGR %q", payload)
			}
			params[i] = v
		}
		for i := 0; i < len(params); i++ {
			p := params[i]
			switch {
			case p == 0:
				state = qaSGRState{fg: "default", bg: "default"}
			case p == 1:
				state.bold = true
			case p == 2:
				state.dim = true
			case p == 3:
				state.italic = true
			case p == 4:
				state.underline = true
			case p == 7:
				state.reverse = true
			case p == 22:
				state.bold = false
				state.dim = false
			case p == 23:
				state.italic = false
			case p == 24:
				state.underline = false
			case p == 27:
				state.reverse = false
			case p == 39:
				state.fg = "default"
			case p == 49:
				state.bg = "default"
			case p == 38 || p == 48:
				if i+2 >= len(params) {
					t.Fatal("incomplete extended color")
				}
				var color string
				if params[i+1] == 5 {
					if params[i+2] > 255 {
						t.Fatal("bad index")
					}
					color = fmt.Sprintf("index:%d", params[i+2])
					i += 2
				} else if params[i+1] == 2 {
					if i+4 >= len(params) {
						t.Fatal("incomplete RGB")
					}
					for _, c := range params[i+2 : i+5] {
						if c > 255 {
							t.Fatal("bad RGB")
						}
					}
					color = fmt.Sprintf("rgb:#%02x%02x%02x", params[i+2], params[i+3], params[i+4])
					i += 4
				} else {
					t.Fatalf("unknown color mode in %q", payload)
				}
				if p == 38 {
					state.fg = color
				} else {
					state.bg = color
				}
			case (p >= 30 && p <= 37) || (p >= 90 && p <= 97):
				state.fg = fmt.Sprintf("ansi:%d", p)
			case (p >= 40 && p <= 47) || (p >= 100 && p <= 107):
				state.bg = fmt.Sprintf("ansi:%d", p)
			default:
				t.Fatalf("unexpected SGR attribute %d", p)
			}
		}
	}
	return visible, state
}

var qaStyleRoles = []terminal.Role{terminal.RoleText, terminal.RoleMuted, terminal.RoleBorder, terminal.RoleSelection, terminal.RoleAccent, terminal.RoleSuccess, terminal.RoleWarning, terminal.RoleError, terminal.RoleInfo, terminal.RoleKey}

func qaStyleColor(p themes.Palette, role terminal.Role) string {
	switch role {
	case terminal.RoleMuted:
		return p.Muted
	case terminal.RoleBorder:
		return p.Border
	case terminal.RoleAccent:
		return p.Accent
	case terminal.RoleSuccess:
		return p.Success
	case terminal.RoleWarning:
		return p.Warning
	case terminal.RoleError:
		return p.Error
	case terminal.RoleInfo:
		return p.Info
	case terminal.RoleKey:
		return p.Key
	default:
		return p.Text
	}
}

// Index order: base, text, muted, border, selection, accent, success, warning,
// error, info, key. Gruvbox selection/error use reviewed contrast corrections.
var qaThemeIndices = map[string][]int{
	"tokyo-night": {234, 153, 146, 239, 237, 111, 149, 179, 210, 117, 141},
	"gruvbox":     {235, 180, 138, 59, 238, 109, 143, 179, 203, 108, 174},
	"catppuccin":  {235, 189, 146, 241, 239, 111, 151, 223, 211, 116, 218},
}

func TestQAStyleExactRGBAndQuantizedRolesRestoreBase(t *testing.T) {
	for _, info := range qaApprovedThemes()[1:] {
		for _, mode := range []string{"truecolor", "256"} {
			t.Run(info.ID+"/"+mode, func(t *testing.T) {
				caps := themes.Capabilities{OutputTTY: true, Term: "xterm-256color"}
				if mode == "truecolor" {
					caps.ColorTerm = "truecolor"
				}
				styler, err := themes.NewStyler(info.ID, caps)
				if err != nil {
					t.Fatal(err)
				}
				base := qaSGRState{fg: "rgb:" + info.Palette.Text, bg: "rgb:" + info.Palette.Background}
				indices := qaThemeIndices[info.ID]
				if mode == "256" {
					base.fg = fmt.Sprintf("index:%d", indices[1])
					base.bg = fmt.Sprintf("index:%d", indices[0])
				}
				for i, role := range qaStyleRoles {
					want := base
					want.fg = "rgb:" + qaStyleColor(info.Palette, role)
					if role == terminal.RoleSelection {
						want.bg = "rgb:" + info.Palette.Selection
					}
					if mode == "256" {
						idx := i + 1
						if role == terminal.RoleSelection {
							idx = 1
						}
						want.fg = fmt.Sprintf("index:%d", indices[idx])
						if role == terminal.RoleSelection {
							want.bg = fmt.Sprintf("index:%d", indices[4])
						}
					}
					label := "> Café 🚀 [active]"
					painted := styler.Paint(role, label)
					runes, final := qaParseSGR(t, painted)
					var plain strings.Builder
					badState := false
					for _, r := range runes {
						plain.WriteRune(r.r)
						if r.state != want && !badState {
							t.Errorf("%s visible rune %q state=%+v want=%+v", role, r.r, r.state, want)
							badState = true
						}
					}
					if plain.String() != label {
						t.Errorf("%s changed text: %q", role, plain.String())
					}
					if final != base {
						t.Errorf("%s ended at %+v, want palette base %+v", role, final, base)
					}
				}
				if styler.Paint(terminal.RoleText, "") != "" {
					t.Error("empty span emitted style bytes")
				}
				if styler.Paint(terminal.Role("future"), "label") != styler.Paint(terminal.RoleText, "label") {
					t.Error("unknown role failed safe text fallback")
				}
			})
		}
	}
}

func TestQAStyleANSI16UsesSemanticColorsAndInheritedBackground(t *testing.T) {
	wantFG := []string{"default", "ansi:90", "ansi:90", "default", "ansi:94", "ansi:92", "ansi:93", "ansi:91", "ansi:96", "ansi:95"}
	for _, id := range []string{"tokyo-night", "gruvbox", "catppuccin"} {
		styler, err := themes.NewStyler(id, themes.Capabilities{OutputTTY: true, Term: "xterm"})
		if err != nil {
			t.Fatal(err)
		}
		for i, role := range qaStyleRoles {
			runes, final := qaParseSGR(t, styler.Paint(role, "> running"))
			want := qaSGRState{fg: wantFG[i], bg: "default", reverse: role == terminal.RoleSelection}
			for _, r := range runes {
				if r.state != want {
					t.Errorf("%s/%s state %+v want %+v", id, role, r.state, want)
					break
				}
			}
			if len(runes) != 9 || final != (qaSGRState{fg: "default", bg: "default"}) {
				t.Errorf("%s/%s lost text or leaked attributes: %+v", id, role, final)
			}
		}
	}
}

func TestQAStyleDisabledAndTerminalDefaultAreByteIdentity(t *testing.T) {
	for _, id := range []string{"terminal-default", "tokyo-night", "gruvbox", "catppuccin"} {
		for _, caps := range []themes.Capabilities{
			{OutputTTY: false, Term: "xterm-256color", ColorTerm: "truecolor"},
			{OutputTTY: true, Term: "xterm-256color", ColorTerm: "truecolor", NoColor: "0"},
			{OutputTTY: true, Term: "dumb", ColorTerm: "truecolor"},
			{OutputTTY: true, Term: "unknown", ColorTerm: "truecolor"},
		} {
			styler, err := themes.NewStyler(id, caps)
			if err != nil {
				t.Fatal(err)
			}
			for _, role := range qaStyleRoles {
				if got := styler.Paint(role, "> running [saved]"); got != "> running [saved]" {
					t.Errorf("disabled %s/%s altered plain output %q", id, role, got)
				}
			}
		}
	}
	for _, caps := range []themes.Capabilities{{OutputTTY: true, Term: "xterm"}, {OutputTTY: true, Term: "xterm-256color"}, {OutputTTY: true, Term: "xterm-256color", ColorTerm: "truecolor"}} {
		styler, err := themes.NewStyler("terminal-default", caps)
		if err != nil {
			t.Fatal(err)
		}
		for _, role := range qaStyleRoles {
			if got := styler.Paint(role, "> selection"); got != "> selection" {
				t.Errorf("default chose colors or attributes: %q", got)
			}
		}
	}
	if _, err := themes.NewStyler("unknown", themes.Capabilities{OutputTTY: true, Term: "xterm"}); err == nil {
		t.Error("unknown theme style accepted")
	}
}

func qaXtermRGB(index int) string {
	if index >= 232 {
		v := 8 + 10*(index-232)
		return fmt.Sprintf("#%02x%02x%02x", v, v, v)
	}
	steps := []int{0, 95, 135, 175, 215, 255}
	n := index - 16
	return fmt.Sprintf("#%02x%02x%02x", steps[n/36], steps[(n/6)%6], steps[n%6])
}

func TestQAStyleActualQuantizedTextPairsMeetContrast(t *testing.T) {
	for _, id := range []string{"tokyo-night", "gruvbox", "catppuccin"} {
		styler, err := themes.NewStyler(id, themes.Capabilities{OutputTTY: true, Term: "xterm-256color"})
		if err != nil {
			t.Fatal(err)
		}
		for _, role := range qaStyleRoles {
			if role == terminal.RoleBorder {
				continue
			}
			runes, _ := qaParseSGR(t, styler.Paint(role, "x"))
			if len(runes) != 1 {
				t.Fatalf("missing visible span")
			}
			var fg, bg int
			if _, err = fmt.Sscanf(runes[0].state.fg, "index:%d", &fg); err != nil {
				t.Fatalf("no actual indexed fg for %s/%s: %+v", id, role, runes[0].state)
			}
			if _, err = fmt.Sscanf(runes[0].state.bg, "index:%d", &bg); err != nil {
				t.Fatal(err)
			}
			if fg < 16 || fg > 255 || bg < 16 || bg > 255 {
				t.Fatal("contrast depends on user-defined ANSI slots")
			}
			if ratio := qaContrast(t, qaXtermRGB(fg), qaXtermRGB(bg)); ratio < 4.5 {
				t.Errorf("%s/%s actual quantized contrast %.3f <4.5", id, role, ratio)
			}
		}
	}
}

func TestQAStyleSelectionRestorationAndIndependentPreview(t *testing.T) {
	caps := themes.Capabilities{OutputTTY: true, Term: "xterm-256color", ColorTerm: "truecolor"}
	saved, err := themes.NewStyler("tokyo-night", caps)
	if err != nil {
		t.Fatal(err)
	}
	before := saved.Paint(terminal.RoleSelection, "> saved")
	preview, err := themes.NewStyler("gruvbox", caps)
	if err != nil {
		t.Fatal(err)
	}
	_ = preview.Paint(terminal.RoleKey, "preview")
	if got := saved.Paint(terminal.RoleSelection, "> saved"); got != before {
		t.Error("local preview changed existing immutable styler")
	}
	visible, final := qaParseSGR(t, before+saved.Paint(terminal.RoleMuted, " secondary"))
	want := qaSGRState{fg: "rgb:#a9b1d6", bg: "rgb:#1a1b26"}
	for _, r := range visible[7:] {
		if !reflect.DeepEqual(r.state, want) {
			t.Errorf("selection leaked into following secondary text %+v", r.state)
			break
		}
	}
	if final != (qaSGRState{fg: "rgb:#c0caf5", bg: "rgb:#1a1b26"}) {
		t.Errorf("last span lost palette base %+v", final)
	}
}
