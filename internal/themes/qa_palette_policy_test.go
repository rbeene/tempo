package themes_test

import (
	"crypto/sha256"
	"fmt"
	"math"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/rbeene/tempo/internal/themes"
)

// These values are the approved palette oracle, deliberately independent of
// production constants and upstream moving branches.
func qaApprovedThemes() []themes.ThemeInfo {
	return []themes.ThemeInfo{
		{ID: "terminal-default", Name: "Terminal default"},
		{ID: "tokyo-night", Name: "Tokyo Night", Palette: themes.Palette{
			Background: "#1a1b26", Text: "#c0caf5", Muted: "#a9b1d6", Border: "#414868", Selection: "#283457",
			Accent: "#7aa2f7", Success: "#9ece6a", Warning: "#e0af68", Error: "#f7768e", Info: "#7dcfff", Key: "#bb9af7",
		}},
		{ID: "gruvbox", Name: "Gruvbox (Material dark)", Palette: themes.Palette{
			Background: "#282828", Text: "#d4be98", Muted: "#a89984", Border: "#665c54", Selection: "#504945",
			Accent: "#7daea3", Success: "#a9b665", Warning: "#d8a657", Error: "#ea6962", Info: "#89b482", Key: "#d3869b",
		}},
		{ID: "catppuccin", Name: "Catppuccin Mocha", Palette: themes.Palette{
			Background: "#1e1e2e", Text: "#cdd6f4", Muted: "#a6adc8", Border: "#585b70", Selection: "#45475a",
			Accent: "#89b4fa", Success: "#a6e3a1", Warning: "#f9e2af", Error: "#f38ba8", Info: "#94e2d5", Key: "#f5c2e7",
		}},
	}
}

func TestQAPaletteRegistryApprovedValues(t *testing.T) {
	want := qaApprovedThemes()
	got := themes.Catalog()
	if len(got) != len(want) {
		t.Errorf("catalog has %d themes, want exactly four", len(got))
	}
	seen := map[string]bool{}
	for _, item := range got {
		if seen[item.ID] {
			t.Errorf("duplicate ID %q", item.ID)
		}
		seen[item.ID] = true
	}
	for _, expected := range want {
		t.Run(expected.ID, func(t *testing.T) {
			actual, err := themes.Lookup(expected.ID)
			if err != nil {
				t.Fatalf("known theme lookup: %v", err)
			}
			if actual.ID != expected.ID || actual.Name != expected.Name || actual.Palette != expected.Palette {
				t.Errorf("theme = %+v, want ID/name/palette %+v", actual, expected)
			}
			if actual.Selected {
				t.Error("static registry must not pretend to know persisted selection")
			}
			found := false
			for _, item := range got {
				if item.ID == expected.ID {
					found = true
					if !reflect.DeepEqual(item, actual) {
						t.Errorf("Catalog and Lookup disagree: %+v versus %+v", item, actual)
					}
				}
			}
			if !found {
				t.Errorf("catalog missing %q", expected.ID)
			}
			if expected.ID == "terminal-default" && (actual.Source != nil || actual.License != nil) {
				t.Error("inherited terminal colors must have null source and license")
			}
		})
	}
}

func TestQAPaletteUnknownIDsReject(t *testing.T) {
	for _, id := range []string{"", "default", "Tokyo Night", "TOKYO-NIGHT", " tokyo-night", "gruvbox ", "catppuccin-latte", "../tokyo-night", "\x1b[31m"} {
		t.Run(strconv.Quote(id), func(t *testing.T) {
			if _, err := themes.Lookup(id); err == nil {
				t.Errorf("unknown or noncanonical theme %q accepted", id)
			}
		})
	}
}

func TestQAPaletteRegistryReturnsIndependentValues(t *testing.T) {
	for _, expected := range qaApprovedThemes() {
		original, err := themes.Lookup(expected.ID)
		if err != nil {
			t.Fatalf("lookup %s: %v", expected.ID, err)
		}
		// Capture pointee values separately: retaining a pointer would allow a
		// shared-metadata bug to mutate both sides of our comparison.
		var source, license string
		if original.Source != nil {
			source = *original.Source
		}
		if original.License != nil {
			license = *original.License
		}
		mutate := func(v *themes.ThemeInfo) {
			v.ID, v.Name, v.Selected = "changed", "changed", true
			v.Palette.Text = "#000000"
			if v.Source != nil {
				*v.Source = "changed"
			}
			if v.License != nil {
				*v.License = "changed"
			}
		}
		mutate(&original)
		catalog := themes.Catalog()
		for i := range catalog {
			mutate(&catalog[i])
		}
		again, err := themes.Lookup(expected.ID)
		if err != nil {
			t.Fatalf("lookup after mutation: %v", err)
		}
		if again.ID != expected.ID || again.Name != expected.Name || again.Palette != expected.Palette || again.Selected {
			t.Errorf("caller mutation changed registry %s: %+v", expected.ID, again)
		}
		if source != "" && (again.Source == nil || *again.Source != source) {
			t.Errorf("source aliased for %s", expected.ID)
		}
		if license != "" && (again.License == nil || *again.License != license) {
			t.Errorf("license aliased for %s", expected.ID)
		}
		fresh := themes.Catalog()
		if len(fresh) != 4 {
			t.Errorf("catalog damaged by caller mutation: %+v", fresh)
		}
		for _, item := range fresh {
			if item.ID == "changed" || item.Name == "changed" || item.Selected {
				t.Errorf("catalog shares caller-owned values: %+v", item)
			}
		}
	}
}

func TestQAPaletteMeaningfulTextContrast(t *testing.T) {
	for _, expected := range qaApprovedThemes()[1:] {
		t.Run(expected.ID, func(t *testing.T) {
			actual, err := themes.Lookup(expected.ID)
			if err != nil {
				t.Fatal(err)
			}
			p := actual.Palette
			for role, fg := range map[string]string{"text": p.Text, "muted": p.Muted, "accent": p.Accent, "success": p.Success, "warning": p.Warning, "error": p.Error, "info": p.Info, "key": p.Key} {
				if ratio := qaContrast(t, fg, p.Background); ratio < 4.5 {
					t.Errorf("%s/base contrast %.3f is below 4.5", role, ratio)
				}
			}
			if ratio := qaContrast(t, p.Text, p.Selection); ratio < 4.5 {
				t.Errorf("text/selection contrast %.3f is below 4.5", ratio)
			}
			if p.Muted == p.Border {
				t.Error("decorative dark border used for meaningful secondary text")
			}
		})
	}
}

func qaContrast(t *testing.T, a, b string) float64 {
	t.Helper()
	luminance := func(hex string) float64 {
		if !regexp.MustCompile(`^#[0-9a-f]{6}$`).MatchString(hex) {
			t.Fatalf("not exact RGB: %q", hex)
		}
		var linear [3]float64
		for i := range linear {
			component, err := strconv.ParseUint(hex[1+i*2:3+i*2], 16, 8)
			if err != nil {
				t.Fatal(err)
			}
			v := float64(component) / 255
			if v <= 0.04045 {
				linear[i] = v / 12.92
			} else {
				linear[i] = math.Pow((v+0.055)/1.055, 2.4)
			}
		}
		return .2126*linear[0] + .7152*linear[1] + .0722*linear[2]
	}
	x, y := luminance(a), luminance(b)
	if x < y {
		x, y = y, x
	}
	return (x + .05) / (y + .05)
}

func TestQAPaletteImmutableAttributionAndCompleteNotices(t *testing.T) {
	immutable := regexp.MustCompile(`^https://(?:github\.com/[^/]+/[^/]+/blob|raw\.githubusercontent\.com/[^/]+/[^/]+)/[0-9a-f]{40}/[^?#]+$`)
	repositories := map[string][]string{
		"tokyo-night": {"tokyo-night/tokyo-night-vscode-theme", "enkia/tokyo-night-vscode-theme"},
		"gruvbox":     {"omacom/omarchy", "sainnhe/gruvbox-material"},
		"catppuccin":  {"omacom/omarchy", "catppuccin/palette"},
	}
	// Independently fetched and checked against the source manifest; these
	// exact pins replace the earlier URL-shape-only evidence.
	pinnedSources := map[string]string{
		"tokyo-night": "https://github.com/tokyo-night/tokyo-night-vscode-theme/blob/7c0f11eaef322f293621ca7befe462214b7ea468/README.md",
		"gruvbox":     "https://github.com/omacom/omarchy/blob/821ae589059ffdadc970315f866c94b55d268af7/themes/gruvbox/colors.toml",
		"catppuccin":  "https://github.com/omacom/omarchy/blob/821ae589059ffdadc970315f866c94b55d268af7/themes/catppuccin/colors.toml",
	}
	for id, repos := range repositories {
		info, err := themes.Lookup(id)
		if err != nil {
			t.Errorf("lookup %s: %v", id, err)
			continue
		}
		if info.Source == nil || !immutable.MatchString(*info.Source) {
			t.Errorf("%s source must identify immutable upstream file, got %v", id, info.Source)
		} else {
			matched := false
			for _, repo := range repos {
				matched = matched || strings.Contains(*info.Source, "/"+repo+"/")
			}
			if !matched {
				t.Errorf("%s source is not its actual palette family: %s", id, *info.Source)
			}
		}
		if info.License == nil || *info.License != "MIT" {
			t.Errorf("%s missing accurate MIT license metadata", id)
		}
		if info.Source == nil || *info.Source != pinnedSources[id] {
			t.Errorf("%s source does not match independently verified immutable evidence", id)
		}
	}
	notices, err := os.ReadFile("../../THIRD_PARTY_NOTICES.md")
	if err != nil {
		t.Fatalf("read shipped attribution: %v", err)
	}
	text := strings.Join(strings.Fields(string(notices)), " ")
	// Hash the complete upstream text after whitespace normalization so normal
	// Markdown wrapping is allowed, while no license sentence may be omitted.
	licenseHashes := map[string]string{
		"Tokyo Night":      "185a9a0acfa1f4e12338f65608c9e3933f8041df294fb5cb58cf95f4c6f01c3f",
		"Omarchy":          "3ddd3a5a80e5d9d803af3e9d91a7f700a6e2bf00c2b50aa1b0f8ebb47c12c9f4",
		"Gruvbox Material": "de7da56196f28e3202e61a131f9536ed6c4e67d423c9812e451df4795a34d042",
		"Catppuccin":       "1df5eb833200f4a6a7ce160747a478c859137ce3f581ea20104d816401670f1b",
	}
	blocks := regexp.MustCompile("(?s)```text\\n(.*?)\\n```").FindAllSubmatch(notices, -1)
	for name, want := range licenseHashes {
		found := false
		for _, block := range blocks {
			normalized := strings.Join(strings.Fields(string(block[1])), " ")
			found = found || fmt.Sprintf("%x", sha256.Sum256([]byte(normalized))) == want
		}
		if !found {
			t.Errorf("complete verified %s license not retained", name)
		}
	}
	for _, owner := range []string{"Enkia", "sainnhe", "Catppuccin", "David Heinemeier Hansson"} {
		if !strings.Contains(text, owner) {
			t.Errorf("missing copyright owner %q", owner)
		}
	}
	// A link or SPDX label alone does not preserve the license. Each copied
	// project's notice must retain the complete grant, condition and disclaimer.
	for _, clause := range []string{
		"Permission is hereby granted, free of charge, to any person obtaining a copy",
		"to use, copy, modify, merge, publish, distribute, sublicense, and/or sell",
		"The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.",
		`THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.`,
		"IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY",
	} {
		if count := strings.Count(text, clause); count < 4 {
			t.Errorf("full MIT clause retained %d times, want all four source notices: %q", count, clause)
		}
	}
}

func TestQAColorPolicyKnownTerminalMatrix(t *testing.T) {
	basic := []string{"ansi", "linux", "xterm", "xterm-color", "screen", "tmux", "rxvt", "rxvt-unicode"}
	indexed := []string{"xterm-256color", "screen-256color", "tmux-256color", "rxvt-unicode-256color", "alacritty", "xterm-kitty", "xterm-ghostty", "foot", "foot-extra", "wezterm"}
	for _, group := range []struct {
		terms []string
		mode  themes.ColorMode
	}{{basic, themes.ColorANSI16}, {indexed, themes.ColorANSI256}} {
		for _, term := range group.terms {
			for _, hint := range []string{"", "truecolor", "24bit", "TRUECOLOR", "24BIT", "unknown", "truecolor-extra"} {
				t.Run(term+"/"+hint, func(t *testing.T) {
					want := group.mode
					if strings.EqualFold(hint, "truecolor") || strings.EqualFold(hint, "24bit") {
						want = themes.ColorTrueColor
					}
					caps := themes.Capabilities{OutputTTY: true, Term: term, ColorTerm: hint, NoColor: ""}
					if got := themes.ResolveColorMode(caps); got != want {
						t.Errorf("%+v => %q, want %q", caps, got, want)
					}
				})
			}
		}
	}
}

func TestQAColorPolicyDisablingConditionsTakePrecedence(t *testing.T) {
	for _, term := range []string{"", "dumb", "unknown", "vt100", "xterm-unrecognized-suffix", "invented-256color", "xterm-direct", "tmux-direct", "XTERM", " xterm", "xterm\n", "alacritty-unknown", "kitty", "foot-direct", "wezterm-future"} {
		for _, hint := range []string{"", "truecolor", "24bit"} {
			caps := themes.Capabilities{OutputTTY: true, Term: term, ColorTerm: hint}
			if got := themes.ResolveColorMode(caps); got != themes.ColorNone {
				t.Errorf("unsupported terminal %+v => %q", caps, got)
			}
		}
	}
	for _, term := range []string{"xterm", "xterm-256color", "alacritty", "xterm-kitty", "xterm-ghostty", "foot", "foot-extra", "wezterm"} {
		for _, hint := range []string{"", "truecolor", "24bit"} {
			for _, noColor := range []string{"1", "0", "false", " "} {
				caps := themes.Capabilities{OutputTTY: true, Term: term, ColorTerm: hint, NoColor: noColor}
				if got := themes.ResolveColorMode(caps); got != themes.ColorNone {
					t.Errorf("nonempty NO_COLOR %+v => %q", caps, got)
				}
			}
			caps := themes.Capabilities{OutputTTY: false, Term: term, ColorTerm: hint}
			if got := themes.ResolveColorMode(caps); got != themes.ColorNone {
				t.Errorf("redirected output %+v => %q", caps, got)
			}
		}
	}
}
