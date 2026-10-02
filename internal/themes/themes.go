// Package themes defines Tempo's local appearance data and color policy.
package themes

// Palette maps presentation roles to RGB colors. An empty value inherits the
// terminal color; terminal-default uses empty values for every role.
type Palette struct {
	Background string `json:"background"`
	Text       string `json:"text"`
	Muted      string `json:"muted"`
	Border     string `json:"border"`
	Selection  string `json:"selection"`
	Accent     string `json:"accent"`
	Success    string `json:"success"`
	Warning    string `json:"warning"`
	Error      string `json:"error"`
	Info       string `json:"info"`
	Key        string `json:"key"`
}

type ThemeInfo struct {
	ID       string  `json:"id"`
	Name     string  `json:"name"`
	Selected bool    `json:"selected"`
	Palette  Palette `json:"palette"`
	Source   *string `json:"source"`
	License  *string `json:"license"`
}

// Capabilities is supplied by the presentation caller, never read from global
// environment or discovered through terminal queries by this package.
type Capabilities struct {
	OutputTTY bool
	Term      string
	ColorTerm string
	NoColor   string
}

type ColorMode string

const (
	ColorNone      ColorMode = "none"
	ColorANSI16    ColorMode = "ansi16"
	ColorANSI256   ColorMode = "ansi256"
	ColorTrueColor ColorMode = "truecolor"
)
