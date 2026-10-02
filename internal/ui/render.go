package ui

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rivo/uniseg"
)

// Render clips complete graphemes before styling. The last terminal column is
// reserved to avoid automatic wrapping; frame length never exceeds the viewport.
func (m *Model) Render(styler terminal.Styler) []string {
	width := max(0, m.columns-1)
	if m.rows <= 0 {
		return nil
	}
	if width == 0 {
		return []string{""}
	}
	lines := make([]string, 0, min(m.rows, 64))
	add := func(role terminal.Role, text string) {
		if len(lines) >= m.rows {
			return
		}
		text = clip(text, width)
		if styler != nil {
			text = styler.Paint(role, text)
		}
		lines = append(lines, text)
	}
	if m.columns < 40 || m.rows < 8 {
		add(terminal.RoleWarning, "Terminal too small")
		add(terminal.RoleMuted, "Resize to 40x8")
		if m.stale {
			add(terminal.RoleWarning, "Stale: "+m.reason)
		}
		add(terminal.RoleKey, "q Quit")
		return lines
	}
	add(terminal.RoleAccent, "TEMPO  /  LOCAL ACTIVITY")
	add(terminal.RoleBorder, strings.Repeat("─", width))
	if m.stale {
		add(terminal.RoleWarning, "Stale · "+m.reason)
	} else {
		scope := "Computer not initialized"
		if m.snapshot.ComputerID != nil {
			scope = "Computer " + *m.snapshot.ComputerID
		}
		add(terminal.RoleMuted, scope)
	}
	// Reserve two footer lines; each timer retains its two distinct duration
	// labels even when the optional state/count suffixes must be clipped.
	available := m.rows - len(lines) - 2
	if !m.observed {
		message := "Loading local activity…"
		if m.stale {
			message = "Local activity unavailable"
		}
		add(terminal.RoleInfo, message)
	} else if len(m.snapshot.ProjectTimers) == 0 {
		add(terminal.RoleInfo, "No activity yet")
		add(terminal.RoleMuted, "Link a project to begin local capture.")
	} else {
		capacity := max(1, available/3)
		start := max(0, m.selected-capacity+1)
		end := min(len(m.snapshot.ProjectTimers), start+capacity)
		for i := start; i < end; i++ {
			timer := m.snapshot.ProjectTimers[i]
			prefix, role := "  ", terminal.RoleText
			if i == m.selected {
				prefix, role = "> ", terminal.RoleSelection
			}
			add(role, prefix+"Project "+timer.ProjectID+"  ·  Account "+timer.AccountID)
			add(terminal.RoleText, fmt.Sprintf("  Provisional union  %s   %d active / %d waiting", duration(timer.ProvisionalUnionNS), len(timer.ActiveActorRefs), len(timer.WaitingActorRefs)))
			add(terminal.RoleMuted, fmt.Sprintf("  Confirmed closed   %s   %d queued / %d need attention", duration(timer.ConfirmedClosedNS), timer.QueuedCount, timer.NeedsAttentionCount))
		}
	}
	for len(lines) < m.rows-2 {
		add(terminal.RoleText, "")
	}
	syncState := "paused"
	if m.snapshot.SyncEnabled {
		syncState = "enabled"
	}
	worker := m.snapshot.Worker.State
	if worker == "" {
		worker = "unavailable"
	}
	add(terminal.RoleInfo, "Sync "+syncState+"  ·  Worker "+worker)
	add(terminal.RoleKey, "↑/↓ Select   r Refresh   q Quit")
	return lines
}

func duration(ns string) string {
	n, valid := canonicalCounter(ns)
	if !valid {
		return "unavailable"
	}
	seconds := n / 1_000_000_000
	return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
}

func clip(text string, cells int) string {
	text = sanitize(text)
	var b strings.Builder
	state := -1
	used := 0
	for len(text) > 0 {
		cluster, rest, width, next := uniseg.FirstGraphemeClusterInString(text, state)
		if used+width > cells {
			break
		}
		// Leading combining-only clusters must not modify the preceding UI span.
		if width > 0 || used > 0 {
			b.WriteString(cluster)
			used += width
		}
		text, state = rest, next
	}
	return b.String()
}

// sanitize strips complete escape strings (including their payload), terminal
// controls, malformed UTF-8 and bidi formatting while retaining emoji joiners.
func sanitize(text string) string {
	var b strings.Builder
	for i := 0; i < len(text); {
		if text[i] == 0x1b {
			i++
			if i >= len(text) {
				break
			}
			switch text[i] {
			case '[':
				i++
				for i < len(text) {
					c := text[i]
					i++
					if c >= 0x40 && c <= 0x7e {
						break
					}
				}
			case ']', 'P', '^', '_', 'X':
				i++
				for i < len(text) {
					if text[i] == 7 {
						i++
						break
					}
					if text[i] == 0x1b && i+1 < len(text) && text[i+1] == '\\' {
						i += 2
						break
					}
					i++
				}
			default:
				// Consume an ESC intermediate/final sequence such as charset selection.
				for i < len(text) && text[i] >= 0x20 && text[i] <= 0x2f {
					i++
				}
				if i < len(text) {
					i++
				}
			}
			continue
		}
		r, n := utf8.DecodeRuneInString(text[i:])
		i += n
		if r == utf8.RuneError || unicode.IsControl(r) || r >= 0x202a && r <= 0x202e || r >= 0x2066 && r <= 0x2069 || r == 0x061c || r == 0x200e || r == 0x200f || r == 0x2028 || r == 0x2029 {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
