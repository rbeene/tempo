package ui

import (
	"context"
	"strings"

	"github.com/rbeene/tempo/internal/terminal"
	"github.com/rivo/uniseg"
)

// promptBridge adapts shared guided services to the runner's sole input
// consumer. Neither a service nor this bridge writes to the terminal.
type promptBridge struct {
	ctx      context.Context
	requests chan promptRequest
}
type promptRequest struct {
	kind        string
	ctx         context.Context
	title       string
	body        string
	choices     []terminal.Choice
	defaultText string
	reply       chan promptReply
}
type promptReply struct {
	choiceID, text string
	secret         []byte
	confirmed      bool
	err            error
}

func newPromptBridge(ctx context.Context) *promptBridge {
	return &promptBridge{ctx: ctx, requests: make(chan promptRequest, 1)}
}
func (p *promptBridge) Choose(ctx context.Context, title string, choices []terminal.Choice) (string, error) {
	r := p.ask(promptRequest{kind: "choose", ctx: ctx, title: title, choices: append([]terminal.Choice(nil), choices...)})
	defer clear(r.secret)
	return r.choiceID, r.err
}
func (p *promptBridge) Text(ctx context.Context, title, defaultText string) (string, error) {
	r := p.ask(promptRequest{kind: "text", ctx: ctx, title: title, defaultText: defaultText})
	defer clear(r.secret)
	return r.text, r.err
}
func (p *promptBridge) Secret(ctx context.Context, title string) ([]byte, error) {
	r := p.ask(promptRequest{kind: "secret", ctx: ctx, title: title})
	defer clear(r.secret)
	return append([]byte(nil), r.secret...), r.err
}
func (p *promptBridge) Confirm(ctx context.Context, title string) (bool, error) {
	r := p.ask(promptRequest{kind: "confirm", ctx: ctx, title: title})
	defer clear(r.secret)
	return r.confirmed, r.err
}

// View displays complete read-only details through the same modal owner.
func (p *promptBridge) View(ctx context.Context, title, body string) error {
	r := p.ask(promptRequest{kind: "view", ctx: ctx, title: title, body: body})
	defer clear(r.secret)
	return r.err
}
func (p *promptBridge) ask(request promptRequest) (reply promptReply) {
	request.reply = make(chan promptReply, 1)
	canceled := func() error {
		if request.ctx.Err() != nil {
			return context.Cause(request.ctx)
		}
		if p.ctx.Err() != nil {
			return context.Cause(p.ctx)
		}
		return nil
	}
	defer func() {
		if reply.err != nil {
			clear(reply.secret)
			reply.secret = nil
			select {
			case abandoned := <-request.reply:
				clear(abandoned.secret)
			default:
			}
		}
	}()
	if err := canceled(); err != nil {
		return promptReply{err: err}
	}
	select {
	case p.requests <- request:
	case <-request.ctx.Done():
		return promptReply{err: context.Cause(request.ctx)}
	case <-p.ctx.Done():
		return promptReply{err: context.Cause(p.ctx)}
	}
	select {
	case reply = <-request.reply:
		// Cancellation wins before a private reply transfers to its caller.
		if err := canceled(); err != nil {
			clear(reply.secret)
			return promptReply{err: err}
		}
		return reply
	case <-request.ctx.Done():
		return promptReply{err: context.Cause(request.ctx)}
	case <-p.ctx.Done():
		return promptReply{err: context.Cause(p.ctx)}
	}
}

// promptModel holds only an active form; secrets never enter the timer Model.
type promptModel struct {
	caller                      context.Context
	kind, title, body, text     string
	choices                     []terminal.Choice
	secret                      []byte
	columns, rows               int
	selected, scroll            int
	affirmative, reviewed, done bool
}

func newPromptModel(request promptRequest, columns, rows int) *promptModel {
	m := &promptModel{caller: request.ctx, kind: request.kind, title: request.title, body: request.body, choices: append([]terminal.Choice(nil), request.choices...), columns: columns, rows: rows}
	if request.kind == "secret" {
		m.secret = make([]byte, 0, 16384)
	} else if request.kind == "text" {
		m.text = sanitize(request.defaultText)
	}
	return m
}
func (m *promptModel) Resize(columns, rows int) { m.columns, m.rows = columns, rows }
func (m *promptModel) matches() []terminal.Choice {
	query := strings.ToLower(m.text)
	choices := make([]terminal.Choice, 0, len(m.choices))
	for _, choice := range m.choices {
		if strings.Contains(strings.ToLower(sanitize(choice.Label)+" "+sanitize(choice.ID)), query) {
			choices = append(choices, choice)
		}
	}
	return choices
}
func (m *promptModel) Handle(event terminal.Event) (promptReply, bool) {
	if m.done {
		return promptReply{}, false
	}
	finish := func(reply promptReply) (promptReply, bool) { m.done = true; return reply, true }
	if m.caller != nil && m.caller.Err() != nil {
		m.Close()
		return finish(promptReply{err: context.Cause(m.caller)})
	}
	if event.Kind == "escape" {
		m.Close()
		return finish(promptReply{err: &terminal.ExitError{Code: 0}})
	}
	if event.Kind == "resize" {
		m.Resize(event.Columns, event.Rows)
		return promptReply{}, false
	}
	if m.kind == "view" {
		switch event.Kind {
		case "up":
			m.scroll = max(0, m.scroll-1)
		case "down":
			m.scroll = min(m.warningScrollLimit(), m.scroll+1)
		case "enter":
			if m.columns >= 40 && m.rows >= 8 {
				return finish(promptReply{})
			}
		}
		return promptReply{}, false
	}
	if m.kind == "confirm" {
		switch event.Kind {
		case "text":
			if strings.EqualFold(event.Text, "y") {
				m.affirmative = true
			}
			if strings.EqualFold(event.Text, "n") {
				m.affirmative = false
			}
		case "up":
			m.scroll = max(0, m.scroll-1)
		case "down":
			m.scroll = min(m.warningScrollLimit(), m.scroll+1)
		case "enter":
			if m.columns >= 40 && m.rows >= 8 && (!m.affirmative || m.reviewed) {
				return finish(promptReply{confirmed: m.affirmative})
			}
		}
		return promptReply{}, false
	}
	switch event.Kind {
	case "text", "paste":
		text := sanitize(event.Text)
		limit, length := 16384, len(m.text)
		if m.kind == "choose" {
			limit = 256
		}
		if m.kind == "secret" {
			length = len(m.secret)
		}
		if length+len(text) > limit {
			m.Close()
			return finish(promptReply{err: &terminal.ExitError{Code: 1}})
		}
		if m.kind == "secret" {
			m.secret = append(m.secret, text...)
		} else {
			m.text += text
		}
		m.selected = 0
	case "backspace":
		if m.kind == "secret" {
			end := lastClusterStart(m.secret)
			clear(m.secret[end:])
			m.secret = m.secret[:end]
		} else {
			m.text = m.text[:lastClusterStart([]byte(m.text))]
		}
		m.selected = 0
	case "up":
		m.selected = max(0, m.selected-1)
	case "down":
		m.selected = min(max(0, len(m.matches())-1), m.selected+1)
	case "enter":
		if m.columns < 40 || m.rows < 8 {
			return promptReply{}, false
		}
		switch m.kind {
		case "choose":
			choices := m.matches()
			if len(choices) > 0 {
				return finish(promptReply{choiceID: choices[min(m.selected, len(choices)-1)].ID})
			}
		case "text":
			if len(m.text) <= 16384 {
				return finish(promptReply{text: m.text})
			}
		case "secret":
			return finish(promptReply{secret: append([]byte(nil), m.secret...)})
		}
	}
	return promptReply{}, false
}

// Work on byte clusters for secret backspace; no secret string is retained or
// passed to the ordinary renderer or styling boundary.
func lastClusterStart(data []byte) int {
	start, consumed, state := 0, 0, -1
	for len(data) > 0 {
		start = consumed
		cluster, rest, _, next := uniseg.FirstGraphemeCluster(data, state)
		consumed += len(cluster)
		data, state = rest, next
	}
	return start
}
func (m *promptModel) warningLines() []string {
	if m.kind == "view" {
		// Strip escape strings across line breaks before splitting trusted rows.
		var lines []string
		for _, paragraph := range strings.Split(sanitizeText(m.body, true), "\n") {
			lines = append(lines, wrapWords(paragraph, max(0, m.columns-1))...)
		}
		return lines
	}
	return wrapWords(sanitize(strings.ReplaceAll(m.title, "\n", " ")), max(0, m.columns-1))
}
func (m *promptModel) warningScrollLimit() int { return max(0, len(m.warningLines())-max(1, m.rows-3)) }
func (m *promptModel) Render(styler terminal.Styler) []string {
	if m.rows <= 0 {
		return nil
	}
	width := max(0, m.columns-1)
	if width == 0 {
		return []string{""}
	}
	lines := make([]string, 0, min(m.rows, 32))
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
		add(terminal.RoleKey, "Esc Cancel")
		return lines
	}
	if m.kind == "confirm" || m.kind == "view" {
		title := "Review scoped change"
		if m.kind == "view" {
			title = m.title
		}
		add(terminal.RoleAccent, title)
		warning := m.warningLines()
		capacity := max(1, m.rows-3)
		m.scroll = min(m.scroll, max(0, len(warning)-capacity))
		end := min(len(warning), m.scroll+capacity)
		for _, line := range warning[m.scroll:end] {
			add(terminal.RoleText, line)
		}
		if m.kind == "view" {
			add(terminal.RoleMuted, "Read-only information")
			add(terminal.RoleKey, "↑/↓ Read  Enter Back  Escape Back")
			return lines
		}
		if end == len(warning) {
			m.reviewed = true
		}
		selected := "No"
		if m.affirmative {
			selected = "Yes"
		}
		if !m.reviewed {
			selected += " · Read full warning before Yes"
		}
		add(terminal.RoleWarning, "Selected: "+selected)
		add(terminal.RoleKey, "↑/↓ Read  y/n Choose  Enter Submit  Esc Cancel")
		return lines
	}
	add(terminal.RoleAccent, m.title)
	switch m.kind {
	case "choose":
		add(terminal.RoleMuted, "Search: "+m.text)
		choices := m.matches()
		capacity := max(1, m.rows-3)
		start := max(0, m.selected-capacity+1)
		for i := start; i < min(len(choices), start+capacity); i++ {
			role, prefix := terminal.RoleText, "  "
			if i == m.selected {
				role, prefix = terminal.RoleSelection, "> "
			}
			add(role, prefix+choices[i].Label+" ["+choices[i].ID+"]")
		}
		if len(choices) == 0 {
			add(terminal.RoleInfo, "No matches")
		}
	case "text":
		add(terminal.RoleText, m.text)
	case "secret":
		add(terminal.RoleMuted, strings.Repeat("*", min(len(m.secret), width)))
	}
	add(terminal.RoleKey, "Enter Submit  Esc Cancel")
	return lines
}
func (m *promptModel) Close() {
	clear(m.secret[:cap(m.secret)])
	m.secret = nil
	m.text = ""
	m.done = true
}

// Wrap complete words where possible; split long locators only at grapheme
// boundaries. Preserve the full warning instead of clipping it.
func wrapWords(text string, cells int) []string {
	if cells <= 0 {
		return []string{""}
	}
	lines := []string{}
	line := ""
	for _, word := range strings.Fields(text) {
		if line != "" && uniseg.StringWidth(line)+1+uniseg.StringWidth(word) <= cells {
			line += " " + word
			continue
		}
		if line != "" {
			lines = append(lines, line)
			line = ""
		}
		state, used := -1, 0
		for len(word) > 0 {
			cluster, rest, width, next := uniseg.FirstGraphemeClusterInString(word, state)
			if used+width > cells && line != "" {
				lines = append(lines, line)
				line = ""
				used = 0
			}
			if width <= cells && (width > 0 || used > 0) {
				line += cluster
				used += width
			}
			word, state = rest, next
		}
	}
	if line != "" || len(lines) == 0 {
		lines = append(lines, line)
	}
	return lines
}
