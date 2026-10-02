package hookstate

import (
	"bytes"
	"encoding/json"
	"strings"
)

func nativeEvents(host string) []string {
	common := []string{"SessionStart", "SessionEnd", "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop", "PreToolUse", "PostToolUse"}
	if host == "codex" {
		return append(common, "Interrupt", "PermissionRequest", "PreCompact", "PostCompact")
	}
	return append(common, "PostToolUseFailure", "PermissionRequest", "StopFailure", "TaskCreated", "TaskCompleted")
}

// These offsets describe validated JSON. Edits splice only owned array elements
// or newly inserted fields; foreign tokens and their number spelling stay intact.
type jsonSpan struct {
	start, end int
	fields     map[string]*jsonSpan
	elements   []*jsonSpan
}

func jsonTree(b []byte) *jsonSpan {
	i := 0
	space := func() {
		for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == '\r' || b[i] == '\t') {
			i++
		}
	}
	var parse func() *jsonSpan
	var quoted func()
	quoted = func() {
		i++
		for i < len(b) {
			if b[i] == '\\' {
				i += 2
				continue
			}
			if b[i] == '"' {
				i++
				return
			}
			i++
		}
	}
	parse = func() *jsonSpan {
		space()
		n := &jsonSpan{start: i}
		switch b[i] {
		case '{':
			i++
			n.fields = map[string]*jsonSpan{}
			space()
			for b[i] != '}' {
				start := i
				quoted()
				var key string
				_ = json.Unmarshal(b[start:i], &key)
				space()
				i++
				n.fields[key] = parse()
				space()
				if b[i] == ',' {
					i++
					space()
				} else {
					break
				}
			}
			i++
		case '[':
			i++
			space()
			for b[i] != ']' {
				n.elements = append(n.elements, parse())
				space()
				if b[i] == ',' {
					i++
					space()
				} else {
					break
				}
			}
			i++
		case '"':
			quoted()
		default:
			for i < len(b) && !strings.ContainsRune(",]} \n\r\t", rune(b[i])) {
				i++
			}
		}
		n.end = i
		return n
	}
	return parse()
}
func splice(b []byte, start, end int, value []byte) []byte {
	out := make([]byte, 0, len(b)+len(value))
	out = append(out, b[:start]...)
	out = append(out, value...)
	return append(out, b[end:]...)
}
func addField(b []byte, object *jsonSpan, key string, value []byte) []byte {
	k, _ := json.Marshal(key)
	prefix := ""
	if len(object.fields) > 0 {
		prefix = ","
	}
	field := append([]byte(prefix), k...)
	field = append(field, ':')
	field = append(field, value...)
	return splice(b, object.end-1, object.end-1, field)
}
func quoteCommand(path string) string { return "'" + strings.ReplaceAll(path, "'", "'\"'\"'") + "'" }
func nativeGroup(host, executable string) string {
	b, _ := json.Marshal(struct {
		Hooks []struct {
			Type    string `json:"type"`
			Command string `json:"command"`
			Timeout int    `json:"timeout"`
		} `json:"hooks"`
	}{Hooks: []struct {
		Type    string `json:"type"`
		Command string `json:"command"`
		Timeout int    `json:"timeout"`
	}{{"command", quoteCommand(executable) + " hook " + host + " --input-stdin", 2}}})
	return string(b)
}
func ownedJSON(host string, b []byte, old, next string) ([]byte, error) {
	if len(b) == 0 {
		b = []byte("{}")
	}
	if err := validateHookDocument(b); err != nil {
		return nil, err
	}
	for _, event := range nativeEvents(host) {
		root := jsonTree(b)
		hooks := root.fields["hooks"]
		if hooks == nil {
			if old != "" {
				return nil, problem("revision_conflict")
			}
			if next == "" {
				continue
			}
			b = addField(b, root, "hooks", []byte("{}"))
			hooks = jsonTree(b).fields["hooks"]
		}
		array := hooks.fields[event]
		if array == nil {
			if old != "" {
				return nil, problem("revision_conflict")
			}
			if next != "" {
				b = addField(b, hooks, event, []byte("["+next+"]"))
			}
			continue
		}
		matches := []int{}
		for i, group := range array.elements {
			// Matching bytes prove our exact owned span, but differently
			// formatted copies of the same command are still ambiguous.
			var candidate struct {
				Hooks []struct {
					Command string `json:"command"`
				} `json:"hooks"`
			}
			var expected struct {
				Hooks []struct {
					Command string `json:"command"`
				} `json:"hooks"`
			}
			reference := next
			if reference == "" {
				reference = old
			}
			_ = json.Unmarshal(b[group.start:group.end], &candidate)
			_ = json.Unmarshal([]byte(reference), &expected)
			if len(expected.Hooks) == 1 {
				for _, handler := range candidate.Hooks {
					if handler.Command == expected.Hooks[0].Command && (old == "" || string(b[group.start:group.end]) != old) {
						return nil, problem("revision_conflict")
					}
				}
			}
			if string(b[group.start:group.end]) == old && old != "" {
				matches = append(matches, i)
			}
			if old == "" && next != "" && bytes.Equal(b[group.start:group.end], []byte(next)) {
				return nil, problem("revision_conflict")
			}
		}
		if old != "" {
			if len(matches) != 1 {
				return nil, problem("revision_conflict")
			}
			idx := matches[0]
			entry := array.elements[idx]
			if next != "" {
				b = splice(b, entry.start, entry.end, []byte(next))
			} else {
				start, end := entry.start, entry.end
				if idx+1 < len(array.elements) {
					end = array.elements[idx+1].start
				} else if idx > 0 {
					start = array.elements[idx-1].end
				}
				b = splice(b, start, end, nil)
			}
		} else if next != "" {
			prefix := ""
			if len(array.elements) > 0 {
				prefix = ","
			}
			b = splice(b, array.end-1, array.end-1, []byte(prefix+next))
		}
	}
	return b, nil
}
