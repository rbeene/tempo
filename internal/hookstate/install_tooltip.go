package hookstate

import (
	"bytes"
	"encoding/json"
	"github.com/pelletier/go-toml/v2"
	"strings"
	"unicode/utf8"
)

type tooltipOwnership struct {
	Before   string `json:"before"`
	Inserted string `json:"inserted"`
	NewFile  bool   `json:"new_file"`
}
type tooltipField struct {
	start, end, lineStart, lineEnd, insert, tableEnd int
	value                                            string
	table                                            bool
	newline                                          string
}

// This deliberately edits only canonical table syntax. It rejects ambiguous
// representations instead of normalizing or serializing a user's TOML file.
func inspectTooltip(b []byte) (tooltipField, error) {
	f := tooltipField{start: -1, end: -1, insert: len(b), tableEnd: len(b), newline: "\n"}
	if !utf8.Valid(b) || bytes.IndexByte(b, 0) >= 0 || bytes.Contains(b, []byte(`"""`)) || bytes.Contains(b, []byte(`'''`)) {
		return f, problem("validation")
	}
	var document map[string]any
	if toml.Unmarshal(b, &document) != nil {
		return f, problem("validation")
	}
	if tui, ok := document["tui"].(map[string]any); ok {
		if value, present := tui["show_tooltips"]; present {
			if _, ok := value.(bool); !ok {
				return f, problem("validation")
			}
		}
	}
	if bytes.Contains(b, []byte("\r\n")) {
		f.newline = "\r\n"
	}
	table := ""
	seenTable := map[string]bool{}
	offset := 0
	for _, line := range strings.SplitAfter(string(b), "\n") {
		raw := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		uncommented, valid := tomlWithoutComment(raw)
		if !valid {
			return f, problem("validation")
		}
		clean := strings.TrimSpace(uncommented)
		if clean == "" {
			offset += len(line)
			continue
		}
		if strings.HasPrefix(clean, "[") {
			name, valid := tomlTableName(clean)
			if !valid || seenTable[name] {
				return f, problem("validation")
			}
			if table == "tui" {
				f.tableEnd = offset
			}
			table = name
			if table == "tui" && clean != "[tui]" {
				return f, problem("validation")
			}
			seenTable[table] = true
			if table == "tui" {
				f.table = true
				f.insert = offset + len(line)
			}
			offset += len(line)
			continue
		}
		key, value, ok := strings.Cut(clean, "=")
		if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return f, problem("validation")
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if strings.Contains(key, "tui") && table == "" {
			return f, problem("validation")
		}
		if table == "tui" && (strings.Trim(key, `"'`) == "show_tooltips" || strings.Contains(key, "show_tooltips")) {
			if key != "show_tooltips" || f.start >= 0 || value != "true" && value != "false" {
				return f, problem("validation")
			}
			eq := strings.Index(raw, "=")
			valueOffset := eq + 1
			for valueOffset < len(raw) && (raw[valueOffset] == ' ' || raw[valueOffset] == '\t') {
				valueOffset++
			}
			f.start = offset + valueOffset
			f.end = f.start + len(value)
			f.lineStart = offset
			f.lineEnd = offset + len(line)
			f.value = value
		}
		offset += len(line)
	}
	return f, nil
}
func tomlWithoutComment(line string) (string, bool) {
	quote := byte(0)
	escaped := false
	for i := 0; i < len(line); i++ {
		c := line[i]
		if quote != 0 {
			if quote == '"' && c == '\\' && !escaped {
				escaped = true
				continue
			}
			if c == quote && !escaped {
				quote = 0
			}
			escaped = false
			continue
		}
		if c == '"' || c == '\'' {
			quote = c
		} else if c == '#' {
			return line[:i], true
		}
	}
	return line, quote == 0
}
func editTooltip(file previewFile, old *tooltipOwnership, uninstall bool) ([]byte, *tooltipOwnership, error) {
	b := file.Bytes
	f, err := inspectTooltip(b)
	if err != nil {
		return nil, nil, err
	}
	if old != nil {
		if f.value != "false" {
			return nil, nil, problem("revision_conflict")
		}
		if !uninstall {
			return b, old, nil
		}
		if old.Before == "true" {
			return splice(b, f.start, f.end, []byte("true")), nil, nil
		}
		if old.Inserted == "" || bytes.Count(b, []byte(old.Inserted)) != 1 {
			return nil, nil, problem("revision_conflict")
		}
		at := bytes.Index(b, []byte(old.Inserted))
		// A newly owned table becomes a foreign container when the user adds
		// siblings. Retain its header so those values keep their TOML scope.
		if strings.Contains(old.Inserted, "[tui]") {
			body := splice(b[f.insert:f.tableEnd], f.lineStart-f.insert, f.lineEnd-f.insert, nil)
			if len(bytes.TrimSpace(body)) > 0 {
				return splice(b, f.lineStart, f.lineEnd, nil), nil, nil
			}
		}
		after := splice(b, at, at+len(old.Inserted), nil)
		if old.NewFile && len(after) == 0 {
			return nil, nil, nil
		}
		return after, nil, nil
	}
	if uninstall || f.value == "false" {
		return b, nil, nil
	}
	own := &tooltipOwnership{Before: f.value, NewFile: !file.Exists}
	if f.value == "true" {
		return splice(b, f.start, f.end, []byte("false")), own, nil
	}
	insertion := "show_tooltips = false" + f.newline
	if !f.table {
		if len(b) > 0 && b[len(b)-1] != '\n' {
			insertion = f.newline + insertion
		}
		prefix := "[tui]" + f.newline
		if strings.HasPrefix(insertion, f.newline) {
			insertion = f.newline + prefix + strings.TrimPrefix(insertion, f.newline)
		} else {
			insertion = prefix + insertion
		}
	} else if f.insert > 0 && b[f.insert-1] != '\n' {
		insertion = f.newline + insertion
	}
	own.Inserted = insertion
	return splice(b, f.insert, f.insert, []byte(insertion)), own, nil
}

func tomlTableName(header string) (string, bool) {
	if len(header) < 3 || header[0] != '[' || header[len(header)-1] != ']' || strings.HasPrefix(header, "[[") {
		return "", false
	}
	inner := strings.TrimSpace(header[1 : len(header)-1])
	parts := []string{}
	for len(inner) > 0 {
		var token string
		if inner[0] == '"' || inner[0] == '\'' {
			quote := inner[0]
			end := 1
			escaped := false
			for end < len(inner) {
				if quote == '"' && inner[end] == '\\' && !escaped {
					escaped = true
					end++
					continue
				}
				if inner[end] == quote && !escaped {
					break
				}
				escaped = false
				end++
			}
			if end >= len(inner) {
				return "", false
			}
			raw := inner[:end+1]
			if quote == '"' {
				if json.Unmarshal([]byte(raw), &token) != nil {
					return "", false
				}
			} else {
				token = raw[1 : len(raw)-1]
			}
			inner = strings.TrimSpace(inner[end+1:])
		} else {
			end := 0
			for end < len(inner) && (inner[end] >= 'a' && inner[end] <= 'z' || inner[end] >= 'A' && inner[end] <= 'Z' || inner[end] >= '0' && inner[end] <= '9' || inner[end] == '_' || inner[end] == '-') {
				end++
			}
			if end == 0 {
				return "", false
			}
			token = inner[:end]
			inner = strings.TrimSpace(inner[end:])
		}
		parts = append(parts, token)
		if inner == "" {
			break
		}
		if inner[0] != '.' {
			return "", false
		}
		inner = strings.TrimSpace(inner[1:])
		if inner == "" {
			return "", false
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, "."), true
}
