package worker

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// This parser reads the published plist data model, independently of the encoder.
func qaWorkerPlistValue(d *xml.Decoder, start xml.StartElement) (any, error) {
	switch start.Name.Local {
	case "string", "key", "integer":
		var text string
		if err := d.DecodeElement(&text, &start); err != nil {
			return nil, err
		}
		return text, nil
	case "true", "false":
		if err := d.Skip(); err != nil {
			return nil, err
		}
		return start.Name.Local == "true", nil
	case "array", "dict", "plist":
		values := []any{}
		for {
			tok, err := d.Token()
			if err != nil {
				return nil, err
			}
			switch v := tok.(type) {
			case xml.StartElement:
				value, err := qaWorkerPlistValue(d, v)
				if err != nil {
					return nil, err
				}
				values = append(values, value)
			case xml.EndElement:
				if v.Name != start.Name {
					return nil, fmt.Errorf("mismatched plist close")
				}
				if start.Name.Local == "array" {
					return values, nil
				}
				if start.Name.Local == "plist" {
					if len(values) != 1 {
						return nil, fmt.Errorf("plist root count")
					}
					return values[0], nil
				}
				if len(values)%2 != 0 {
					return nil, fmt.Errorf("odd dictionary")
				}
				m := map[string]any{}
				for i := 0; i < len(values); i += 2 {
					k, ok := values[i].(string)
					if !ok {
						return nil, fmt.Errorf("nonstring key")
					}
					if _, exists := m[k]; exists {
						return nil, fmt.Errorf("duplicate key")
					}
					m[k] = values[i+1]
				}
				return m, nil
			}
		}
	}
	return nil, fmt.Errorf("unsupported plist element %s", start.Name.Local)
}
func qaWorkerParsePlist(t *testing.T, b []byte) map[string]any {
	t.Helper()
	d := xml.NewDecoder(bytes.NewReader(b))
	for {
		tok, err := d.Token()
		if err != nil {
			t.Fatal(err)
		}
		if start, ok := tok.(xml.StartElement); ok {
			v, err := qaWorkerPlistValue(d, start)
			if err != nil {
				t.Fatal(err)
			}
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatal("plist not dictionary")
			}
			for {
				tok, err = d.Token()
				if err == io.EOF {
					return m
				}
				if err != nil {
					t.Fatal(err)
				}
				if c, ok := tok.(xml.CharData); !ok || strings.TrimSpace(string(c)) != "" {
					t.Fatal("extra plist content")
				}
			}
		}
	}
}

// Independently interpret the limited, quoted systemd command/environment grammar
// required by this contract. A lone % would trigger a systemd specifier expansion.
func qaWorkerUnitTokens(t *testing.T, text string) []string {
	t.Helper()
	out := []string{}
	for strings.TrimSpace(text) != "" {
		text = strings.TrimSpace(text)
		if text[0] != '"' {
			word, rest, _ := strings.Cut(text, " ")
			if word != "worker" && word != "run" {
				t.Fatalf("dynamic service token not wholly quoted: %q", text)
			}
			out = append(out, word)
			text = rest
			continue
		}
		i := 1
		for ; i < len(text); i++ {
			if text[i] == '\\' {
				i++
				continue
			}
			if text[i] == '"' {
				break
			}
		}
		if i >= len(text) {
			t.Fatalf("unterminated service token: %q", text)
		}
		value, err := strconv.Unquote(text[:i+1])
		if err != nil {
			t.Fatal(err)
		}
		var decoded strings.Builder
		for j := 0; j < len(value); j++ {
			if value[j] == '%' {
				if j+1 >= len(value) || value[j+1] != '%' {
					t.Fatalf("unescaped systemd specifier %q", value)
				}
				j++
			}
			decoded.WriteByte(value[j])
		}
		out = append(out, decoded.String())
		text = text[i+1:]
	}
	return out
}
func qaWorkerCheckDefinition(t *testing.T, s *Service) {
	t.Helper()
	d := s.definition()
	o := s.options
	wantArgs := []any{o.Executable, "worker", "run"}
	wantEnv := map[string]any{"TEMPO_STATE": o.StatePath, "TEMPO_CONFIG": o.ConfigPath, "TEMPO_WORKER_MODE": "managed"}
	if o.Platform == "darwin" {
		m := qaWorkerParsePlist(t, d.Bytes)
		if !reflect.DeepEqual(m["ProgramArguments"], wantArgs) || !reflect.DeepEqual(m["EnvironmentVariables"], wantEnv) {
			t.Fatalf("plist changed literal argv/environment: %+v", m)
		}
		if m["Label"] != d.ServiceID || m["Disabled"] != true || m["KeepAlive"] != true || m["ExitTimeOut"] != "35" || m["ThrottleInterval"] != "10" {
			t.Fatalf("wrong lifecycle plist: %+v", m)
		}
		for _, k := range []string{"StandardInPath", "StandardOutPath", "StandardErrorPath"} {
			if m[k] != "/dev/null" {
				t.Fatalf("unbounded service stream %s=%v", k, m[k])
			}
		}
		if filepath.Ext(d.Path) != ".plist" {
			t.Fatalf("wrong plist suffix %q", d.Path)
		}
	} else {
		if filepath.Ext(d.Path) != ".service" {
			t.Fatalf("Linux definition is not a unit: %q", d.Path)
		}
		fields := map[string]string{}
		section := ""
		for _, line := range strings.Split(string(d.Bytes), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			if strings.HasPrefix(line, "[") {
				section = line
				continue
			}
			key, value, ok := strings.Cut(line, "=")
			if !ok {
				t.Fatalf("invalid unit line %q", line)
			}
			key = section + key
			if _, exists := fields[key]; exists {
				if key == "[Service]Environment" {
					fields[key] += " " + value
					continue
				}
				t.Fatalf("duplicate field %s", key)
			}
			fields[key] = value
		}
		argv := qaWorkerUnitTokens(t, fields["[Service]ExecStart"])
		if len(argv) != 3 || argv[0] != ":"+o.Executable || argv[1] != "worker" || argv[2] != "run" {
			t.Fatalf("ExecStart changed argv or permits environment expansion: %+v", argv)
		}
		env := map[string]any{}
		for _, token := range qaWorkerUnitTokens(t, fields["[Service]Environment"]) {
			k, v, ok := strings.Cut(token, "=")
			if !ok {
				t.Fatal("invalid environment assignment")
			}
			if _, exists := env[k]; exists {
				t.Fatalf("duplicate environment %s", k)
			}
			env[k] = v
		}
		if !reflect.DeepEqual(env, wantEnv) {
			t.Fatalf("unit changed literal environment: %+v", env)
		}
		for k, want := range map[string]string{"[Service]Type": "exec", "[Service]Restart": "on-failure", "[Service]RestartSec": "10s", "[Service]TimeoutStopSec": "35s", "[Service]StandardInput": "null", "[Service]StandardOutput": "null", "[Service]StandardError": "null", "[Install]WantedBy": "default.target"} {
			if fields[k] != want && !((k == "[Service]RestartSec" || k == "[Service]TimeoutStopSec") && fields[k] == strings.TrimSuffix(want, "s")) {
				t.Fatalf("unit %s=%q want %q", k, fields[k], want)
			}
		}
		for _, k := range []string{"[Service]EnvironmentFile", "[Service]ExecStop", "[Service]RuntimeMaxSec", "[Install]Alias", "[Install]Also"} {
			if _, ok := fields[k]; ok {
				t.Fatalf("unexpected service effect %s", k)
			}
		}
	}
	if bytes.Contains(d.Bytes, []byte("HARVEST_TOKEN")) || bytes.Contains(d.Bytes, []byte("CODEX_HOME")) {
		t.Fatal("service serialized unrelated environment")
	}
}
func TestQAWorkerServiceDefinitionPreservesLiteralPaths(t *testing.T) {
	for _, platform := range []string{"darwin", "linux"} {
		t.Run(platform, func(t *testing.T) {
			for _, name := range []string{"spaces and 雪", `quote"single'amp&angle<>`, `literal$HOME${X}%h%%back\slash`, `trailing\`} {
				t.Run(name, func(t *testing.T) {
					o, _ := qaWorkerOptions(t)
					o.Platform = platform
					base := filepath.Join(filepath.Dir(o.Executable), name)
					o.Executable = filepath.Join(base, "tempo")
					o.StatePath = filepath.Join(base, "state.json")
					o.ConfigPath = filepath.Join(base, "config.json")
					o.ServiceDir = filepath.Join(base, "services")
					s := qaWorkerNew(t, o)
					qaWorkerCheckDefinition(t, s)
					if _, err := os.Stat(base); !os.IsNotExist(err) {
						t.Fatalf("pure generation wrote path: %v", err)
					}
				})
			}
		})
	}
}
func TestQAWorkerServiceDefinitionPublishedPlistMatchesParsedIntent(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	base := filepath.Join(filepath.Dir(o.Executable), `path 雪 "$HOME" %h & \`)
	o.Executable = filepath.Join(base, "tempo")
	o.StatePath = filepath.Join(base, "state.json")
	o.ConfigPath = filepath.Join(base, "config.json")
	o.ServiceDir = filepath.Join(base, "services")
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(601)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	if _, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(601), Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	record := qaWorkerReadControl(t, s.options)
	b, err := os.ReadFile(record.Owned.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, s.definition().Bytes) {
		t.Fatal("published bytes differ from intended definition")
	}
	qaWorkerCheckDefinition(t, s)
}
func TestQAWorkerServicePathsRejectControlsBeforeEffects(t *testing.T) {
	for _, field := range []string{"executable", "state", "config", "services"} {
		for _, bad := range []string{"line\nbreak", "carriage\rreturn", "tab\there", "nul\x00here", "del\x7fhere", "invalid\xffutf8"} {
			t.Run(field+"/"+strconv.Quote(bad), func(t *testing.T) {
				o, _ := qaWorkerOptions(t)
				path := filepath.Join(filepath.Dir(o.Executable), bad)
				switch field {
				case "executable":
					o.Executable = path
				case "state":
					o.StatePath = path
				case "config":
					o.ConfigPath = path
				case "services":
					o.ServiceDir = path
				}
				_, err := New(o)
				qaWorkerCode(t, err, "validation")
				entries, err := os.ReadDir(filepath.Dir(o.Executable))
				if field == "executable" {
					entries, err = os.ReadDir(filepath.Dir(path))
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 1 || entries[0].Name() != "tempo" {
					t.Fatalf("invalid constructor wrote fixture directory: %v", entries)
				}
			})
		}
	}
}

func TestQAWorkerOversizedDefinitionRejectedBeforeReservationOrManager(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	o.Executable = "/" + strings.Repeat("x", 70000)
	s, err := New(o)
	if err == nil {
		_, err = s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(602), Confirmed: true})
	}
	qaWorkerCode(t, err, "validation")
	qaWorkerAbsent(t, o)
}
