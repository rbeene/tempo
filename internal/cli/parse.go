package cli

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rbeene/tempo/internal/identity"
)

type parsed struct {
	command Command
	flags   map[string]string
	args    []string
}

func parse(args []string) (parsed, error) {
	p := parsed{flags: map[string]string{}}
	if len(args) == 0 {
		args = []string{"help"}
	}
	if len(args) == 1 && args[0] == "--version" {
		args = []string{"version"}
	}
	all := optionTypes()
	help := false
	words := []string{}
	for i := 0; i < len(args); i++ {
		s := args[i]
		if s == "--help" || s == "-h" {
			help = true
			continue
		}
		if !strings.HasPrefix(s, "--") {
			if strings.HasPrefix(s, "-") {
				return p, problem("usage", "unknown option")
			}
			words = append(words, s)
			continue
		}
		k, v, eq := strings.Cut(strings.TrimPrefix(s, "--"), "=")
		kind, ok := all[k]
		if !ok {
			return p, problem("usage", "unknown option; use tempo help")
		}
		if _, ok := p.flags[k]; ok {
			return p, problem("usage", "duplicate option --"+k)
		}
		if kind == "bool" {
			if eq {
				return p, problem("usage", "--"+k+" does not take a value")
			}
			v = "true"
		} else if !eq {
			i++
			if i >= len(args) {
				return p, problem("usage", "missing value for --"+k)
			}
			v = args[i]
		}
		p.flags[k] = v
	}
	if help {
		for _, c := range commands {
			if c.Name == "help" {
				p.command = c
				break
			}
		}
		return p, nil
	}
	if len(words) == 0 {
		return p, problem("usage", "command required")
	}
	name := words[0]
	n := 1
	if name != "help" && name != "schema" && name != "version" && name != "link" && name != "setup" && name != "doctor" {
		if len(words) < 2 {
			return p, problem("usage", "subcommand required; use tempo help")
		}
		name += " " + words[1]
		n = 2
	}
	found := false
	for _, c := range commands {
		if c.Name == name {
			p.command = c
			found = true
			break
		}
	}
	if !found {
		return p, problem("usage", "unknown command; use tempo help")
	}
	p.args = words[n:]
	for k := range p.flags {
		if _, ok := globalFlags[k]; !ok {
			if _, ok := p.command.Flags[k]; !ok {
				return p, problem("usage", "option --"+k+" is not valid for this command")
			}
		}
	}
	if p.command.Positionals == "" && len(p.args) != 0 || (p.command.Positionals == "ID" || p.command.Positionals == "UUID") && len(p.args) != 1 || (p.command.Positionals == "[ID]" || p.command.Positionals == "[UUID]") && len(p.args) > 1 {
		return p, problem("usage", "invalid arguments for "+name)
	}
	for _, id := range p.args {
		if strings.Contains(p.command.Positionals, "UUID") {
			if !uuidPattern.MatchString(id) {
				return p, problem("validation", "ID must be a canonical UUID")
			}
			continue
		}
		if !validID(id) {
			return p, problem("validation", "ID must be a positive integer")
		}
	}
	for k, v := range p.flags {
		kind := all[k]
		if kind == "uuid" && !uuidPattern.MatchString(v) {
			return p, problem("validation", "--"+k+" must be a canonical UUID")
		}
		if kind == "counter" {
			if _, e := strconv.ParseUint(v, 10, 64); e != nil || v == "" || (len(v) > 1 && v[0] == '0') || strings.Trim(v, "0123456789") != "" {
				return p, problem("validation", "--"+k+" must be a canonical counter")
			}
		}
		if kind == "id" && !validID(v) {
			return p, problem("validation", "--"+k+" must be a positive integer")
		}
		if kind == "boolean" && v != "true" && v != "false" {
			return p, problem("validation", "--"+k+" must be true or false")
		}
	}
	return p, nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func validID(s string) bool { return identity.Valid(s) }

func date(s string, now time.Time) (string, error) {
	switch s {
	case "today":
		return now.Format("2006-01-02"), nil
	case "yesterday":
		return now.AddDate(0, 0, -1).Format("2006-01-02"), nil
	}
	d, e := time.Parse("2006-01-02", s)
	if e != nil || d.Format("2006-01-02") != s {
		return "", problem("validation", "date must be YYYY-MM-DD, today or yesterday")
	}
	return s, nil
}
func hours(s string) (float64, error) {
	var h float64
	var e error
	if strings.Contains(s, ":") {
		parts := strings.Split(s, ":")
		if len(parts) != 2 || len(parts[1]) != 2 || strings.Trim(parts[0]+parts[1], "0123456789") != "" {
			return 0, problem("validation", "duration must be decimal hours, H:MM, or 1h30m")
		}
		var whole, mins int
		whole, e = strconv.Atoi(parts[0])
		if e == nil {
			mins, e = strconv.Atoi(parts[1])
		}
		if mins >= 60 {
			e = fmt.Errorf("minutes")
		}
		h = float64(whole) + float64(mins)/60
	} else {
		h, e = strconv.ParseFloat(s, 64)
		if e != nil {
			var d time.Duration
			d, e = time.ParseDuration(s)
			h = d.Hours()
		}
	}
	if e != nil || math.IsNaN(h) || math.IsInf(h, 0) || h < 0 || h > 24 {
		return 0, problem("validation", "duration must be finite and between 0 and 24 hours")
	}
	return h, nil
}
func clockTime(s string) (string, error) {
	t, e := time.Parse("15:04", s)
	if e != nil || t.Format("15:04") != s {
		return "", problem("validation", "time must use 24-hour HH:MM")
	}
	return t.Format("3:04pm"), nil
}

func optionTypes() map[string]string {
	all := map[string]string{}
	for k, v := range globalFlags {
		all[k] = v
	}
	for _, c := range commands {
		for k, v := range c.Flags {
			all[k] = v
		}
	}
	return all
}

// Discover the error-output mode without treating consumed flag values as flags.
func wantsJSON(args []string) bool {
	kinds := optionTypes()
	for i := 0; i < len(args); i++ {
		if args[i] == "--json" {
			return true
		}
		k, _, eq := strings.Cut(strings.TrimPrefix(args[i], "--"), "=")
		if kind, ok := kinds[k]; strings.HasPrefix(args[i], "--") && ok && kind != "bool" && !eq {
			i++
		}
	}
	return false
}

// Determine forced local output even when parsing later fails, while treating
// option values (including Harvest notes containing "activity") as literals.
func wantsLocalJSON(args []string, redirected bool) bool {
	options := optionTypes()
	first := ""
	forced := false
	for i := 0; i < len(args); i++ {
		token := args[i]
		if strings.HasPrefix(token, "--") {
			key, _, eq := strings.Cut(strings.TrimPrefix(token, "--"), "=")
			if key == "non-interactive" && !eq {
				forced = true
			}
			if kind, ok := options[key]; ok && kind != "bool" && !eq {
				i++
			}
			continue
		}
		if strings.HasPrefix(token, "-") {
			continue
		}
		if first == "" {
			first = token
		}
	}
	return (first == "activity" || first == "link" || first == "links" || first == "setup" || first == "doctor") && forced || (first == "setup" || first == "doctor" || first == "link") && redirected
}
