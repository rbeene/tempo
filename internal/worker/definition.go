package worker

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/rbeene/tempo/internal/privatefs"
)

func xmlText(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
func unitToken(s string) string { return strconv.Quote(strings.ReplaceAll(s, "%", "%%")) }
func (s *Service) definition() definition {
	if s.options.Platform == "linux" {
		label := "tempo-worker-" + digest([]byte(s.options.StatePath)) + ".service"
		text := "[Unit]\nDescription=Tempo automatic sync worker\n[Service]\nType=exec\nExecStart=" + unitToken(":"+s.options.Executable) + " worker run\nEnvironment=" + unitToken("TEMPO_STATE="+s.options.StatePath) + " " + unitToken("TEMPO_CONFIG="+s.options.ConfigPath) + " " + unitToken("TEMPO_WORKER_MODE=managed") + "\nRestart=on-failure\nRestartSec=10\nTimeoutStopSec=35\nStandardInput=null\nStandardOutput=null\nStandardError=null\n[Install]\nWantedBy=default.target\n"
		b := []byte(text)
		return definition{ServiceID: label, Path: filepath.Join(s.options.ServiceDir, label), Bytes: b, Digest: digest(b)}
	}

	label := "io.beene.tempo.worker." + digest([]byte(s.options.StatePath))
	text := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>` + label + `</string>
<key>Disabled</key><true/>
<key>ProgramArguments</key><array><string>` + xmlText(s.options.Executable) + `</string><string>worker</string><string>run</string></array>
<key>EnvironmentVariables</key><dict><key>TEMPO_STATE</key><string>` + xmlText(s.options.StatePath) + `</string><key>TEMPO_CONFIG</key><string>` + xmlText(s.options.ConfigPath) + `</string><key>TEMPO_WORKER_MODE</key><string>managed</string></dict>
<key>KeepAlive</key><true/><key>ThrottleInterval</key><integer>10</integer><key>ExitTimeOut</key><integer>35</integer>
<key>StandardInPath</key><string>/dev/null</string><key>StandardOutPath</key><string>/dev/null</string><key>StandardErrorPath</key><string>/dev/null</string>
</dict></plist>
`
	b := []byte(text)
	return definition{ServiceID: label, Path: filepath.Join(s.options.ServiceDir, label+".plist"), Bytes: b, Digest: digest(b)}
}
func (s *Service) installDefinition(ctx context.Context, d definition) error {
	if s.options.Platform == "linux" {
		return s.installLinux(ctx, d)
	}
	if e := s.verifyManagerVacant(ctx, d.ServiceID); e != nil {
		return e
	}
	domain := fmt.Sprintf("gui/%d", s.options.UID)
	if _, e := s.manager(ctx, "disable", domain+"/"+d.ServiceID); e != nil {
		return e
	}
	if e := s.fault("control_after_disabled"); e != nil {
		return e
	}
	result, e := s.manager(ctx, "print-disabled", domain)
	if e != nil {
		return e
	}
	if !disabledEvidence(result.Stdout, d.ServiceID) {
		return issue("manager")
	}
	return s.publishDefinition(d)
}
func (s *Service) publishDefinition(d definition) error {
	if e := s.fault("control_before_publish"); e != nil {
		return e
	}
	if e := s.requireUnpublished(d); e != nil {
		return e
	}
	root, info, e := privatefs.OpenServiceDirectory(s.options.ServiceDir, true)
	if e != nil {
		return fileError(e)
	}
	defer root.Close()
	verify := func() error {
		now, e := os.Lstat(s.options.ServiceDir)
		if e != nil || !os.SameFile(info, now) {
			return issue("state_corrupt")
		}
		_, e = root.Lstat(filepath.Base(d.Path))
		if !os.IsNotExist(e) {
			return issue("revision_conflict")
		}
		return nil
	}
	if e = writeAtomic(root, filepath.Base(d.Path), d.Bytes, verify, s.fault, true); e != nil {
		return e
	}
	return s.fault("control_after_publish")
}
func disabledEvidence(b []byte, label string) bool {
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[0]) != "disabled services = {" || strings.TrimSpace(lines[len(lines)-1]) != "}" {
		return false
	}
	pattern := regexp.MustCompile(`^"([^"\x00-\x1f]+)"\s*=>\s*(true|false)$`)
	seen := map[string]bool{}
	found := false
	for _, line := range lines[1 : len(lines)-1] {
		m := pattern.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil || seen[m[1]] {
			return false
		}
		seen[m[1]] = true
		if m[1] == label {
			found = m[2] == "true"
		}
	}
	return found
}

func (s *Service) verifyManagerVacant(ctx context.Context, label string) error {
	loaded, e := s.managerLoaded(ctx, label)
	if e != nil {
		return e
	}
	if loaded {
		return issue("revision_conflict")
	}
	return nil
}
func (s *Service) managerLoaded(ctx context.Context, label string) (bool, error) {
	uid, e := s.manager(ctx, "manageruid")
	if e != nil {
		return false, e
	}
	if strings.TrimSpace(string(uid.Stdout)) != strconv.Itoa(s.options.UID) {
		return false, issue("manager")
	}
	name, e := s.manager(ctx, "managername")
	if e != nil {
		return false, e
	}
	if strings.TrimSpace(string(name.Stdout)) != "Aqua" {
		return false, issue("manager")
	}
	jobs, e := s.manager(ctx, "list")
	if e != nil {
		return false, e
	}
	if len(jobs.Stdout) == 0 || jobs.Stdout[len(jobs.Stdout)-1] != '\n' {
		return false, issue("manager")
	}
	lines := strings.Split(strings.TrimSuffix(string(jobs.Stdout), "\n"), "\n")
	if len(lines) == 0 || strings.Join(strings.Fields(lines[0]), " ") != "PID Status Label" {
		return false, issue("manager")
	}
	found := false
	seen := map[string]bool{}
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) != 3 {
			return false, issue("manager")
		}
		if fields[0] != "-" {
			pid, e := strconv.ParseInt(fields[0], 10, 64)
			if e != nil || pid <= 0 {
				return false, issue("manager")
			}
		}
		if _, e := strconv.ParseInt(fields[1], 10, 32); e != nil {
			return false, issue("manager")
		}
		if fields[2] == "" || seen[fields[2]] {
			return false, issue("manager")
		}
		seen[fields[2]] = true
		if fields[2] == label {
			found = true
		}
	}
	return found, nil
}

// Recovery accepts only the exact bytes named by the durable original intent.
// It neither republishes the file nor changes the manager's persistent policy.
func (s *Service) recoverPublication(ctx context.Context, d definition) error {
	if s.options.Platform == "linux" {
		return s.recoverLinuxPublication(ctx, d)
	}
	if e := s.verifyManagerVacant(ctx, d.ServiceID); e != nil {
		return e
	}
	result, e := s.manager(ctx, "print-disabled", fmt.Sprintf("gui/%d", s.options.UID))
	if e != nil {
		return e
	}
	if !disabledEvidence(result.Stdout, d.ServiceID) {
		return issue("manager")
	}
	return s.resyncPublication(d)
}
func (s *Service) resyncPublication(d definition) error {
	if e := s.fault("control_recovery_resync"); e != nil {
		return e
	}
	root, info, e := privatefs.OpenServiceDirectory(s.options.ServiceDir, false)
	if e != nil {
		return issue("revision_conflict")
	}
	defer root.Close()
	if e = s.verifyDefinition(d); e != nil {
		return e
	}
	f, e := privatefs.OpenNoFollow(root, filepath.Base(d.Path), os.O_RDONLY, 0)
	if e != nil {
		return issue("revision_conflict")
	}
	defer f.Close()
	if e = f.Sync(); e != nil {
		return issue("local_write_unknown")
	}
	now, e := os.Lstat(s.options.ServiceDir)
	if e != nil || !os.SameFile(info, now) {
		return issue("revision_conflict")
	}
	if e = s.verifyDefinition(d); e != nil {
		return e
	}
	dir, e := root.Open(".")
	if e != nil {
		return issue("local_write_unknown")
	}
	defer dir.Close()
	if e = dir.Sync(); e != nil {
		return issue("local_write_unknown")
	}
	return nil
}
