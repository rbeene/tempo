package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rbeene/tempo/internal/activity"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type qaWorkerManager struct {
	verified    bool
	t           *testing.T
	o           Options
	commands    []Command
	disabled    bool
	label       string
	ambiguous   bool
	requestID   string
	loadedLabel string
	probeMode   string
}

func (m *qaWorkerManager) Run(_ context.Context, c Command) (CommandResult, error) {
	m.commands = append(m.commands, c)
	if len(c.Args) == 0 {
		m.t.Fatalf("empty manager command%+v", c)
	}
	if strings.Contains(c.Executable, "sh") {
		m.t.Fatalf("manager uses shell%+v", c)
	}
	switch c.Args[0] {
	case "manageruid":
		if m.probeMode == "wrong_uid" {
			return CommandResult{Stdout: []byte("502\n")}, nil
		}
		return CommandResult{Stdout: []byte("501\n")}, nil
	case "managername":
		if m.probeMode == "wrong_domain" {
			return CommandResult{Stdout: []byte("System\n")}, nil
		}
		return CommandResult{Stdout: []byte("Aqua\n")}, nil
	case "list":
		switch m.probeMode {
		case "malformed":
			return CommandResult{Stdout: []byte("unparseable\n")}, nil
		case "truncated":
			return CommandResult{Stdout: []byte("PID\tStatus\tLabel\n123\t0")}, nil
		case "failed":
			return CommandResult{ExitCode: 1}, nil
		}
		table := "PID\tStatus\tLabel\n"
		if m.loadedLabel != "" {
			table += "123\t0\t" + m.loadedLabel + "\n"
		}
		return CommandResult{Stdout: []byte(table)}, nil
	case "disable":
		if len(c.Args) != 2 || !strings.HasPrefix(c.Args[1], "gui/501/") {
			m.t.Fatalf("unsafe disable scope%+v", c)
		}
		m.label = strings.TrimPrefix(c.Args[1], "gui/501/")
		b, err := os.ReadFile(m.o.StatePath + ".worker-control.json")
		if err != nil {
			m.t.Fatalf("manager side effect before durable reservation: %v", err)
		}
		var record controlRecord
		if json.Unmarshal(b, &record) != nil || record.Pending == nil || record.Pending.Action != "install" || record.Pending.Request.RequestID != m.requestID {
			m.t.Fatalf("bad control reservation=%s", b)
		}
		qaWorkerNoPublished(m.t, m.o.ServiceDir)
		m.disabled = true
		return CommandResult{}, nil
	case "print-disabled":
		if len(c.Args) != 2 || c.Args[1] != "gui/501" {
			m.t.Fatalf("unsafe probe%+v", c)
		}
		if !m.verified {
			qaWorkerNoPublished(m.t, m.o.ServiceDir)
		}
		if m.ambiguous {
			return CommandResult{Stdout: []byte("unknown synthetic format")}, nil
		}
		if !m.disabled {
			return CommandResult{Stdout: []byte("disabled services = {\n}\n")}, nil
		}
		m.verified = true
		return CommandResult{Stdout: []byte("disabled services = {\n\t\"" + m.label + "\" => true\n}\n")}, nil
	default:
		m.t.Fatalf("install started/enabled or made unexpected manager action%+v", c)
		return CommandResult{}, nil
	}
}
func qaWorkerNoPublished(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("definition published before verified disable: %v", entries)
	}
}
func qaWorkerReadControl(t *testing.T, o Options) controlRecord {
	t.Helper()
	b, err := os.ReadFile(o.StatePath + ".worker-control.json")
	if err != nil {
		t.Fatal(err)
	}
	var r controlRecord
	if err = json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	return r
}
func qaWorkerInstallFixture(t *testing.T) (*Service, Options, *qaWorkerManager, Result) {
	t.Helper()
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(1)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	result, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(1), Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	return s, o, m, result
}
func TestQAWorkerInstallReservesThenDisablesBeforePublicationWithoutStarting(t *testing.T) {
	s, o, m, result := qaWorkerInstallFixture(t)
	if !m.disabled || m.label == "" || result.ContractVersion != 1 || result.Status.State == "running" {
		t.Fatalf("unsafe install result%+v commands%+v", result, m.commands)
	}
	record := qaWorkerReadControl(t, o)
	if record.Pending != nil || record.Owned == nil || record.Receipts[qaWorkerID(1)].Result == nil {
		t.Fatalf("install not terminal=%+v", record)
	}
	published, err := os.ReadFile(record.Owned.Path)
	if err != nil || !reflect.DeepEqual(published, record.Owned.Bytes) {
		t.Fatalf("owned definition mismatch %v", err)
	}
	if !strings.Contains(string(published), "<key>Disabled</key>") || !strings.Contains(string(published), "<true") {
		t.Fatalf("published definition lacks default disabled flag%s", published)
	}
	// Simulated new login: only a published definition with persistent enablement
	// could start. The manager's override remains disabled throughout installation.
	if !m.disabled {
		t.Fatal("new session could run installed worker")
	}
	if _, err = os.Stat(o.StatePath); !os.IsNotExist(err) {
		t.Fatalf("install initialized capture state: %v", err)
	}
	calls := len(m.commands)
	replay, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(1), Confirmed: true})
	if err != nil || !reflect.DeepEqual(result, replay) || len(m.commands) != calls {
		t.Fatalf("completed replay changed effects/result%+v err%v calls%d", replay, err, len(m.commands))
	}
	_, err = s.Start(context.Background(), ControlRequest{RequestID: qaWorkerID(1)})
	qaWorkerCode(t, err, "request_conflict")
	if len(m.commands) != calls {
		t.Fatal("conflicting request changed manager")
	}
}
func TestQAWorkerInstallUnknownDisableEvidenceCannotPublish(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, ambiguous: true, requestID: qaWorkerID(2)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	_, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(2), Confirmed: true})
	if err == nil || !m.disabled {
		t.Fatalf("did not reach and reject unknown disable verification: err%v commands%+v", err, m.commands)
	}
	qaWorkerNoPublished(t, o.ServiceDir)
}
func TestQAWorkerUnconfirmedInstallHasNoFilesystemOrManagerEffects(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	s := qaWorkerNew(t, o)
	_, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(3)})
	qaWorkerCode(t, err, "confirmation_required")
	qaWorkerAbsent(t, o)
}
func TestQAWorkerUninstallRejectsEditedOwnedDefinitionBeforeManagerEffects(t *testing.T) {
	s, o, m, _ := qaWorkerInstallFixture(t)
	record := qaWorkerReadControl(t, o)
	edited := append([]byte("user edit\n"), record.Owned.Bytes...)
	if err := os.WriteFile(record.Owned.Path, edited, 0600); err != nil {
		t.Fatal(err)
	}
	calls := len(m.commands)
	_, err := s.Uninstall(context.Background(), ControlRequest{RequestID: qaWorkerID(4), Confirmed: true})
	qaWorkerCode(t, err, "revision_conflict")
	after, err := os.ReadFile(record.Owned.Path)
	if err != nil || !reflect.DeepEqual(after, edited) || len(m.commands) != calls {
		t.Fatalf("uninstall touched edited definition/effects err%v commands%+v", err, m.commands)
	}
	if _, err = os.Stat(filepath.Dir(o.StatePath)); err != nil {
		t.Fatal("control history removed")
	}
}

func TestQAWorkerPendingInstallRecoveryPreservesRequest(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(21)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	hit := 0
	s.fail = func(stage string) error {
		if stage == "control_after_reserved" {
			hit++
			return errors.New("synthetic interrupted reservation")
		}
		return nil
	}
	req := ControlRequest{RequestID: qaWorkerID(21), Confirmed: true}
	_, err := s.Install(context.Background(), req)
	if err == nil || hit != 1 || m.disabled {
		t.Fatalf("reservation boundary hit%d disabled%v err%v", hit, m.disabled, err)
	}
	before := qaWorkerReadControl(t, o)
	if before.Pending == nil || before.Pending.Request != req {
		t.Fatalf("lost reservation%+v", before)
	}
	restarted := qaWorkerNew(t, o)
	result, err := restarted.Install(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	after := qaWorkerReadControl(t, o)
	receipt := after.Receipts[req.RequestID]
	if after.Pending != nil || receipt.Result == nil || !reflect.DeepEqual(*receipt.Result, result) || receipt.Intent.Fingerprint != before.Pending.Fingerprint {
		t.Fatalf("recovered wrong receipt%+v", after)
	}
	calls := len(m.commands)
	_, err = restarted.Install(context.Background(), req)
	if err != nil || len(m.commands) != calls {
		t.Fatalf("terminal replay effects err%v", err)
	}
}
func TestQAWorkerPendingInstallCannotReplaceForeignDefinition(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(22)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	hit := 0
	s.fail = func(stage string) error {
		if stage == "control_after_reserved" {
			hit++
			return errors.New("reserved")
		}
		return nil
	}
	req := ControlRequest{RequestID: qaWorkerID(22), Confirmed: true}
	_, err := s.Install(context.Background(), req)
	if err == nil || hit != 1 {
		t.Fatalf("did not reserve hit%d err%v", hit, err)
	}
	pending := qaWorkerReadControl(t, o).Pending
	if pending == nil || pending.Expected.Path == "" {
		t.Fatal("missing durable expected path")
	}
	if err = os.MkdirAll(filepath.Dir(pending.Expected.Path), 0700); err != nil {
		t.Fatal(err)
	}
	foreign := []byte("foreign service bytes\n")
	if err = os.WriteFile(pending.Expected.Path, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	m.commands = nil
	_, err = qaWorkerNew(t, o).Install(context.Background(), req)
	qaWorkerCode(t, err, "revision_conflict")
	got, err := os.ReadFile(pending.Expected.Path)
	if err != nil || !reflect.DeepEqual(got, foreign) || m.disabled {
		t.Fatalf("foreign definition changed disabled%v err%v", m.disabled, err)
	}
}
func TestQAWorkerUnknownManagerContextCannotDisableOrPublish(t *testing.T) {
	for _, mode := range []string{"wrong_uid", "wrong_domain", "malformed", "truncated", "failed"} {
		t.Run(mode, func(t *testing.T) {
			o, _ := qaWorkerOptions(t)
			m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(23), probeMode: mode}
			o.Runner = m
			_, err := qaWorkerNew(t, o).Install(context.Background(), ControlRequest{RequestID: qaWorkerID(23), Confirmed: true})
			if err == nil || len(m.commands) == 0 || m.disabled {
				t.Fatalf("did not reject observed unknown scope commands%v disabled%v err%v", m.commands, m.disabled, err)
			}
			qaWorkerNoPublished(t, o.ServiceDir)
		})
	}
}
func TestQAWorkerLoadedForeignLabelCannotBeDisabled(t *testing.T) {
	_, o, m, _ := qaWorkerInstallFixture(t)
	owned := qaWorkerReadControl(t, o).Owned
	if err := os.Remove(owned.Path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(o.StatePath + ".worker-control.json"); err != nil {
		t.Fatal(err)
	}
	// Fixture retains a loaded exact label but removes all Tempo ownership evidence.
	m.loadedLabel = owned.ServiceID
	m.commands = nil
	m.disabled = false
	m.verified = false
	m.requestID = qaWorkerID(24)
	_, err := qaWorkerNew(t, o).Install(context.Background(), ControlRequest{RequestID: qaWorkerID(24), Confirmed: true})
	if err == nil || m.disabled || len(m.commands) == 0 {
		t.Fatalf("foreign loaded label touched commands%v err%v", m.commands, err)
	}
	qaWorkerNoPublished(t, o.ServiceDir)
}

func TestQAWorkerPublishedPendingInstallRecoversExactDefinition(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(31)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	hit := 0
	s.fail = func(stage string) error {
		if stage == "control_after_publish" {
			hit++
			return errors.New("crash after exact publication")
		}
		return nil
	}
	req := ControlRequest{RequestID: qaWorkerID(31), Confirmed: true}
	_, err := s.Install(context.Background(), req)
	if err == nil || hit != 1 {
		t.Fatalf("publication barrier hit%d err%v", hit, err)
	}
	pending := qaWorkerReadControl(t, o)
	if pending.Pending == nil {
		t.Fatal("lost publication intent")
	}
	before, err := os.ReadFile(pending.Pending.Expected.Path)
	if err != nil || !reflect.DeepEqual(before, pending.Pending.Expected.Bytes) {
		t.Fatalf("fixture didn't publish owned bytes%v", err)
	}
	result, err := qaWorkerNew(t, o).Install(context.Background(), req)
	if err != nil {
		t.Fatalf("exact pending publication cannot recover: %v", err)
	}
	completed := qaWorkerReadControl(t, o)
	after, err := os.ReadFile(pending.Pending.Expected.Path)
	if err != nil || !reflect.DeepEqual(before, after) || completed.Pending != nil || completed.Owned == nil || completed.Receipts[req.RequestID].Result == nil || !reflect.DeepEqual(*completed.Receipts[req.RequestID].Result, result) {
		t.Fatalf("bad publication recovery%+v err%v", completed, err)
	}
}

func TestQAWorkerInstallPublicationCannotReplaceRacingForeignFile(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	m := &qaWorkerManager{t: t, o: o, requestID: qaWorkerID(32)}
	o.Runner = m
	s := qaWorkerNew(t, o)
	injected := 0
	var target string
	foreign := []byte("foreign file created at publication boundary\n")
	s.fail = func(stage string) error {
		if stage != "rename" || injected != 0 {
			return nil
		}
		entries, err := os.ReadDir(o.ServiceDir)
		if err != nil {
			return nil
		}
		hasTemp := false
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), ".tempo") {
				hasTemp = true
			}
		}
		if !hasTemp {
			return nil
		}
		pending := qaWorkerReadControl(t, o).Pending
		if pending == nil {
			t.Fatal("publication has no durable intent")
		}
		target = pending.Expected.Path
		if err = os.WriteFile(target, foreign, 0600); err != nil {
			t.Fatal(err)
		}
		injected++
		return nil
	}
	_, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(32), Confirmed: true})
	if injected != 1 {
		t.Fatalf("did not reach service rename injection: err%v", err)
	}
	got, readErr := os.ReadFile(target)
	if err == nil || readErr != nil || !reflect.DeepEqual(got, foreign) {
		t.Fatalf("foreign publication overwritten err%v read%v bytes%s", err, readErr, got)
	}
}

type qaWorkerLifecycleManager struct {
	t                *testing.T
	o                Options
	label, path      string
	disabled, loaded bool
	commands         []Command
}

func (m *qaWorkerLifecycleManager) Run(_ context.Context, c Command) (CommandResult, error) {
	m.commands = append(m.commands, c)
	if len(c.Args) == 0 {
		m.t.Fatal("empty manager args")
	}
	op := c.Args[0]
	switch op {
	case "manageruid":
		return CommandResult{Stdout: []byte("501\n")}, nil
	case "managername":
		return CommandResult{Stdout: []byte("Aqua\n")}, nil
	case "list":
		table := "PID\tStatus\tLabel\n"
		if m.loaded {
			table += "123\t0\t" + m.label + "\n"
		}
		return CommandResult{Stdout: []byte(table)}, nil
	case "print-disabled":
		return CommandResult{Stdout: []byte("disabled services = {\n\"" + m.label + "\" => " + map[bool]string{true: "true", false: "false"}[m.disabled] + "\n}\n")}, nil
	case "enable", "disable", "bootstrap", "bootout", "kickstart":
		pending := qaWorkerReadControl(m.t, m.o).Pending
		if pending == nil {
			m.t.Fatalf("lifecycle effect%s without durable intent", op)
		}
		if !reflect.DeepEqual(pending.Expected.Bytes, qaWorkerReadControl(m.t, m.o).Owned.Bytes) {
			m.t.Fatal("effect did not reserve exact ownership")
		}
		switch op {
		case "enable":
			if pending.Action != "start" {
				m.t.Fatal("non-start enabled")
			}
			m.disabled = false
		case "disable":
			if pending.Action != "stop" && pending.Action != "uninstall" {
				m.t.Fatal("unexpected disable")
			}
			m.disabled = true
		case "bootstrap":
			if m.disabled || pending.Action != "start" || len(c.Args) != 3 || c.Args[1] != "gui/501" || c.Args[2] != m.path {
				m.t.Fatalf("unsafe bootstrap%+v", c)
			}
			m.loaded = true
		case "bootout":
			if !m.disabled {
				m.t.Fatal("bootout before persistent disable")
			}
			m.loaded = false
		case "kickstart":
			if strings.Contains(strings.Join(c.Args, " "), "-k") {
				m.t.Fatal("kickstart killed healthy process")
			}
			if m.disabled {
				m.t.Fatal("kickstart disabled service")
			}
		}
		return CommandResult{}, nil
	default:
		m.t.Fatalf("unexpected lifecycle manager command%+v", c)
		return CommandResult{}, nil
	}
}
func TestQAWorkerStartStopUninstallPreserveRuntimeAndReplay(t *testing.T) {
	_, o, _, _ := qaWorkerInstallFixture(t)
	owned := qaWorkerReadControl(t, o).Owned
	m := &qaWorkerLifecycleManager{t: t, o: o, label: owned.ServiceID, path: owned.Path, disabled: true}
	o.Runner = m
	s := qaWorkerNew(t, o)
	pending := activity.SyncRunInput{RequestID: qaWorkerID(300), Limit: 20}
	qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, Pending: &pending, InstanceMode: "foreground"})
	runtimeBefore, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
	if err != nil {
		t.Fatal(err)
	}
	started, err := s.Start(context.Background(), ControlRequest{RequestID: qaWorkerID(301)})
	if err != nil {
		t.Fatal(err)
	}
	if m.disabled || !m.loaded || started.Status.State == "running" {
		t.Fatalf("start invented live owner or didn't enable/load result%+v manager%+v", started, m)
	}
	stopped, err := s.Stop(context.Background(), ControlRequest{RequestID: qaWorkerID(302)})
	if err != nil {
		t.Fatal(err)
	}
	if !m.disabled || m.loaded || stopped.Status.State != "stopped" {
		t.Fatalf("stop failed persistent disable%+v", stopped)
	}
	uninstalled, err := s.Uninstall(context.Background(), ControlRequest{RequestID: qaWorkerID(303), Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if uninstalled.Status.Installed == nil || *uninstalled.Status.Installed {
		t.Fatalf("uninstall retained installed status%+v", uninstalled)
	}
	if _, err = os.Stat(owned.Path); !os.IsNotExist(err) {
		t.Fatalf("owned definition survived uninstall%v", err)
	}
	runtimeAfter, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
	if err != nil || !reflect.DeepEqual(runtimeBefore, runtimeAfter) {
		t.Fatalf("control lifecycle erased pending runtime%v", err)
	}
	records := qaWorkerReadControl(t, o)
	if records.Pending != nil || records.Owned != nil || len(records.Receipts) != 4 {
		t.Fatalf("uninstall erased replay journal%+v", records)
	}
	calls := len(m.commands)
	again, err := s.Start(context.Background(), ControlRequest{RequestID: qaWorkerID(301)})
	if err != nil || !reflect.DeepEqual(started, again) || len(m.commands) != calls {
		t.Fatalf("old start replay restarted uninstalled service err%v", err)
	}
}

func TestQAWorkerStopAndUninstallDisableBeforeBootout(t *testing.T) {
	for _, action := range []string{"stop", "uninstall"} {
		t.Run(action, func(t *testing.T) {
			_, o, _, _ := qaWorkerInstallFixture(t)
			owned := qaWorkerReadControl(t, o).Owned
			m := &qaWorkerLifecycleManager{t: t, o: o, label: owned.ServiceID, path: owned.Path, loaded: true}
			o.Runner = m
			s := qaWorkerNew(t, o)
			pending := activity.SyncRunInput{RequestID: qaWorkerID(310), Limit: 20}
			qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, Pending: &pending, InstanceMode: "managed"})
			before, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
			if err != nil {
				t.Fatal(err)
			}
			var result Result
			if action == "stop" {
				result, err = s.Stop(context.Background(), ControlRequest{RequestID: qaWorkerID(311)})
			} else {
				result, err = s.Uninstall(context.Background(), ControlRequest{RequestID: qaWorkerID(311), Confirmed: true})
			}
			if err != nil {
				t.Fatal(err)
			}
			if !m.disabled || m.loaded || result.Status.State == "running" {
				t.Fatalf("unsafe stop result%+v manager%+v", result, m)
			}
			after, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("lifecycle erased pending runtime%v", err)
			}
			record := qaWorkerReadControl(t, o)
			if record.Pending != nil || record.Receipts[qaWorkerID(311)].Result == nil {
				t.Fatalf("missing lifecycle terminal receipt%+v", record)
			}
			if action == "uninstall" {
				if record.Owned != nil {
					t.Fatal("uninstall retained ownership")
				}
				if _, err = os.Stat(owned.Path); !os.IsNotExist(err) {
					t.Fatalf("uninstall did not remove exact owned file%v", err)
				}
			} else if _, err = os.Stat(owned.Path); err != nil {
				t.Fatalf("stop deleted service definition%v", err)
			}
		})
	}
}

func TestQAWorkerControlHistoryCapacityPreservesExistingReplay(t *testing.T) {
	s, o, m, original := qaWorkerInstallFixture(t)
	record := qaWorkerReadControl(t, o)
	template := record.Receipts[qaWorkerID(1)]
	for i := 2; i <= 4096; i++ {
		r := template
		r.Intent.Request.RequestID = qaWorkerID(i)
		record.Receipts[qaWorkerID(i)] = r
	}
	bytes, err := json.Marshal(record)
	if err != nil || len(bytes) >= 16<<20 {
		t.Fatalf("capacity fixture exceeds independent byte cap size%d err%v", len(bytes), err)
	}
	if err = os.WriteFile(o.StatePath+".worker-control.json", bytes, 0600); err != nil {
		t.Fatal(err)
	}
	calls := len(m.commands)
	_, err = s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(4097), Confirmed: true})
	qaWorkerCode(t, err, "control_history_full")
	var typed *Error
	if !errors.As(err, &typed) || typed.Retryable || typed.Uncertain {
		t.Fatalf("capacity error not certain/nonretryable%#v", err)
	}
	after, err := os.ReadFile(o.StatePath + ".worker-control.json")
	if err != nil || !reflect.DeepEqual(bytes, after) || len(m.commands) != calls {
		t.Fatalf("capacity refusal changed history/effects%v", err)
	}
	replay, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(1), Confirmed: true})
	if err != nil || !reflect.DeepEqual(original, replay) || len(m.commands) != calls {
		t.Fatalf("capacity blocked existing receipt replay%v", err)
	}
}

type qaWorkerSystemdManager struct {
	t                       *testing.T
	o                       Options
	path                    string
	loaded, enabled, active bool
	foreign, malformed      bool
	commands                []Command
	mutations               []string
}

func (m *qaWorkerSystemdManager) Run(_ context.Context, c Command) (CommandResult, error) {
	m.commands = append(m.commands, c)
	if filepath.Base(c.Executable) != "systemctl" {
		m.t.Fatalf("Linux called wrong manager%+v", c)
	}
	args := c.Args
	if len(args) == 1 && args[0] == "--version" {
		return CommandResult{Stdout: []byte("systemd 247\n")}, nil
	}
	if len(args) < 2 || args[0] != "--user" {
		m.t.Fatalf("manager escaped per-user scope%+v", c)
	}
	op := args[1]
	if op == "--version" {
		return CommandResult{Stdout: []byte("systemd 247\n")}, nil
	}
	if op == "show" {
		if m.malformed {
			return CommandResult{Stdout: []byte("LoadState=loaded\nActiveState=unknown\n")}, nil
		}
		if m.foreign {
			return CommandResult{Stdout: []byte("LoadState=loaded\nActiveState=inactive\nFragmentPath=/synthetic/foreign.service\nUnitFileState=disabled\n")}, nil
		}
		if !m.loaded {
			return CommandResult{Stdout: []byte("LoadState=not-found\nActiveState=inactive\nFragmentPath=\nUnitFileState=\n")}, nil
		}
		return CommandResult{Stdout: []byte("LoadState=loaded\nActiveState=" + map[bool]string{true: "active", false: "inactive"}[m.active] + "\nFragmentPath=" + m.path + "\nUnitFileState=" + map[bool]string{true: "enabled", false: "disabled"}[m.enabled] + "\n")}, nil
	}
	switch op {
	case "daemon-reload", "enable", "start", "disable", "stop":
	default:
		m.t.Fatalf("unapproved manager action%+v", c)
	}
	pending := qaWorkerReadControl(m.t, m.o).Pending
	if pending == nil {
		m.t.Fatalf("systemd effect before reservation%s", op)
	}
	m.path = pending.Expected.Path
	m.mutations = append(m.mutations, op)
	switch op {
	case "daemon-reload":
		b, err := os.ReadFile(m.path)
		if err == nil {
			if !reflect.DeepEqual(b, pending.Expected.Bytes) {
				m.t.Fatal("reload before exact definition publication")
			}
			m.loaded = true
		} else if os.IsNotExist(err) && pending.Action == "uninstall" {
			m.loaded = false
		} else {
			m.t.Fatalf("reload without published owned file%v", err)
		}
	case "enable":
		if pending.Action != "start" {
			m.t.Fatal("install enabled service")
		}
		m.enabled = true
	case "start":
		if !m.enabled || !m.loaded {
			m.t.Fatal("start before explicit enable/publication")
		}
		m.active = true
	case "disable":
		m.enabled = false
	case "stop":
		if m.enabled {
			m.t.Fatal("stop before disable")
		}
		m.active = false
	}
	return CommandResult{}, nil
}
func TestQAWorkerLinuxPerUserLifecyclePreservesRuntime(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	o.Platform = "linux"
	m := &qaWorkerSystemdManager{t: t, o: o}
	o.Runner = m
	s := qaWorkerNew(t, o)
	_, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(501), Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !m.loaded || m.enabled || m.active || !reflect.DeepEqual(m.mutations, []string{"daemon-reload"}) {
		t.Fatalf("install enabled/started or skipped publication%+v", m)
	}
	pending := activity.SyncRunInput{RequestID: qaWorkerID(510), Limit: 20}
	qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, Pending: &pending, InstanceMode: "managed"})
	before, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
	if err != nil {
		t.Fatal(err)
	}
	started, err := s.Start(context.Background(), ControlRequest{RequestID: qaWorkerID(502)})
	if err != nil {
		t.Fatal(err)
	}
	if !m.enabled || !m.active || started.Status.State == "running" {
		t.Fatalf("start failed or invented owner%+v", started)
	}
	if _, err = s.Stop(context.Background(), ControlRequest{RequestID: qaWorkerID(503)}); err != nil {
		t.Fatal(err)
	}
	if m.enabled || m.active {
		t.Fatal("stop retained login enablement")
	}
	if _, err = s.Uninstall(context.Background(), ControlRequest{RequestID: qaWorkerID(504), Confirmed: true}); err != nil {
		t.Fatal(err)
	}
	if m.loaded {
		t.Fatal("uninstall did not reload removal")
	}
	after, err := os.ReadFile(o.StatePath + ".worker-runtime.json")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("Linux lifecycle erased pending pass%v", err)
	}
	if len(qaWorkerReadControl(t, o).Receipts) != 4 {
		t.Fatal("Linux lifecycle lost receipts")
	}
}
func TestQAWorkerLinuxForeignOrUnknownManagerCannotPublish(t *testing.T) {
	for _, mode := range []string{"foreign", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			o, _ := qaWorkerOptions(t)
			o.Platform = "linux"
			m := &qaWorkerSystemdManager{t: t, o: o, foreign: mode == "foreign", malformed: mode == "malformed"}
			o.Runner = m
			_, err := qaWorkerNew(t, o).Install(context.Background(), ControlRequest{RequestID: qaWorkerID(520), Confirmed: true})
			if err == nil || len(m.commands) == 0 || len(m.mutations) != 0 {
				t.Fatalf("guard not reached or mutated commands%v effects%v err%v", m.commands, m.mutations, err)
			}
			qaWorkerNoPublished(t, o.ServiceDir)
		})
	}
}

func TestQAWorkerPendingUninstallRecoversAlreadyRemovedDefinition(t *testing.T) {
	_, o, _, _ := qaWorkerInstallFixture(t)
	owned := qaWorkerReadControl(t, o).Owned
	m := &qaWorkerLifecycleManager{t: t, o: o, label: owned.ServiceID, path: owned.Path, loaded: true}
	o.Runner = m
	s := qaWorkerNew(t, o)
	hit := 0
	s.fail = func(stage string) error {
		if stage == "control_after_manager_effect" {
			hit++
			return errors.New("lost uninstall completion")
		}
		return nil
	}
	req := ControlRequest{RequestID: qaWorkerID(601), Confirmed: true}
	_, err := s.Uninstall(context.Background(), req)
	if err == nil || hit != 1 {
		t.Fatalf("uninstall didn't reach effect barrier hit%d err%v", hit, err)
	}
	if _, err = os.Stat(owned.Path); !os.IsNotExist(err) {
		t.Fatalf("fixture did not remove owned definition%v", err)
	}
	if m.loaded || !m.disabled {
		t.Fatal("fixture didn't complete manager stop")
	}
	record := qaWorkerReadControl(t, o)
	if record.Pending == nil || record.Pending.Request != req {
		t.Fatal("missing pending uninstall")
	}
	got, err := qaWorkerNew(t, o).Uninstall(context.Background(), req)
	if err != nil {
		t.Fatalf("pending successful removal cannot recover%v", err)
	}
	final := qaWorkerReadControl(t, o)
	if final.Pending != nil || final.Owned != nil || final.Receipts[req.RequestID].Result == nil || got.Status.Installed == nil || *got.Status.Installed {
		t.Fatalf("uninstall replay not terminal%+v", final)
	}
}

func TestQAWorkerLinuxPublicationRecoveryRequiresDurabilityBeforeReceipt(t *testing.T) {
	o, _ := qaWorkerOptions(t)
	o.Platform = "linux"
	m := &qaWorkerSystemdManager{t: t, o: o}
	o.Runner = m
	s := qaWorkerNew(t, o)
	firstHit := 0
	req := ControlRequest{RequestID: qaWorkerID(701), Confirmed: true}
	s.fail = func(stage string) error {
		if stage != "directory_sync" || firstHit != 0 {
			return nil
		}
		record := qaWorkerReadControl(t, o)
		if record.Pending == nil {
			return nil
		}
		if _, err := os.Stat(record.Pending.Expected.Path); err != nil {
			return nil
		}
		firstHit++
		return errors.New("first publication directory durability uncertain")
	}
	_, err := s.Install(context.Background(), req)
	if err == nil || firstHit != 1 {
		t.Fatalf("didn't reach published directory fault hit%d err%v", firstHit, err)
	}
	before, err := os.ReadFile(o.StatePath + ".worker-control.json")
	if err != nil {
		t.Fatal(err)
	}
	pending := qaWorkerReadControl(t, o).Pending
	if pending == nil {
		t.Fatal("publication fault lost original intent")
	}
	recovered := qaWorkerNew(t, o)
	hit := 0
	recovered.fail = func(stage string) error {
		if stage == "control_recovery_resync" {
			hit++
			return errors.New("recovery resync unavailable")
		}
		return nil
	}
	_, err = recovered.Install(context.Background(), req)
	if err == nil || hit != 1 {
		t.Fatalf("replay skipped service durability recovery hit%d err%v", hit, err)
	}
	qaWorkerCode(t, err, "local_write_unknown")
	after, err := os.ReadFile(o.StatePath + ".worker-control.json")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("failed recovery terminalized uncertain publication%v", err)
	}
	recovered.fail = nil
	if _, err = recovered.Install(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	final := qaWorkerReadControl(t, o)
	if final.Pending != nil || final.Receipts[req.RequestID].Result == nil {
		t.Fatal("successful resync didn't finish original receipt")
	}
}

func TestQAWorkerControllerResultPreservesExistingRuntimeFailure(t *testing.T) {
	s, o, _, _ := qaWorkerInstallFixture(t)
	category := "auth"
	qaWorkerWriteRuntime(t, o, runtimeRecord{Version: 1, InstanceMode: "foreground", FailureCategory: &category})
	result, err := s.Install(context.Background(), ControlRequest{RequestID: qaWorkerID(901), Confirmed: true})
	if err != nil {
		t.Fatal(err)
	}
	status, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Status.FailureCategory == nil || *status.Status.FailureCategory != "auth" || result.Status.FailureCategory == nil || *result.Status.FailureCategory != "auth" {
		t.Fatalf("controller result erased actionable failure result%+v current%+v", result.Status, status.Status)
	}
}

func TestQAWorkerUninstallRejectsSymlinkAndHardlinkOwnership(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			s, o, m, _ := qaWorkerInstallFixture(t)
			owned := qaWorkerReadControl(t, o).Owned
			target := filepath.Join(t.TempDir(), "unrelated")
			if err := os.WriteFile(target, owned.Bytes, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(owned.Path); err != nil {
				t.Fatal(err)
			}
			var err error
			if kind == "symlink" {
				err = os.Symlink(target, owned.Path)
			} else {
				err = os.Link(target, owned.Path)
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := len(m.commands)
			_, err = s.Uninstall(context.Background(), ControlRequest{RequestID: qaWorkerID(920), Confirmed: true})
			qaWorkerCode(t, err, "revision_conflict")
			if len(m.commands) != calls {
				t.Fatal("unsafe ownership reached manager")
			}
			got, err := os.ReadFile(target)
			if err != nil || !reflect.DeepEqual(got, owned.Bytes) {
				t.Fatalf("unrelated target changed%v", err)
			}
			if _, err = os.Lstat(owned.Path); err != nil {
				t.Fatal("unsafe link was deleted")
			}
		})
	}
}
