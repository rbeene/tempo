package activity

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQAActivityInitializationTransactionPersistsPrivately(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.restart()
	err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
		if st.ComputerID != qaComputer || st.Bindings[qaBindingA] != h.bindings[qaBindingA] || st.Revision == "" || st.Revision == "0" {
			t.Fatalf("transaction lost initialized identity/bindings/revision: %+v", st)
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		mode os.FileMode
	}{{filepath.Dir(h.path), 0700}, {h.path, 0600}, {h.path + ".lock", 0600}} {
		st, err := os.Stat(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != tc.mode {
			t.Fatalf("%s permissions=%o want %o", tc.path, st.Mode().Perm(), tc.mode)
		}
	}
}

func TestQAActivityAbortedTransactionPreservesBytes(t *testing.T) {
	h := qaNew(t)
	h.seed()
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("synthetic callback rejection")
	err = h.service.store.update(context.Background(), func(st *state) (bool, error) {
		delete(st.Bindings, qaBindingA)
		st.ComputerID = "44444444-4444-4444-8444-444444444444"
		return true, sentinel
	})
	if err == nil {
		t.Fatal("aborted callback was acknowledged")
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("callback failure replaced original durable state")
	}
	h.restart()
	err = h.service.store.update(context.Background(), func(st *state) (bool, error) {
		if st.ComputerID != qaComputer || st.Bindings[qaBindingA].ID != qaBindingA {
			t.Fatal("failed update leaked into reload")
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQAActivityPreRenameFailuresPreserveOriginalState(t *testing.T) {
	for _, stage := range []string{"before_write", "file_sync", "rename"} {
		t.Run(stage, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			before, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			h.service.store.fail = func(s string) error {
				if s == stage {
					return errors.New("PRIVATE-STORAGE-DETAIL")
				}
				return nil
			}
			err = h.service.store.update(context.Background(), func(st *state) (bool, error) { delete(st.Bindings, qaBindingB); return true, nil })
			if err == nil {
				t.Fatal("failed write was acknowledged")
			}
			var ae *Error
			if !errors.As(err, &ae) || ae.Uncertain || strings.Contains(err.Error(), "PRIVATE-STORAGE") {
				t.Fatalf("unsafe precommit error: %#v", err)
			}
			after, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("pre-rename failure changed original state")
			}
			entries, err := os.ReadDir(filepath.Dir(h.path))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if strings.HasSuffix(e.Name(), ".tmp") {
					t.Fatalf("temporary state leaked after failure: %s", e.Name())
				}
			}
		})
	}
}

func TestQAActivityUnknownDurabilityNeedsFreshSyncBeforeAcknowledgment(t *testing.T) {
	h := qaNew(t)
	h.seed()
	h.service.store.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("synthetic directory sync failure")
		}
		return nil
	}
	err := h.service.store.update(context.Background(), func(st *state) (bool, error) { delete(st.Bindings, qaBindingB); return true, nil })
	qaCode(t, err, "local_write_unknown")
	h.restart()
	h.service.store.fail = func(stage string) error {
		if stage == "directory_sync" {
			return errors.New("still not durable")
		}
		return nil
	}
	check := func(st *state) (bool, error) {
		if _, ok := st.Bindings[qaBindingB]; ok {
			t.Fatal("test did not exercise visible post-rename state")
		}
		return false, nil
	}
	err = h.service.store.update(context.Background(), check)
	qaCode(t, err, "local_write_unknown")
	h.service.store.fail = nil
	if err = h.service.store.update(context.Background(), check); err != nil {
		t.Fatalf("durability recovery failed: %v", err)
	}
}

func TestQAActivityReadRejectsMissingLockAndUnsafeStateWithoutWrites(t *testing.T) {
	for _, kind := range []string{"missing-lock", "state-symlink", "lock-symlink", "state-directory", "public-mode", "truncated-json", "future-schema", "duplicate-key", "case-variant"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			original, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing-lock":
				err = os.Remove(h.path + ".lock")
			case "state-symlink":
				target := h.path + ".original"
				if err = os.Rename(h.path, target); err == nil {
					err = os.Symlink(target, h.path)
				}
			case "lock-symlink":
				target := h.path + ".lock-original"
				if err = os.Rename(h.path+".lock", target); err == nil {
					err = os.Symlink(target, h.path+".lock")
				}
			case "state-directory":
				if err = os.Remove(h.path); err == nil {
					err = os.Mkdir(h.path, 0700)
				}
			case "public-mode":
				err = os.Chmod(h.path, 0644)
			case "truncated-json":
				err = os.WriteFile(h.path, []byte(`{"schema_version":1,"secret":"PRIVATE-STATE"`), 0600)
			case "future-schema":
				err = os.WriteFile(h.path, bytes.Replace(original, []byte(`"schema_version":1`), []byte(`"schema_version":2`), 1), 0600)
			case "duplicate-key":
				err = os.WriteFile(h.path, bytes.Replace(original, []byte(`"schema_version":1`), []byte(`"schema_version":1,"schema_version":1`), 1), 0600)
			case "case-variant":
				err = os.WriteFile(h.path, bytes.Replace(original, []byte(`"schema_version":1`), []byte(`"Schema_Version":1`), 1), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(h.path)
			_, _, err = h.service.store.read(context.Background())
			qaCode(t, err, "state_corrupt")
			if strings.Contains(err.Error(), "PRIVATE-STATE") {
				t.Fatal("corrupt state contents leaked")
			}
			after, _ := os.ReadFile(h.path)
			if !bytes.Equal(before, after) {
				t.Fatal("unsafe read changed bytes")
			}
			if kind == "missing-lock" {
				if _, err := os.Lstat(h.path + ".lock"); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("read recreated missing coordination lock")
				}
			}
		})
	}
}

func TestQAActivityStoreProcessHelper(t *testing.T) {
	path := os.Getenv("TEMPO_QA_STORE_PATH")
	if path == "" {
		t.Skip("subprocess helper")
	}
	s := New(Options{Path: path, LockTimeout: time.Second})
	if os.Getenv("TEMPO_QA_HOLD_LOCK") == "1" {
		lock, ok, err := s.store.acquire(context.Background(), false)
		if err != nil || !ok {
			t.Fatalf("hold lock: %v", err)
		}
		defer lock.close()
		fmt.Println("LOCKED")
		var b [1]byte
		_, _ = os.Stdin.Read(b[:])
		return
	}
	id := os.Getenv("TEMPO_QA_BINDING_ID")
	err := s.store.update(context.Background(), func(st *state) (bool, error) {
		st.Bindings[id] = BindingSnapshot{ID: id, Revision: "1", Attribution: Attribution{AccountID: "1", UserID: "2", ProjectID: "3", TaskID: "4", Timezone: "UTC"}}
		return true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestQAActivityStoreConcurrentProcessesPreserveEveryTransaction(t *testing.T) {
	h := qaNew(t)
	h.seed()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const n = 6
	commands := make([]*exec.Cmd, n)
	outputs := make([]bytes.Buffer, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("44444444-4444-4444-8444-%012d", i)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQAActivityStoreProcessHelper$")
		cmd.Env = append(os.Environ(), "TEMPO_QA_STORE_PATH="+h.path, "TEMPO_QA_BINDING_ID="+id)
		cmd.Stdout = &outputs[i]
		cmd.Stderr = &outputs[i]
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		commands[i] = cmd
	}
	for i, cmd := range commands {
		if err := cmd.Wait(); err != nil {
			t.Fatalf("writer%d: %v %s", i, err, outputs[i].String())
		}
	}
	h.restart()
	s, ok, err := h.service.store.read(context.Background())
	if err != nil || !ok {
		t.Fatalf("reload: %v", err)
	}
	if len(s.Bindings) != n+2 {
		t.Fatalf("concurrent processes lost writes: bindings=%d want%d", len(s.Bindings), n+2)
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("44444444-4444-4444-8444-%012d", i)
		if s.Bindings[id].ID != id {
			t.Fatalf("lost binding %s", id)
		}
	}
}

func TestQAActivityHeldProcessLockIsBoundedAndDoesNotAcknowledge(t *testing.T) {
	h := qaNew(t)
	h.seed()
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestQAActivityStoreProcessHelper$")
	cmd.Env = append(os.Environ(), "TEMPO_QA_STORE_PATH="+h.path, "TEMPO_QA_HOLD_LOCK=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stdin.Close(); _ = cmd.Wait() }()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "LOCKED\n" {
		t.Fatalf("holder readiness: %q %v %s", line, err, stderr.String())
	}
	for _, mode := range []string{"timeout", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			readCtx := context.Background()
			if mode == "cancelled" {
				c, cancelRead := context.WithCancel(readCtx)
				cancelRead()
				readCtx = c
			}
			started := time.Now()
			_, _, err := h.service.store.read(readCtx)
			qaCode(t, err, "state_busy")
			if time.Since(started) > time.Second {
				t.Fatal("lock wait exceeded generous one-second bound")
			}
		})
	}
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("blocked read changed state")
	}
}

func TestQAActivityLockReplacementBeforeCommitPreservesOriginalState(t *testing.T) {
	h := qaNew(t)
	h.seed()
	before, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	h.service.store.fail = func(stage string) error {
		if stage != "before_write" {
			return nil
		}
		if err := os.Rename(h.path+".lock", h.path+".old-lock"); err != nil {
			return err
		}
		return os.WriteFile(h.path+".lock", nil, 0600)
	}
	err = h.service.store.update(context.Background(), func(st *state) (bool, error) { delete(st.Bindings, qaBindingB); return true, nil })
	qaCode(t, err, "state_corrupt")
	after, err := os.ReadFile(h.path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("transaction committed after coordination lock replacement")
	}
}

func TestQAActivityCorruptHistoryReferencesPreserveEvidence(t *testing.T) {
	for _, kind := range []string{"negative-duration", "missing-supporting-segment", "outbox-interval-mismatch", "noncanonical-actor-sequence", "finalized-segment-without-output", "duplicate-interval-support", "foreign-interval-computer"} {
		t.Run(kind, func(t *testing.T) {
			h := qaNew(t)
			h.seed()
			h.ingest(0, qaEvent("A", "1", "1", "work", qaBindingA))
			h.ingest(20, qaEvent("A", "1", "2", "finish", ""))
			data, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			var st map[string]any
			if err = json.Unmarshal(data, &st); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "negative-duration":
				st["intervals"].([]any)[0].(map[string]any)["duration_ns"] = "-1"
			case "missing-supporting-segment":
				st["intervals"].([]any)[0].(map[string]any)["segment_ids"] = []any{"99999999-9999-4999-8999-999999999999"}
			case "outbox-interval-mismatch":
				for _, v := range st["outbox"].(map[string]any) {
					v.(map[string]any)["interval"].(map[string]any)["id"] = "99999999-9999-4999-8999-999999999999"
				}
			case "noncanonical-actor-sequence":
				for _, v := range st["actors"].(map[string]any) {
					v.(map[string]any)["sequence"] = "02"
				}
			case "finalized-segment-without-output":
				st["intervals"] = []any{}
				st["outbox"] = map[string]any{}
			case "duplicate-interval-support":
				original := st["intervals"].([]any)[0]
				encoded, err := json.Marshal(original)
				if err != nil {
					t.Fatal(err)
				}
				var cloned map[string]any
				if err := json.Unmarshal(encoded, &cloned); err != nil {
					t.Fatal(err)
				}
				id := "99999999-9999-4999-8999-999999999999"
				cloned["id"] = id
				st["intervals"] = append(st["intervals"].([]any), cloned)
				var item map[string]any
				for _, v := range st["outbox"].(map[string]any) {
					encoded, err = json.Marshal(v)
					if err != nil {
						t.Fatal(err)
					}
					if err = json.Unmarshal(encoded, &item); err != nil {
						t.Fatal(err)
					}
					break
				}
				item["id"] = "88888888-8888-4888-8888-888888888888"
				item["interval"] = cloned
				item["correlation"] = "tempo:" + id
				st["outbox"].(map[string]any)[id] = item
			case "foreign-interval-computer":
				st["intervals"].([]any)[0].(map[string]any)["computer_id"] = "99999999-9999-4999-8999-999999999999"
				for _, v := range st["outbox"].(map[string]any) {
					v.(map[string]any)["interval"].(map[string]any)["computer_id"] = "99999999-9999-4999-8999-999999999999"
				}
			}
			corrupt, err := json.Marshal(st)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(h.path, corrupt, 0600); err != nil {
				t.Fatal(err)
			}
			h.restart()
			_, err = h.service.Status(context.Background())
			qaCode(t, err, "state_corrupt")
			after, err := os.ReadFile(h.path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(corrupt, after) {
				t.Fatal("invalid history was repaired or overwritten by read")
			}
		})
	}
}

func TestQAActivityConcurrentStateReplacementCannotBeOverwritten(t *testing.T) {
	for _, initiallyExists := range []bool{true, false} {
		for _, changed := range []bool{true, false} {
			t.Run(fmt.Sprintf("initially-exists-%t/changed-%t", initiallyExists, changed), func(t *testing.T) {
				h := qaNew(t)
				if initiallyExists {
					h.seed()
				}
				replacement := []byte(`{"schema_version":2,"private_evidence":"preserve concurrent replacement"}`)
				err := h.service.store.update(context.Background(), func(st *state) (bool, error) {
					if !initiallyExists {
						st.ComputerID = qaComputer
						for id, b := range h.bindings {
							st.Bindings[id] = b
						}
					}
					delete(st.Bindings, qaBindingB)
					tmp := h.path + ".external-replacement"
					if err := os.WriteFile(tmp, replacement, 0600); err != nil {
						return false, err
					}
					if err := os.Rename(tmp, h.path); err != nil {
						return false, err
					}
					return changed, nil
				})
				qaCode(t, err, "state_corrupt")
				after, readErr := os.ReadFile(h.path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				if !bytes.Equal(after, replacement) {
					t.Fatalf("pending transaction overwrote newer/corrupt replacement: %q", after)
				}
			})
		}
	}
}
