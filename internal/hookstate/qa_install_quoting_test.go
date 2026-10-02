package hookstate

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestQAInstallNativeCommandQuotesExecutableExactly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("supported host command grammar is POSIX shell")
	}
	for _, host := range []string{"codex", "claude"} {
		t.Run(host, func(t *testing.T) {
			f := qaNewInstallFixture(t)
			path := filepath.Join(f.root, "tempo space's $TEMPO_QA_QUOTING `printf harmless` ü")
			qaInstallWrite(t, path, []byte("#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\"\n"))
			if err := os.Chmod(path, 0700); err != nil {
				t.Fatal(err)
			}
			f.options.Executable = path
			qaInstallApply(t, New(f.options), f.intent(host, "project"), 51)
			groups := qaInstallNative(t, f.target(host, "project"))["SessionStart"]
			if len(groups) != 1 || len(groups[0].Hooks) != 1 {
				t.Fatal("expected one installed SessionStart command")
			}
			rendered := groups[0].Hooks[0].Command
			// Execute only the inert temporary capture file through the host's command
			// grammar. No native host, Tempo binary, credential or service is launched.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "/bin/sh", "-c", rendered)
			command.Env = append(os.Environ(), "TEMPO_QA_QUOTING=must-not-expand")
			output, err := command.Output()
			if err != nil {
				t.Fatalf("quoted inert executable failed: %v", err)
			}
			want := strings.Join([]string{path, "hook", host, "--input-stdin", ""}, "\n")
			if string(output) != want {
				t.Fatalf("native shell changed executable/arguments: %q want %q", output, want)
			}
		})
	}
}
func TestQAInstallRejectsExecutableControlCharacters(t *testing.T) {
	f := qaNewInstallFixture(t)
	f.options.Executable = filepath.Join(f.root, "tempo\nsecond command")
	qaInstallWrite(t, f.options.Executable, []byte("inert"))
	before := qaInstallTree(t, f.root)
	_, err := New(f.options).PreviewInstall(context.Background(), f.intent("codex", "project"))
	qaInstallError(t, err, "validation")
	qaInstallAssertUnchanged(t, f, before)
}
