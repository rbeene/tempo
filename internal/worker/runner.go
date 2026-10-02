package worker

import (
	"bytes"
	"context"
	"os/exec"
	"sync"
	"time"
)

// ProcessRunner owns each child until exit and reaps it before returning.
// Output is aggregate-bounded and is discarded entirely on every failure.
type ProcessRunner struct{}
type commandOutput struct {
	mu             sync.Mutex
	stdout, stderr bytes.Buffer
	used           int
	cancel         context.CancelFunc
}
type commandStream struct {
	output *commandOutput
	stderr bool
}

func (w commandStream) Write(b []byte) (int, error) {
	o := w.output
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(b) > 65536-o.used {
		o.cancel()
		return 0, issue("manager")
	}
	o.used += len(b)
	if w.stderr {
		return o.stderr.Write(b)
	}
	return o.stdout.Write(b)
}
func (ProcessRunner) Run(ctx context.Context, c Command) (CommandResult, error) {
	if !validPath(c.Executable) {
		return CommandResult{}, issue("validation")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	output := &commandOutput{cancel: cancel}
	cmd := exec.CommandContext(ctx, c.Executable, c.Args...)
	cmd.Stdin = nil
	cmd.Stdout = commandStream{output: output}
	cmd.Stderr = commandStream{output: output, stderr: true}
	// A descendant retaining inherited pipes cannot extend the command deadline.
	cmd.WaitDelay = 100 * time.Millisecond
	if e := cmd.Run(); e != nil || ctx.Err() != nil {
		return CommandResult{}, issue("manager")
	}
	return CommandResult{Stdout: output.stdout.Bytes(), Stderr: output.stderr.Bytes()}, nil
}
