package main

import (
	"context"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"github.com/rbeene/tempo/internal/terminal"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() { os.Exit(run()) }
func run() int {
	if handled, code := auth.HelperMain(os.Args[1:]); handled {
		return code
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	done := make(chan struct{})
	stopped := make(chan struct{})
	defer func() { close(done); <-stopped }()
	go func() {
		defer close(stopped)
		select {
		case sig := <-signals:
			code := 130
			if sig == syscall.SIGTERM {
				code = 143
			}
			cancel(&terminal.ExitError{Code: code})
		case <-done:
		}
	}()
	if !cli.WorkerRunInvocation(os.Args[1:]) && !cli.InteractiveInvocation(os.Args[1:], os.Stdin, os.Stdout) {
		var end context.CancelFunc
		ctx, end = context.WithTimeout(ctx, 2*time.Minute)
		defer end()
	}
	code := cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{})

	return code
}
