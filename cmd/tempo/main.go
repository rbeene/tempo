package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rbeene/tempo/internal/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	// One bounded command, including pagination and all read-only preflight calls.
	ctx, timeout := context.WithTimeout(ctx, 2*time.Minute)
	defer timeout()
	os.Exit(cli.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, cli.Dependencies{}))
}
