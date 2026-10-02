package cli_test

import (
	"context"
	"github.com/rbeene/tempo/internal/auth"
	"github.com/rbeene/tempo/internal/cli"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// qaLegacyRun explicitly opts existing synthetic stores into the bounded service
// seam. Production never promotes a synchronous legacy store this way.
func qaLegacyRun(t *testing.T, ctx context.Context, args []string, in io.Reader, out, stderr io.Writer, d cli.Dependencies) int {
	t.Helper()
	if d.Auth == nil {
		lockPath := filepath.Join(t.TempDir(), "owner.lock")
		d.Auth = auth.NewService(auth.Options{ConfigPath: d.ConfigPath, LockPath: lockPath, Getenv: d.Getenv, NewProvider: d.NewProvider, PersistentAvailable: func() bool { return true }, Runner: auth.RunnerFunc(func(c context.Context, r auth.NativeRequest, l *os.File) (auth.NativeReply, error) {
			return auth.HandleNative(c, r, l, auth.HandlerDependencies{Store: d.Store, SaveConfig: d.SaveConfig, LockPath: lockPath})
		})})
	}
	return cli.Run(ctx, args, in, out, stderr, d)
}
