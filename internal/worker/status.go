package worker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/rbeene/tempo/internal/activity"
	"github.com/rbeene/tempo/internal/privatefs"
)

func Observe(ctx context.Context, path string, base activity.WorkerStatus) activity.WorkerStatus {
	return observe(ctx, path, base, nil)
}

// control is the controller's already-locked predicted record for its own result.
// Other observations read the persisted record and keep any pending diagnostic.
func observe(ctx context.Context, path string, base activity.WorkerStatus, control *controlRecord) activity.WorkerStatus {
	base.State = "not_installed"
	base.Installed = nil
	base.InstanceMode = nil
	base.LastSuccess = nil
	base.FailureCategory = nil
	attention := func() activity.WorkerStatus {
		c := "state_corrupt"
		base.State = "needs_attention"
		base.FailureCategory = &c
		return base
	}
	if ctx.Err() != nil {
		return attention()
	}
	resolved, e := activity.ResolveStatePath(path)
	if e != nil {
		return attention()
	}
	path = resolved
	var r controlRecord
	exists := control != nil
	if exists {
		r = *control
	} else {
		raw, err := readPath(path+".worker-control.json", maxControlBytes)
		if err == nil {
			if decode(raw, &r) != nil || !validControl(r) {
				return attention()
			}
			exists = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return attention()
		}
	}
	if exists {
		installed := r.Owned != nil
		base.Installed = &installed
		if installed {
			base.State = "stopped"
			root, _, err := privatefs.OpenServiceDirectory(filepath.Dir(r.Owned.Path), false)
			var actual []byte
			if err == nil {
				actual, err = readPrivate(root, filepath.Base(r.Owned.Path), 65536)
				root.Close()
			}
			if err != nil || !bytes.Equal(actual, r.Owned.Bytes) {
				c := "revision_conflict"
				base.State = "needs_attention"
				base.FailureCategory = &c
			}
		}
		if r.Pending != nil {
			base.State = "needs_attention"
			c := "control_pending"
			base.FailureCategory = &c
		}
	}
	b, e := readPath(path+".worker-runtime.json", 65536)
	if errors.Is(e, os.ErrNotExist) {
		return base
	}
	if e != nil {
		return attention()
	}
	var runtime runtimeRecord
	if decode(b, &runtime) != nil || !validRuntime(runtime) {
		return attention()
	}
	base.LastSuccess = runtime.LastSuccess
	if base.State != "needs_attention" {
		base.FailureCategory = runtime.FailureCategory
	}
	root, _, e := privatefs.OpenDirectory(filepath.Dir(path), false)
	if e != nil {
		return attention()
	}
	defer root.Close()
	f, e := privatefs.OpenNoFollow(root, filepath.Base(path)+".worker.lock", os.O_RDONLY, 0)
	if errors.Is(e, os.ErrNotExist) {
		return base
	}
	if e != nil {
		return attention()
	}
	defer f.Close()
	fi, e := f.Stat()
	if e != nil || !privatefs.PrivateInfo(fi, false) {
		return attention()
	}
	free, e := privatefs.TryLock(f)
	if e != nil {
		return attention()
	}
	if free {
		privatefs.Unlock(f)
		return base
	}
	if runtime.InstanceMode != "foreground" && runtime.InstanceMode != "managed" {
		return attention()
	}
	if base.State != "needs_attention" {
		base.State = "running"
	}
	base.InstanceMode = &runtime.InstanceMode
	return base
}
