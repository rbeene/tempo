package terminal

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
)

// WriteDiagnostic writes plain diagnostics after terminal restoration. OS-file
// writes have a bounded lifetime without an abandoned writer goroutine.
func WriteDiagnostic(ctx context.Context, writer io.Writer, data []byte) (int, error) {
	if ctx.Err() != nil {
		return 0, context.Cause(ctx)
	}
	file, isFile := writer.(*os.File)
	if !isFile {
		return writer.Write(data)
	}
	owned, restore, err := duplicateTerminal(file)
	if err != nil {
		return 0, &ExitError{Code: 1}
	}
	defer restore()
	deadline := time.Now().Add(250 * time.Millisecond)
	if earlier, ok := ctx.Deadline(); ok && earlier.Before(deadline) {
		deadline = earlier
	}
	if err := owned.SetWriteDeadline(deadline); err != nil {
		// Regular files have no poll deadline and do not wait for a reader.
		info, statErr := owned.Stat()
		if !errors.Is(err, os.ErrNoDeadline) || statErr != nil || !info.Mode().IsRegular() {
			return 0, &ExitError{Code: 1}
		}
	}
	if ctx.Err() != nil {
		return 0, context.Cause(ctx)
	}
	n, err := owned.Write(data)
	if err != nil || n != len(data) {
		if ctx.Err() != nil {
			return n, context.Cause(ctx)
		}
		return n, &ExitError{Code: 1}
	}
	return n, nil
}
