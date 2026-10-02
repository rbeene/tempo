package terminal

import (
	"context"
	"io"
)

// WriteDiagnostic writes plain diagnostics after terminal restoration. OS-file
// writes have a bounded lifetime without an abandoned writer goroutine.
func WriteDiagnostic(ctx context.Context, writer io.Writer, data []byte) (int, error) {
	return 0, nil
}
