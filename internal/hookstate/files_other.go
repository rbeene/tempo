//go:build !darwin && !linux

package hookstate

import "os"

func openArtifact(string) (*os.File, error) { return nil, problem("unsupported_contract") }
