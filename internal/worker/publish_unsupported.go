//go:build !darwin && !linux

package worker

import "os"

func publishExclusive(*os.Root, string, string) error { return issue("unsupported") }
