//go:build !darwin && !linux

package auth

import (
	"context"
	"os"
)

func AcquireMutationLock(context.Context, string) (*os.File, error) {
	return nil, issue("config", unchanged())
}

func validateMutationOwner(*os.File, string) error { return issue("config", unchanged()) }
