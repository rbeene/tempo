// Package worker owns optional synchronization scheduling and service controls.
package worker

import (
	"context"
	"time"

	"github.com/rbeene/tempo/internal/activity"
)

type SyncActions interface {
	SyncStatus(context.Context) (activity.SyncStatus, error)
	SyncNow(context.Context, activity.SyncRunInput, activity.SyncDependencies) (activity.SyncRun, error)
}
type ControlRequest struct {
	RequestID string `json:"request_id"`
	Confirmed bool   `json:"confirmed"`
}
type Result struct {
	ContractVersion int                   `json:"contract_version"`
	Status          activity.WorkerStatus `json:"status"`
}
type NotifyKind uint8

const (
	Wake NotifyKind = iota + 1
	Recheck
)

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}
type Command struct {
	Executable string
	Args       []string
}
type CommandResult struct {
	ExitCode       int
	Stdout, Stderr []byte
}
type CommandRunner interface {
	Run(context.Context, Command) (CommandResult, error)
}
type Options struct {
	StatePath, ConfigPath, Executable, ServiceDir string
	Platform                                      string
	InstanceMode                                  string
	UID                                           int
	Sync                                          SyncActions
	SyncDependencies                              activity.SyncDependencies
	Clock                                         Clock
	Runner                                        CommandRunner
	NewRequestID                                  func() (string, error)
	Jitter                                        func(time.Duration) time.Duration
}
type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Uncertain bool   `json:"uncertain"`
}

func (e *Error) Error() string { return e.Message }

// Runtime and control records have different lock owners and different files.
type runtimeRecord struct {
	Version         int                    `json:"version"`
	Pending         *activity.SyncRunInput `json:"pending"`
	LastSuccess     *time.Time             `json:"last_success"`
	FailureCategory *string                `json:"failure_category"`
	BackoffUntil    *time.Time             `json:"backoff_until"`
	BackoffNS       int64                  `json:"backoff_ns"`
	InstanceMode    string                 `json:"instance_mode"`
}
type definition struct {
	ServiceID string `json:"service_id"`
	Path      string `json:"path"`
	Digest    string `json:"digest"`
	Bytes     []byte `json:"bytes"`
}
type controlIntent struct {
	Action      string         `json:"action"`
	Request     ControlRequest `json:"request"`
	Fingerprint string         `json:"fingerprint"`
	Expected    definition     `json:"expected"`
}
type controlReceipt struct {
	Intent controlIntent `json:"intent"`
	Result *Result       `json:"result,omitempty"`
	Error  *Error        `json:"error,omitempty"`
}
type controlRecord struct {
	Version  int                       `json:"version"`
	Owned    *definition               `json:"owned"`
	Pending  *controlIntent            `json:"pending"`
	Receipts map[string]controlReceipt `json:"receipts"`
}
