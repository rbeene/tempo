package activity

import "context"

// WorkerObserver decorates a local snapshot without another activity-store read.
type WorkerObserver func(context.Context, WorkerStatus) WorkerStatus

// ResolveStatePath shares the existing store path policy without creating state.
func ResolveStatePath(path string) (string, error) { return (&fileStore{path: path}).location() }

func (s *Service) observedWorker(ctx context.Context, base WorkerStatus) WorkerStatus {
	if s.observeWorker == nil {
		return base
	}
	observed := s.observeWorker(ctx, base)
	observed.QueuedCount = base.QueuedCount
	observed.SubmittingCount = base.SubmittingCount
	observed.UnknownCount = base.UnknownCount
	observed.SyncEnabled = base.SyncEnabled
	return observed
}
