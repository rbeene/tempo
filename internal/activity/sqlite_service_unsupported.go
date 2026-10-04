//go:build (!darwin && !linux) || (!amd64 && !arm64)

package activity

import "context"

func (s *Service) linkSQLite(context.Context, LinkInput, LinkDependencies) (BindingResult, error) {
	return BindingResult{}, failure("unsupported_contract")
}

func (s *Service) ingestSQLite(context.Context, Event) (EventResult, error) {
	return EventResult{}, failure("unsupported_contract")
}

func (s *Service) ingestHostSQLite(context.Context, HostEvent) (HostReceipt, error) {
	return HostReceipt{}, failure("unsupported_contract")
}

func (s *Service) statusSQLite(context.Context) (ActivitySnapshot, error) {
	return ActivitySnapshot{}, failure("unsupported_contract")
}

func (s *Service) syncConfigureSQLite(context.Context, SyncConfigureInput, SyncDependencies) (SyncConfigurationResult, error) {
	return SyncConfigurationResult{}, failure("unsupported_contract")
}

func (s *Service) syncControlSQLite(context.Context, string, bool) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) syncNowSQLite(context.Context, SyncRunInput, SyncDependencies) (SyncRun, error) {
	return SyncRun{}, failure("unsupported_contract")
}

func (s *Service) listBindingsSQLite(context.Context) (BindingList, error) {
	return BindingList{}, failure("unsupported_contract")
}

func (s *Service) showBindingSQLite(context.Context, ShowBindingInput) (BindingList, error) {
	return BindingList{}, failure("unsupported_contract")
}

func (s *Service) syncStatusSQLite(context.Context) (SyncStatus, error) {
	return SyncStatus{}, failure("unsupported_contract")
}

func (s *Service) unlinkSQLite(context.Context, UnlinkInput) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) repairBindingSQLite(context.Context, RepairBindingInput) (BindingResult, error) {
	return BindingResult{}, failure("unsupported_contract")
}

func (s *Service) observeSourceSQLite(context.Context, SourceObservation) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) observeClockSQLite(context.Context, ClockObservation) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) observeHostSQLite(context.Context, HostObservation) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) hostReceiptsSQLite(context.Context, HostReceiptFilter) (HostReceiptList, error) {
	return HostReceiptList{}, failure("unsupported_contract")
}

func (s *Service) syncReconcileSQLite(context.Context, SyncReconcileInput, SyncDependencies) (SyncRun, error) {
	return SyncRun{}, failure("unsupported_contract")
}

func (s *Service) syncResolveSQLite(context.Context, SyncResolveInput, SyncDependencies) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) reviewSQLite(context.Context, ReviewInput) (ReviewList, error) {
	return ReviewList{}, failure("unsupported_contract")
}

func (s *Service) previewSQLite(context.Context, RecoveryInput) (RecoveryPreview, error) {
	return RecoveryPreview{}, failure("unsupported_contract")
}

func (s *Service) resolveSQLite(context.Context, ResolveInput) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}

func (s *Service) interruptSQLite(context.Context, InterruptInput) (MutationResult, error) {
	return MutationResult{}, failure("unsupported_contract")
}
