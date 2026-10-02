package activity

import "time"

// These exact flattened names and matched native call IDs are supported by the
// pinned Codex runtime. No suffix matching, target body or lineage inference.
func hostWaitTool(name string) bool {
	return name == "wait_agent" || name == "multi_agent_v1wait_agent"
}

func hostWaiting(turn *hostTurn) bool {
	waiting := false
	for _, tool := range turn.Tools {
		if tool.Phase != "pre" {
			continue
		}
		if !hostWaitTool(tool.Name) {
			return false
		}
		waiting = true
	}
	return waiting
}

// Waiting has no open work segment. A positive loss fences continuity without
// inventing a continuously working uncertainty interval over known idle time.
func fenceHostWait(a *Actor, result *HostReceipt, code string) bool {
	result.Disposition = "review_required"
	result.Ordering = "review_required"
	result.DiagnosticCode = code
	result.Actor = &a.Ref
	if a.Health != "continuous" {
		return false
	}
	a.Health = "stale"
	a.Revision = bump(a.Revision)
	return true
}

func captureReview(r HostReceipt) bool {
	return r.Actor != nil && (r.DiagnosticCode == "incomplete_wait" || r.DiagnosticCode == "source_loss_while_waiting")
}

// Preserve the waiting actor's own provenance when a different actor or a
// shared recovery operation observes the loss. No new timing record is created.
func retainHostWaitLoss(st *state, a *Actor, kind string, observed time.Time) bool {
	if a == nil || a.State != "wait_children" || a.Health != "continuous" {
		return false
	}
	var receipt HostReceipt
	for _, prior := range st.HostReceipts {
		r := prior.Result
		if r.Actor != nil && *r.Actor == a.Ref {
			n, _ := counter(r.SnapshotRevision)
			old, _ := counter(receipt.SnapshotRevision)
			if n > old || n == old && r.ID > receipt.ID {
				receipt = r
			}
		}
	}
	// Manual engine actors do not acquire fictitious native provenance.
	if receipt.ID == "" {
		return false
	}
	receipt.ID = newID()
	receipt.SnapshotRevision = bump(st.Revision)
	receipt.Kind, receipt.ToolID = kind, ""
	receipt.ObservedAt = observed
	fenceHostWait(a, &receipt, "source_loss_while_waiting")
	st.HostReceipts[hostHash([]string{"wait_loss", receipt.ID})] = hostReceiptRecord{Fingerprint: hostHash(receipt), Result: receipt}
	return true
}
