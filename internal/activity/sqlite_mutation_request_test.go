//go:build (darwin || linux) && (amd64 || arm64)

package activity

// Independent tests first for the three local request-unit APIs. No Service
// switch, remote replay policy, final graph acceptance or migration is claimed.

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

func TestSQLiteMutationRequestActualTerminalFamiliesRoundTripAndCharge(t *testing.T) {
	_, recovery, _, _ := ueQALegacy(t, "resolved")
	for _, source := range []*state{mqQABindings(t), mqQASyncComplete(t), mqQAComplete(t, recovery, true)} {
		for _, id := range mqQAIDs(source) {
			row := mqQAFrom(source, id)
			t.Run(row.Value.Operation+"/"+id, func(t *testing.T) {
				original := mqQAClone(row)
				charge, err := sqliteMutationRequestCharge(source.ComputerID, row)
				if err != nil || charge != mqQAExpectedCharge(t, row) || !reflect.DeepEqual(original, row) {
					t.Fatal("pure charge differs or mutated caller", err)
				}
				f, meta := mqQASave(t, source, row)
				mqQAReopen(t, f, meta, row)
				c, tx := interopOpen(t, f, false, sqliteio.Read)
				missing, found, err := sqliteReadMutationRequestLocal(tx, meta.ComputerID, mqQANewID, meta.Revision)
				if err != nil || found || !reflect.DeepEqual(missing, sqliteMutationRequestRow{}) {
					t.Fatal("absent request created a usable row", err)
				}
				got := mqQARead(t, tx, meta.ComputerID, row.ID, meta.Revision)
				if got.payload == "" {
					t.Fatal("reader discarded materialized old payload")
				}
				if row.Value.Operation == "activity.resolve" {
					proof, found, err := sqliteReadResolveRequestProof(tx, row.ID, meta.Revision)
					if err != nil || !found || !reflect.DeepEqual(proof.Result, *got.Value.MutationResult) {
						t.Fatal("selected proof interoperability", err)
					}
				} else {
					proof, found, err := sqliteReadResolveRequestProof(tx, row.ID, meta.Revision)
					mqQACode(t, err, "state_corrupt")
					if found || !reflect.DeepEqual(proof, sqliteResolveRequestRow{}) {
						t.Fatal("narrow proof accepted wrong family")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
}

func TestSQLiteMutationRequestNativePendingPhasesAndOfflineTerminalization(t *testing.T) {
	for _, shape := range []string{"run", "reconcile-no-effect", "reconcile-effect", "resolve"} {
		t.Run(shape, func(t *testing.T) {
			beforeSource, finalSource, id := mqQAPhase(t, shape)
			row, final := mqQAFrom(beforeSource, id), mqQAFrom(finalSource, id)
			if row.Value.PendingSync == nil || final.Value.PendingSync != nil {
				t.Fatal("actual source phase fixture missing")
			}
			f, meta := mqQASave(t, beforeSource, row)
			mqQAReopen(t, f, meta, row)
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			before := mqQARead(t, tx, meta.ComputerID, id, meta.Revision)
			owned := mqQARead(t, tx, meta.ComputerID, id, meta.Revision)
			owned.Value.PendingSync.RootIDs[0] = mqQANewID
			if owned.Value.PendingSync.Run != nil {
				owned.Value.PendingSync.Run.Limit = 99
			}
			if owned.Value.PendingSync.Reconcile != nil {
				owned.Value.PendingSync.Reconcile.OutboxID = mqQANewID
			}
			if owned.Value.PendingSync.Resolve != nil {
				owned.Value.PendingSync.Resolve.IfRevision = "caller-mutated"
			}
			if !reflect.DeepEqual(before, mqQARead(t, tx, meta.ComputerID, id, meta.Revision)) {
				t.Fatal("pending reader shared roots or input pointers")
			}
			original, originalFinal := mqQAClone(before), mqQAClone(final)
			delta, err := sqliteWriteMutationRequest(tx, meta.ComputerID, &before, final)
			if err != nil || delta != mqQAExpectedCharge(t, final)-mqQAExpectedCharge(t, before) {
				t.Fatal("pending terminal transition/charge", delta, err)
			}
			if !reflect.DeepEqual(original, before) || !reflect.DeepEqual(originalFinal, final) {
				t.Fatal("phase writer mutated old/new DTO")
			}
			_, charge := mqQAUnit(t, tx, id)
			if charge != mqQAExpectedCharge(t, final) {
				t.Fatal("terminalization retained child charge")
			}
			if interopCount(t, tx, "SELECT count(*) FROM pending_sync") != 0 || interopCount(t, tx, "SELECT count(*) FROM pending_sync_roots") != 0 {
				t.Fatal("terminalization orphaned reservation children")
			}
			// The local API does not repair outbox/attempt ownership. Compose only
			// the actual outbox run-owner change from the native final fixture.
			for _, outbox := range finalSource.Outbox {
				interopDone(t, tx, "UPDATE outbox SET run_request_id=? WHERE id=?", asQAOptionalText(t, outbox.RunRequestID), sqliteio.Text(outbox.ID))
			}
			after := metaQANext(t, meta)
			after.Revision = finalSource.Revision
			after.LogicalBytes += delta
			// Metadata permits same/next, not arbitrary source-history jumps. The
			// native recovery performs one final operation in these fixtures.
			if after.Revision != meta.Revision && after.Revision != bump(meta.Revision) {
				t.Fatal("native phase fixture consumed unexpected revisions")
			}
			// The run_owner TEXT->NULL fixture edit is independently charged.
			for _, o := range beforeSource.Outbox {
				if o.RunRequestID != nil && finalSource.Outbox[o.Interval.ID].RunRequestID == nil {
					after.LogicalBytes -= 8 + int64(len(*o.RunRequestID))
				}
			}
			if err := sqliteUpdateMeta(tx, meta, after); err != nil {
				t.Fatal(err)
			}
			if err := tx.CheckForeignKeys(); err != nil {
				t.Fatal(err)
			}
			interopCommit(t, tx)
			interopClose(t, c)
			mqQAReopen(t, f, after, final)
			c, tx = interopOpen(t, f, false, sqliteio.Write)
			terminal := mqQARead(t, tx, after.ComputerID, id, after.Revision)
			next := mqQAClone(terminal)
			next.payload = ""
			delta, err = sqliteWriteMutationRequest(tx, after.ComputerID, &terminal, next)
			mqQACode(t, err, "validation")
			if delta != 0 {
				t.Fatal("terminal rewrite charged")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			mqQAReopen(t, f, after, final)
		})
	}
}

func TestSQLiteMutationRequestStoredPolicyOraclesDoNotBecomeIngressRules(t *testing.T) {
	syncSource := mqQASyncComplete(t)
	cases := []struct {
		name   string
		source *state
		row    sqliteMutationRequestRow
	}{}
	add := func(name string, source *state, row sqliteMutationRequestRow) {
		copy := asQAJSON(t, source)
		copy.Requests[row.ID] = row.Value
		copy = mqQAComplete(t, copy, true)
		cases = append(cases, struct {
			name   string
			source *state
			row    sqliteMutationRequestRow
		}{name, copy, mqQAFrom(copy, row.ID)})
	}
	for _, op := range []string{"sync.pause", "sync.resume", "sync.resolve"} {
		r := mqQAReID(mqQAFrom(syncSource, qaSyncID(711)), mqQANewID)
		r.Value.Operation = op
		root := syncSource.Intervals[0].ID
		r.Value.MutationResult.Changed = false
		r.Value.MutationResult.AffectedIDs = []string{root, root}
		r.Value.MutationResult.EntityRevision = mqQAString("opaque-noncanonical-01")
		add(op+"-weak-retained-mutation", syncSource, r)
	}
	for _, op := range []string{"sync.resolve", "sync.reconcile"} {
		r := sqliteMutationRequestRow{ID: mqQANewID, Value: mutationRequest{Operation: op, Fingerprint: strings.Repeat("b", 64), Error: &Error{Code: "local_write_unknown", Message: "owned noncanonical historical text", Retryable: true, Uncertain: true}}}
		add(op+"-retained-error", syncSource, r)
	}
	r := mqQAReID(mqQAFrom(syncSource, qaSyncID(1)), mqQANewID)
	r.Value.SyncConfigurationResult.Configuration.Clock = mqQAString("")
	add("duration-clock-present-empty", syncSource, r)
	r = mqQAReID(mqQAFrom(syncSource, qaSyncID(710)), mqQANewID)
	root := r.Value.SyncRun.AttemptedIDs[0]
	r.Value.SyncRun.ResolvedIDs = []string{root}
	r.Value.SyncRun.BlockedIDs = []string{root}
	r.Value.SyncRun.RemainingCount = 17
	add("cross-array-overlap-and-independent-remaining", syncSource, r)
	_, recovery, _, _ := ueQALegacy(t, "resolved")
	for _, op := range []string{"activity.interrupt", "activity.observe_source", "activity.observe_clock", "activity.observe_host"} {
		r := mqQAReID(mqQAFrom(recovery, qaRecoveryRequest), mqQANewID)
		r.Value.Operation = op
		if op != "activity.interrupt" {
			r.Value.MutationResult.Changed = false
			r.Value.MutationResult.AffectedIDs = []string{}
			r.Value.MutationResult.EntityRevision = nil
		}
		add(op+"-complete-stored-rule", recovery, r)
	}
	for _, op := range []string{"activity.resolve", "activity.interrupt", "activity.observe_source", "activity.observe_clock", "activity.observe_host"} {
		for _, code := range []string{"clock_unavailable", "clock_conflict", "recovery_bounds", "attribution_conflict"} {
			r := sqliteMutationRequestRow{ID: mqQANewID, Value: mutationRequest{Operation: op, Fingerprint: strings.Repeat("c", 64), Error: failure(code)}}
			add(op+"/"+code, recovery, r)
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { f, m := mqQASave(t, tc.source, tc.row); mqQAReopen(t, f, m, tc.row) })
	}
	// The permissive pending Resolve fields are proved with its full native
	// uncertain outbox graph, not a partial-state call to validSyncReceipt.
	t.Run("pending-resolve-opaque-fields", func(t *testing.T) {
		st, _, id := mqQAPhase(t, "resolve")
		row := mqQAFrom(st, id)
		p := row.Value.PendingSync
		p.Resolve.EntryID, p.Resolve.IfRevision, p.Resolve.RetryRejected = "", "01", true
		p.EffectCommitted = true
		row.Value.Fingerprint = mutationFingerprint(row.Value.Operation, *p.Resolve)
		st.Requests[id] = row.Value
		st = mqQAComplete(t, st, true)
		f, m := mqQASave(t, st, mqQAFrom(st, id))
		mqQAReopen(t, f, m, mqQAFrom(st, id))
	})
}

func TestSQLiteMutationRequestInvalidInputsRefuseWithoutCollisionOrMutation(t *testing.T) {
	bindings, syncSource := mqQABindings(t), mqQASyncComplete(t)
	binding := mqQAReID(mqQAFrom(bindings, qaLinkRequest), mqQANewID)
	config := mqQAReID(mqQAFrom(syncSource, qaSyncID(1)), mqQANewID)
	run := mqQAReID(mqQAFrom(syncSource, qaSyncID(710)), mqQANewID)
	mutation := mqQAReID(mqQAFrom(bindings, qaSyncID(702)), mqQANewID)
	cases := []struct {
		name   string
		source *state
		row    sqliteMutationRequestRow
		alter  func(*sqliteMutationRequestRow)
	}{
		{"request-id", bindings, binding, func(r *sqliteMutationRequestRow) { r.ID = "private-invalid-id" }},
		{"fingerprint-short", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.Fingerprint = "aa" }},
		{"fingerprint-uppercase", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.Fingerprint = strings.Repeat("A", 64) }},
		{"fingerprint-nonhex", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.Fingerprint = strings.Repeat("z", 64) }},
		{"unknown-operation", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.Operation = "private.unsupported" }},
		{"no-outcome", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult = nil }},
		{"two-outcomes", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.Error = failure("clock_unavailable") }},
		{"wrong-family", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.Operation = "sync.now" }},
		{"after-old-payload", bindings, binding, func(r *sqliteMutationRequestRow) { _, r.payload = mqQAPayload(t, r.Value) }},
		{"result-request-id", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.RequestID = qaSyncID(999) }},
		{"contract-version", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.ContractVersion = 2 }},
		{"zero-snapshot", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.SnapshotRevision = "0" }},
		{"noncanonical-snapshot", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.SnapshotRevision = "01" }},
		{"overflow-snapshot", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.SnapshotRevision = "18446744073709551616" }},
		{"binding-relative-location", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.Binding.Locator = "private/relative" }},
		{"binding-nil-actors", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.Binding.AttachedActors = nil }},
		{"binding-attribution", bindings, binding, func(r *sqliteMutationRequestRow) { r.Value.BindingResult.Binding.Attribution.AccountID = "0" }},
		{"unlink-false", bindings, mutation, func(r *sqliteMutationRequestRow) { r.Value.MutationResult.Changed = false }},
		{"unlink-nil-entity", bindings, mutation, func(r *sqliteMutationRequestRow) { r.Value.MutationResult.EntityRevision = nil }},
		{"unlink-empty-ids", bindings, mutation, func(r *sqliteMutationRequestRow) { r.Value.MutationResult.AffectedIDs = []string{} }},
		{"config-undeclared", syncSource, config, func(r *sqliteMutationRequestRow) { r.Value.SyncConfigurationResult.Configuration.Declared = false }},
		{"config-bad-policy-version", syncSource, config, func(r *sqliteMutationRequestRow) {
			r.Value.SyncConfigurationResult.Configuration.PolicyVersion = "other"
		}},
		{"config-zero-time", syncSource, config, func(r *sqliteMutationRequestRow) {
			r.Value.SyncConfigurationResult.Configuration.DeclaredAt = time.Time{}
		}},
		{"run-nil-array", syncSource, run, func(r *sqliteMutationRequestRow) { r.Value.SyncRun.BlockedIDs = nil }},
		{"run-duplicate-within-array", syncSource, run, func(r *sqliteMutationRequestRow) {
			r.Value.SyncRun.AttemptedIDs = append(r.Value.SyncRun.AttemptedIDs, r.Value.SyncRun.AttemptedIDs[0])
		}},
		{"run-negative-remaining", syncSource, run, func(r *sqliteMutationRequestRow) { r.Value.SyncRun.RemainingCount = -1 }},
		{"run-state", syncSource, run, func(r *sqliteMutationRequestRow) { r.Value.SyncRun.State = "pending" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := mqQAClone(tc.row)
			tc.alter(&row)
			original := mqQAClone(row)
			// Pure charge accepts a checked materialized before payload; only the
			// after-writer rejects that field. All other input faults reject both.
			if tc.name != "after-old-payload" {
				n, err := sqliteMutationRequestCharge(tc.source.ComputerID, row)
				mqQACode(t, err, "validation")
				if n != 0 {
					t.Fatal("invalid charge returned usable bytes")
				}
			}
			f, c, tx, _ := mqQAStart(t, tc.source)
			_ = f
			if interopCount(t, tx, "SELECT count(*) FROM requests") != 0 {
				t.Fatal("validation fixture has a colliding request")
			}
			delta, err := sqliteWriteMutationRequest(tx, tc.source.ComputerID, nil, row)
			mqQACode(t, err, "validation")
			if delta != 0 || !reflect.DeepEqual(row, original) || interopCount(t, tx, "SELECT count(*) FROM requests") != 0 {
				t.Fatal("invalid input mutated caller or SQL request")
			}
			interopSafeError(t, err, "private-invalid-id", "private/relative", "private.unsupported")
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	t.Run("unencodable-config-time", func(t *testing.T) {
		row := mqQAClone(config)
		row.Value.SyncConfigurationResult.Configuration.DeclaredAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		if !validSyncConfiguration(row.Value.SyncConfigurationResult.Configuration) {
			t.Fatal("raw configuration time premise false")
		}
		if _, err := row.Value.SyncConfigurationResult.Configuration.DeclaredAt.MarshalJSON(); err == nil {
			t.Fatal("actual time JSON accepts year10000")
		}
		n, err := sqliteMutationRequestCharge(syncSource.ComputerID, row)
		mqQACode(t, err, "validation")
		if n != 0 {
			t.Fatal("unencodable charge")
		}
		_, c, tx, _ := mqQAStart(t, syncSource)
		n, err = sqliteWriteMutationRequest(tx, syncSource.ComputerID, nil, row)
		mqQACode(t, err, "validation")
		if n != 0 || interopCount(t, tx, "SELECT count(*) FROM requests") != 0 {
			t.Fatal("unencodable request staged")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteMutationRequestStrictSelectedJSONKindsAndLateRows(t *testing.T) {
	st := mqQABindings(t)
	row := mqQAFrom(st, qaLinkRequest)
	kind, payload := mqQAPayload(t, row.Value)
	cases := []struct {
		name      string
		column    string
		value     sqliteio.Value
		duplicate bool
	}{
		{"invalid-utf8", "payload", sqliteio.Text(string([]byte{0xff})), false},
		{"null-result", "payload", sqliteio.Text("null"), false},
		{"array-result", "payload", sqliteio.Text("[]"), false},
		{"trailing-value", "payload", sqliteio.Text(payload + " {}"), false},
		{"duplicate-top", "payload", sqliteio.Text(strings.Replace(payload, "{", "{\"contract_version\":1,", 1)), false},
		{"duplicate-nested", "payload", sqliteio.Text(strings.Replace(payload, "\"binding\":{", "\"binding\":{\"id\":\""+row.Value.BindingResult.Binding.ID+"\",", 1)), false},
		{"unknown-field", "payload", sqliteio.Text(strings.Replace(payload, "{", "{\"private_unknown\":1,", 1)), false},
		{"wrong-case", "payload", sqliteio.Text(strings.Replace(payload, "\"contract_version\"", "\"Contract_Version\"", 1)), false},
		{"wrong-bool-kind", "payload", sqliteio.Text(strings.Replace(payload, "\"changed\":true", "\"changed\":1", 1)), false},
		{"wrong-request-id", "request_id", sqliteio.Text(qaSyncID(999)), false},
		{"wrong-operation", "operation", sqliteio.Text("sync.now"), false},
		{"bad-fingerprint", "fingerprint", sqliteio.Text(strings.Repeat("z", 64)), false},
		{"wrong-kind", "outcome_kind", sqliteio.Text("sync_run"), false},
		{"blob-payload", "payload", sqliteio.Blob([]byte(payload)), false},
		{"integer-operation", "operation", sqliteio.Integer(7), false},
		{"null-fingerprint", "fingerprint", sqliteio.Null(), false},
		{"second-selected-row", "payload", sqliteio.Text(payload), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, c, tx, _ := mqQAStart(t, st)
			mqQARelaxRequests(t, tx)
			mqQARawInsert(t, tx, row)
			if tc.column == "request_id" { // Keep selected PK and corrupt the embedded result ID.
				p := strings.Replace(payload, row.ID, qaSyncID(999), 1)
				interopDone(t, tx, "UPDATE requests SET payload=?", sqliteio.Text(p))
			} else {
				interopDone(t, tx, "UPDATE requests SET "+tc.column+"=?", tc.value)
			}
			if tc.duplicate {
				interopDone(t, tx, "INSERT INTO requests SELECT * FROM requests")
			}
			got, found, err := sqliteReadMutationRequestLocal(tx, st.ComputerID, row.ID, st.Revision)
			mqQAZeroRead(t, got, found, err, "state_corrupt")
			interopSafeError(t, err, payload, "private_unknown")
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
	_ = kind
	// Unselected corrupt history must not be decoded by a PK lookup.
	t.Run("selected-only", func(t *testing.T) {
		_, c, tx, _ := mqQAStart(t, st)
		mqQARawInsert(t, tx, row)
		interopDone(t, tx, "INSERT INTO requests("+mqQARequests+") VALUES(?,?,?,?,?)", sqliteio.Text(mqQANewID), sqliteio.Text("bindings.link"), sqliteio.Text(strings.Repeat("a", 64)), sqliteio.Text("binding_result"), sqliteio.Text("{"))
		got := mqQARead(t, tx, st.ComputerID, row.ID, st.Revision)
		if !reflect.DeepEqual(got, mqQAMaterialize(t, row)) {
			t.Fatal("unselected history changed selected result")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteMutationRequestErrorDetailsAndCanonicalRecovery(t *testing.T) {
	st := mqQABindings(t)
	for _, op := range []string{"activity.observe_clock", "sync.reconcile"} {
		base := failure("local_write_unknown")
		if op == "activity.observe_clock" {
			base = failure("clock_conflict")
		}
		row := sqliteMutationRequestRow{ID: mqQANewID, Value: mutationRequest{Operation: op, Fingerprint: strings.Repeat("a", 64), Error: base}}
		for _, suffix := range []string{"", ",\"details\":null", ",\"details\":{}", ",\"details\":[]", ",\"details\":\"private\""} {
			t.Run(op+"/"+suffix, func(t *testing.T) {
				_, c, tx, _ := mqQAStart(t, st)
				_, p := mqQAPayload(t, row.Value)
				p = strings.TrimSuffix(p, "}") + suffix + "}"
				r := mqQAClone(row)
				r.payload = p
				mqQARawInsert(t, tx, r)
				got, found, err := sqliteReadMutationRequestLocal(tx, st.ComputerID, row.ID, st.Revision)
				if suffix == "" || suffix == ",\"details\":null" {
					if err != nil || !found || got.Value.Error.Details != nil {
						t.Fatal("nil Details policy", err)
					}
				} else {
					mqQAZeroRead(t, got, found, err, "state_corrupt")
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
		for _, name := range []string{"message", "retryable", "details-overlay", "wrong-code", "false-uncertain"} {
			t.Run(op+"/input-"+name, func(t *testing.T) {
				r := mqQAClone(row)
				switch name {
				case "message":
					r.Value.Error.Message = "owned different message"
				case "retryable":
					r.Value.Error.Retryable = !r.Value.Error.Retryable
				case "details-overlay":
					r.Value.Error = requestError(r.Value.Error, r.ID).(*Error)
					if op == "activity.observe_clock" {
						r.Value.Error.Details = map[string]any{"request_id": r.ID}
					}
				case "wrong-code":
					r.Value.Error.Code = "network"
				case "false-uncertain":
					r.Value.Error.Uncertain = !r.Value.Error.Uncertain
				}
				_, c, tx, _ := mqQAStart(t, st)
				delta, err := sqliteWriteMutationRequest(tx, st.ComputerID, nil, r)
				valid := op == "sync.reconcile" && (name == "message" || name == "retryable")
				if valid {
					if err != nil || delta != mqQAExpectedCharge(t, r) {
						t.Fatal("stored sync error overconstrained", err)
					}
				} else {
					mqQACode(t, err, "validation")
					if delta != 0 {
						t.Fatal("invalid error charged")
					}
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
}

func TestSQLiteMutationRequestMaterializedRawPayloadCASAndNegativeCharges(t *testing.T) {
	st, _, id := mqQAPhase(t, "reconcile-no-effect")
	row := mqQAFrom(st, id)
	f, m := mqQASave(t, st, row)
	c, tx := interopOpen(t, f, false, sqliteio.Write)
	oldPayload := mqQANoncanonical(t, row.Value)
	_, canonical := mqQAPayload(t, row.Value)
	if oldPayload == canonical || !strictJSON([]byte(oldPayload)) {
		t.Fatal("noncanonical fixture premise")
	}
	interopDone(t, tx, "UPDATE requests SET payload=? WHERE request_id=?", sqliteio.Text(oldPayload), sqliteio.Text(id))
	before := mqQARead(t, tx, m.ComputerID, id, m.Revision)
	if before.payload != oldPayload || !reflect.DeepEqual(before.Value, row.Value) {
		t.Fatal("reader normalized actual payload")
	}
	charge, err := sqliteMutationRequestCharge(m.ComputerID, before)
	if err != nil || charge != mqQAExpectedCharge(t, before) {
		t.Fatal("old payload raw byte charge", err)
	}
	bad := mqQAClone(before)
	bad.Value.PendingSync.EffectCommitted = true
	if n, err := sqliteMutationRequestCharge(m.ComputerID, bad); n != 0 || bgQAFixtureCode(err) != "validation" {
		t.Fatal("inconsistent typed/raw before observation accepted")
	}
	after := mqQAClone(before)
	after.payload = ""
	after.Value.PendingSync.EffectCommitted = true
	delta, err := sqliteWriteMutationRequest(tx, m.ComputerID, &before, after)
	if err != nil || delta != mqQAExpectedCharge(t, after)-charge {
		t.Fatal("exact raw-old CAS or phase charge", delta, err)
	}
	got := mqQARead(t, tx, m.ComputerID, id, m.Revision)
	if got.payload == oldPayload || !got.Value.PendingSync.EffectCommitted {
		t.Fatal("phase did not materialize owned new DTO")
	}
	// Explicit rollback preserves the previously committed canonical baseline.
	interopRollback(t, tx)
	interopClose(t, c)
	mqQAReopen(t, f, m, row)
	t.Run("stale-valid-whitespace-before", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		before := mqQARead(t, tx, m.ComputerID, id, m.Revision)
		interopDone(t, tx, "UPDATE requests SET payload=? WHERE request_id=?", sqliteio.Text(oldPayload), sqliteio.Text(id))
		snapshot, _ := mqQAUnit(t, tx, id)
		after := mqQAClone(before)
		after.payload = ""
		after.Value.PendingSync.EffectCommitted = true
		n, err := sqliteWriteMutationRequest(tx, m.ComputerID, &before, after)
		mqQACode(t, err, "state_corrupt")
		if n != 0 {
			t.Fatal("stale raw text returned delta")
		}
		unchanged, _ := mqQAUnit(t, tx, id)
		if !reflect.DeepEqual(snapshot, unchanged) {
			t.Fatal("stale before rewrote selected unit")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteMutationRequestPendingProjectionAndFullBeforeDisagreement(t *testing.T) {
	st, _, id := mqQAPhase(t, "resolve")
	row := mqQAFrom(st, id)
	f, m := mqQASave(t, st, row)
	cases := []struct {
		name, query string
		values      []sqliteio.Value
		relax       bool
	}{
		{"request-operation", "UPDATE requests SET operation=? WHERE request_id=?", []sqliteio.Value{sqliteio.Text("sync.reconcile"), sqliteio.Text(id)}, false},
		{"request-fingerprint", "UPDATE requests SET fingerprint=? WHERE request_id=?", []sqliteio.Value{sqliteio.Text(strings.Repeat("f", 64)), sqliteio.Text(id)}, false},
		{"request-kind", "UPDATE requests SET outcome_kind=? WHERE request_id=?", []sqliteio.Value{sqliteio.Text("error"), sqliteio.Text(id)}, false},
		{"missing-request", "DELETE FROM requests WHERE request_id=?", []sqliteio.Value{sqliteio.Text(id)}, false},
		{"pending-owner", "UPDATE pending_sync SET request_id=?", []sqliteio.Value{sqliteio.Text(mqQANewID)}, false},
		{"singleton", "UPDATE pending_sync SET singleton=2", nil, true},
		{"kind", "UPDATE pending_sync SET kind='run'", nil, true},
		{"effect", "UPDATE pending_sync SET effect_committed=1", nil, false},
		{"snapshot", "UPDATE pending_sync SET snapshot_revision=?", []sqliteio.Value{interopCounter(t, bump(m.Revision))}, false},
		{"unused-limit-null", "UPDATE pending_sync SET limit_count=20", nil, true},
		{"outbox", "UPDATE pending_sync SET outbox_id=?", []sqliteio.Value{sqliteio.Text(mqQANewID)}, false},
		{"entry", "UPDATE pending_sync SET entry_id='902'", nil, false},
		{"if-revision", "UPDATE pending_sync SET if_revision='opaque'", nil, false},
		{"retry", "UPDATE pending_sync SET retry_rejected=1", nil, false},
		{"confirmed", "UPDATE pending_sync SET confirmed=0", nil, true},
		{"snapshot-wrong-kind", "UPDATE pending_sync SET snapshot_revision='1'", nil, true},
		{"singleton-wrong-kind", "UPDATE pending_sync SET singleton='1'", nil, true},
		{"effect-wrong-kind", "UPDATE pending_sync SET effect_committed='0'", nil, true},
		{"entry-partial-null", "UPDATE pending_sync SET entry_id=NULL", nil, true},
		{"if-revision-partial-null", "UPDATE pending_sync SET if_revision=NULL", nil, true},
		{"retry-partial-null", "UPDATE pending_sync SET retry_rejected=NULL", nil, true},
		{"confirmed-partial-null", "UPDATE pending_sync SET confirmed=NULL", nil, true},
		{"root-owner", "UPDATE pending_sync_roots SET request_id=?", []sqliteio.Value{sqliteio.Text(mqQANewID)}, false},
		{"root-ordinal", "UPDATE pending_sync_roots SET ordinal=2", nil, false},
		{"root-target", "UPDATE pending_sync_roots SET outbox_id=?", []sqliteio.Value{sqliteio.Text(mqQANewID)}, false},
		{"missing-root", "DELETE FROM pending_sync_roots", nil, false},
		{"missing-pending", "DELETE FROM pending_sync", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			before := mqQARead(t, tx, m.ComputerID, id, m.Revision)
			if tc.relax {
				interopDone(t, tx, "ALTER TABLE pending_sync RENAME TO qa_original_pending")
				interopDone(t, tx, "CREATE TABLE pending_sync(request_id,singleton,kind,effect_committed,snapshot_revision,limit_count,outbox_id,entry_id,if_revision,retry_rejected,confirmed)")
				interopDone(t, tx, "INSERT INTO pending_sync SELECT "+mqQAPending+" FROM qa_original_pending")
			}
			interopDone(t, tx, tc.query, tc.values...)
			changed, _ := mqQAUnit(t, tx, id)
			after := mqQAClone(before)
			after.payload = ""
			n, err := sqliteWriteMutationRequest(tx, m.ComputerID, &before, after)
			mqQACode(t, err, "state_corrupt")
			if n != 0 {
				t.Fatal("stale unit returned charge")
			}
			unchanged, _ := mqQAUnit(t, tx, id)
			if !reflect.DeepEqual(changed, unchanged) {
				t.Fatal("before mismatch mutated selected rows")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			mqQAReopen(t, f, m, row)
		})
	}
}

func TestSQLiteMutationRequestPendingTransitionAndInputMatrix(t *testing.T) {
	for _, shape := range []string{"run", "reconcile-no-effect", "resolve"} {
		t.Run(shape, func(t *testing.T) {
			st, finalSource, id := mqQAPhase(t, shape)
			row := mqQAFrom(st, id)
			f, m := mqQASave(t, st, row)
			for _, axis := range []string{"nil-roots", "duplicate-root", "invalid-root", "nil-input", "two-inputs", "request-id", "snapshot-zero", "wrong-fingerprint", "limit-zero", "limit-101", "too-many-roots", "resolve-unconfirmed", "resolve-root-mismatch"} {
				if shape == "resolve" && (strings.HasPrefix(axis, "limit-") || axis == "too-many-roots") || shape != "resolve" && strings.HasPrefix(axis, "resolve-") {
					continue
				}
				t.Run("input-"+axis, func(t *testing.T) {
					r := mqQAReID(row, mqQANewID)
					p := r.Value.PendingSync
					switch axis {
					case "nil-roots":
						p.RootIDs = nil
					case "duplicate-root":
						p.RootIDs = append(p.RootIDs, p.RootIDs[0])
					case "invalid-root":
						p.RootIDs[0] = "bad-root"
					case "nil-input":
						p.Run = nil
						p.Reconcile = nil
						p.Resolve = nil
					case "two-inputs":
						if p.Run != nil {
							p.Reconcile = &SyncReconcileInput{RequestID: r.ID, Limit: 20}
						} else {
							p.Run = &SyncRunInput{RequestID: r.ID, Limit: 20}
						}
					case "request-id":
						if p.Run != nil {
							p.Run.RequestID = id
						}
						if p.Reconcile != nil {
							p.Reconcile.RequestID = id
						}
						if p.Resolve != nil {
							p.Resolve.RequestID = id
						}
					case "snapshot-zero":
						p.SnapshotRevision = "0"
					case "wrong-fingerprint":
						r.Value.Fingerprint = strings.Repeat("0", 64)
					case "limit-zero", "limit-101":
						n := 0
						if axis == "limit-101" {
							n = 101
						}
						if p.Run != nil {
							p.Run.Limit = n
						}
						if p.Reconcile != nil {
							p.Reconcile.Limit = n
						}
					case "too-many-roots":
						p.RootIDs = []string{row.Value.PendingSync.RootIDs[0], qaSyncID(999)}
						if p.Run != nil {
							p.Run.Limit = 1
						}
						if p.Reconcile != nil {
							p.Reconcile.Limit = 1
						}
					case "resolve-unconfirmed":
						p.Resolve.Confirmed = false
					case "resolve-root-mismatch":
						p.RootIDs[0] = qaSyncID(999)
					}
					// Keep fingerprint otherwise valid so each shape fault is causal.
					if axis != "wrong-fingerprint" {
						if p.Run != nil {
							r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *p.Run)
						}
						if p.Reconcile != nil {
							r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *p.Reconcile)
						}
						if p.Resolve != nil {
							r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *p.Resolve)
						}
					}
					n, err := sqliteMutationRequestCharge(st.ComputerID, r)
					mqQACode(t, err, "validation")
					if n != 0 {
						t.Fatal("invalid pending charge")
					}
					// Fresh empty request/projection tables exclude both PK and singleton
					// collisions as an unrelated reason for refusing this input.
					_, c, tx, _ := mqQAStart(t, st)
					n, err = sqliteWriteMutationRequest(tx, m.ComputerID, nil, r)
					mqQACode(t, err, "validation")
					if n != 0 {
						t.Fatal("invalid pending delta")
					}
					var native *sqliteio.Error
					if errors.As(err, &native) {
						t.Fatal("invalid pending input fell through to a native constraint")
					}
					if interopCount(t, tx, "SELECT count(*) FROM requests") != 0 || interopCount(t, tx, "SELECT count(*) FROM pending_sync") != 0 || interopCount(t, tx, "SELECT count(*) FROM pending_sync_roots") != 0 {
						t.Fatal("invalid pending input staged unit rows")
					}
					interopRollback(t, tx)
					interopClose(t, c)
				})
			}
			for _, axis := range []string{"unchanged", "effect-advance", "input-change", "roots-change", "snapshot-change", "operation-change", "fingerprint-change", "id-change", "wrong-terminal"} {
				t.Run("transition-"+axis, func(t *testing.T) {
					c, tx := interopOpen(t, f, false, sqliteio.Write)
					before := mqQARead(t, tx, m.ComputerID, id, m.Revision)
					after := mqQAClone(before)
					after.payload = ""
					p := after.Value.PendingSync
					switch axis {
					case "effect-advance":
						p.EffectCommitted = true
					case "input-change":
						if p.Run != nil {
							p.Run.Limit++
						}
						if p.Reconcile != nil {
							p.Reconcile.Limit++
						}
						if p.Resolve != nil {
							p.Resolve.EntryID = "902"
						}
					case "roots-change":
						p.RootIDs = []string{}
					case "snapshot-change":
						p.SnapshotRevision = bump(p.SnapshotRevision)
					case "operation-change":
						after.Value.Operation = "sync.pause"
					case "fingerprint-change":
						after.Value.Fingerprint = strings.Repeat("f", 64)
					case "id-change":
						after = mqQAReID(after, mqQANewID)
					case "wrong-terminal":
						after = mqQAFrom(finalSource, id)
						after.Value = mutationRequest{Operation: row.Value.Operation, Fingerprint: row.Value.Fingerprint, BindingResult: &BindingResult{}}
					}
					n, err := sqliteWriteMutationRequest(tx, m.ComputerID, &before, after)
					valid := axis == "unchanged" || axis == "effect-advance" && shape == "reconcile-no-effect"
					if valid {
						if err != nil || n != mqQAExpectedCharge(t, after)-mqQAExpectedCharge(t, before) {
							t.Fatal("permitted phase", err)
						}
					} else {
						mqQACode(t, err, "validation")
						if n != 0 {
							t.Fatal("invalid phase delta")
						}
					}
					interopRollback(t, tx)
					interopClose(t, c)
				})
			}
			mqQAReopen(t, f, m, row)
		})
	}
}

func TestSQLiteMutationRequestSelectedOrphansAndRegressiveEffectRefuse(t *testing.T) {
	st, finalSource, id := mqQAPhase(t, "reconcile-effect")
	row, final := mqQAFrom(st, id), mqQAFrom(finalSource, id)
	f, m := mqQASave(t, st, row)
	t.Run("no-effect-regression", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		before := mqQARead(t, tx, m.ComputerID, id, m.Revision)
		after := mqQAClone(before)
		after.payload = ""
		after.Value.PendingSync.EffectCommitted = false
		n, err := sqliteWriteMutationRequest(tx, m.ComputerID, &before, after)
		mqQACode(t, err, "validation")
		if n != 0 {
			t.Fatal("effect regression charged")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		mqQAReopen(t, f, m, row)
	})
	for _, shape := range []string{"terminal-with-pending", "terminal-with-orphan-roots", "extra-root", "duplicate-pending"} {
		t.Run(shape, func(t *testing.T) {
			c, tx := interopOpen(t, f, false, sqliteio.Write)
			if strings.HasPrefix(shape, "terminal-") {
				kind, payload := mqQAPayload(t, final.Value)
				interopDone(t, tx, "UPDATE requests SET outcome_kind=?,payload=? WHERE request_id=?", sqliteio.Text(kind), sqliteio.Text(payload), sqliteio.Text(id))
				if shape == "terminal-with-orphan-roots" {
					interopDone(t, tx, "DELETE FROM pending_sync WHERE request_id=?", sqliteio.Text(id))
				}
			} else if shape == "extra-root" {
				interopDone(t, tx, "INSERT INTO pending_sync_roots(request_id,ordinal,outbox_id) VALUES(?,?,?)", sqliteio.Text(id), sqliteio.Integer(1), sqliteio.Text(mqQANewID))
			} else {
				interopDone(t, tx, "ALTER TABLE pending_sync RENAME TO qa_original_pending")
				interopDone(t, tx, "CREATE TABLE pending_sync(request_id,singleton,kind,effect_committed,snapshot_revision,limit_count,outbox_id,entry_id,if_revision,retry_rejected,confirmed)")
				interopDone(t, tx, "INSERT INTO pending_sync SELECT "+mqQAPending+" FROM qa_original_pending")
				interopDone(t, tx, "INSERT INTO pending_sync SELECT * FROM pending_sync")
			}
			got, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, id, finalSource.Revision)
			mqQAZeroRead(t, got, found, err, "state_corrupt")
			interopRollback(t, tx)
			interopClose(t, c)
			mqQAReopen(t, f, m, row)
		})
	}
}

func TestSQLiteMutationRequestUnsignedCeilingOwnershipAndJSONRepair(t *testing.T) {
	st := mqQABindings(t)
	row := mqQAFrom(st, qaLinkRequest)
	st.Revision = "18446744073709551615"
	row.Value.BindingResult.SnapshotRevision = st.Revision
	st.Requests[row.ID] = row.Value
	st = mqQAComplete(t, st, true)
	f, m := mqQASave(t, st, row)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, ceiling := range []string{"0", "01", "18446744073709551616", ""} {
		got, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, row.ID, ceiling)
		mqQAZeroRead(t, got, found, err, "validation")
	}
	got, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, row.ID, "18446744073709551614")
	mqQAZeroRead(t, got, found, err, "state_corrupt")
	got = mqQARead(t, tx, m.ComputerID, row.ID, m.Revision)
	got.Value.BindingResult.Binding.AttachedActors = append(got.Value.BindingResult.Binding.AttachedActors, ActorRef{})
	got.Value.BindingResult.Binding.Locator = "caller-mutated"
	again := mqQARead(t, tx, m.ComputerID, row.ID, m.Revision)
	if !reflect.DeepEqual(again, mqQAMaterialize(t, row)) {
		t.Fatal("reader returned shared owned DTO")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	mqQAReopen(t, f, m, row)
	for _, count := range []int{2, 1366} {
		name := "repaired-within-limit"
		if count == 1366 {
			name = "repaired-over-limit"
		}
		t.Run(name, func(t *testing.T) {
			raw := mqQABindings(t)
			r := mqQAFrom(raw, qaLinkRequest)
			r.Value.BindingResult.Binding.Locator = "/" + strings.Repeat(string([]byte{0xff}), count)
			raw.Requests[r.ID] = r.Value
			if utf8.ValidString(r.Value.BindingResult.Binding.Locator) || !validBindingView(r.Value.BindingResult.Binding, raw.ComputerID) {
				t.Fatal("raw valid/invalid UTF8 premise")
			}
			decoded := mqQAComplete(t, raw, count == 2)
			if len(decoded.Requests[r.ID].BindingResult.Binding.Locator) != 1+3*count {
				t.Fatal("actual JSON repair expansion differs")
			}
			f, m := mqQASave(t, raw, r)
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			saved, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, r.ID, m.Revision)
			if count == 2 {
				if err != nil || !found || saved.Value.BindingResult.Binding.Locator != decoded.Requests[r.ID].BindingResult.Binding.Locator {
					t.Fatal("repaired local result", err)
				}
			} else {
				mqQAZeroRead(t, saved, found, err, "state_corrupt")
			}
			interopRollback(t, tx)
			interopClose(t, c)
		})
	}
}

func TestSQLiteMutationRequestNativeConstraintsLateUnitRollbackAndCancellation(t *testing.T) {
	st := mqQABindings(t)
	row := mqQAFrom(st, qaLinkRequest)
	f, m := mqQASave(t, st, row)
	t.Run("real-request-primary-key", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		n, err := sqliteWriteMutationRequest(tx, m.ComputerID, nil, row)
		mqQACode(t, err, "validation")
		mqQANative(t, err, 1555)
		if n != 0 {
			t.Fatal("duplicate delta")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		mqQAReopen(t, f, m, row)
	})
	t.Run("late-pending-native-trigger", func(t *testing.T) {
		pending, _, id := mqQAPhase(t, "reconcile-no-effect")
		r := mqQAFrom(pending, id)
		f, c, tx, meta := mqQAStart(t, pending)
		interopDone(t, tx, "CREATE TRIGGER qa_pending_abort BEFORE INSERT ON pending_sync BEGIN SELECT RAISE(ABORT,'owned-private-trigger-message'); END")
		n, err := sqliteWriteMutationRequest(tx, pending.ComputerID, nil, r)
		mqQANative(t, err, 1811)
		var domain *Error
		if errors.As(err, &domain) && domain.Code == "validation" {
			t.Fatal("non-unique native constraint mislabeled collision")
		}
		if n != 0 {
			t.Fatal("late unit error returned charge")
		}
		seenStep, seenFinalize := false, false
		var visit func(error)
		visit = func(e error) {
			if e == nil {
				return
			}
			if n, ok := e.(*sqliteio.Error); ok && n.Code == 1811 {
				seenStep = seenStep || n.Phase == sqliteio.StepPhase
				seenFinalize = seenFinalize || n.Phase == sqliteio.FinalizePhase
			}
			if many, ok := e.(interface{ Unwrap() []error }); ok {
				for _, child := range many.Unwrap() {
					visit(child)
				}
			} else if one, ok := e.(interface{ Unwrap() error }); ok {
				visit(one.Unwrap())
			}
		}
		visit(err)
		if !seenStep || !seenFinalize {
			t.Fatal("late native error lost checked Step/finalization evidence")
		}
		if interopCount(t, tx, "SELECT count(*) FROM requests") != 1 || interopCount(t, tx, "SELECT count(*) FROM pending_sync") != 0 {
			t.Fatal("late failure did not reach staged requests then native pending refusal")
		}
		if err := sqliteInsertMeta(tx, meta); err != nil {
			t.Fatal(err)
		}
		interopRollback(t, tx)
		interopClose(t, c)
		c, tx = interopOpen(t, f, false, sqliteio.Read)
		if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
			t.Fatal("explicit unit rollback leaked schema/request/metadata")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("canceled-operation-keeps-prior-history", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c, err := sqliteio.Open(ctx, f.directory, f.database, sqliteio.Options{AcquireDeadline: time.Now().Add(250 * time.Millisecond)})
		if err != nil {
			t.Fatal(err)
		}
		ownedConn := c
		t.Cleanup(func() {
			if err := ownedConn.Close(context.Background()); err != nil {
				t.Errorf("owned canceled connection cleanup: %v", err)
			}
		})
		tx, err := c.Begin(ctx, sqliteio.Write)
		if err != nil {
			t.Fatal(err)
		}
		ownedTx := tx
		t.Cleanup(func() { _ = ownedTx.Rollback() })
		proposal := mqQAReID(row, mqQANewID)
		delta, err := sqliteWriteMutationRequest(tx, m.ComputerID, nil, proposal)
		if err != nil || delta == 0 {
			t.Fatal("prior staged positive control", err)
		}
		cancel()
		another := mqQAReID(row, qaSyncID(998))
		n, err := sqliteWriteMutationRequest(tx, m.ComputerID, nil, another)
		if n != 0 || !errors.Is(err, context.Canceled) {
			t.Fatal("canceled write returned effects or lost cancellation cause", err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal("owned cancellation cleanup", err)
		}
		interopClose(t, c)
		mqQAReopen(t, f, m, row)
		c, tx = interopOpen(t, f, false, sqliteio.Read)
		if interopCount(t, tx, "SELECT count(*) FROM requests") != 1 {
			t.Fatal("canceled caller rollback retained staged sibling")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteMutationRequestOrderedRootsEmptyArraysAndIndexedQueries(t *testing.T) {
	st, _, id := mqQAPhase(t, "run-two")
	row := mqQAFrom(st, id)
	if len(row.Value.PendingSync.RootIDs) != 2 {
		t.Fatal("native two-root reservation premise")
	}
	// The stored contract preserves caller order, not sorted root identity.
	p := row.Value.PendingSync
	p.RootIDs[0], p.RootIDs[1] = p.RootIDs[1], p.RootIDs[0]
	st.Requests[id] = row.Value
	st = mqQAComplete(t, st, true)
	f, m := mqQASave(t, st, row)
	mqQAReopen(t, f, m, row)
	t.Run("ordered-child-projection", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		before := mqQARead(t, tx, m.ComputerID, id, m.Revision)
		interopDone(t, tx, "UPDATE pending_sync_roots SET ordinal=ordinal+2 WHERE request_id=?", sqliteio.Text(id))
		interopDone(t, tx, "UPDATE pending_sync_roots SET ordinal=CASE ordinal WHEN 2 THEN 1 ELSE 0 END WHERE request_id=?", sqliteio.Text(id))
		got, found, err := sqliteReadMutationRequestLocal(tx, m.ComputerID, id, m.Revision)
		mqQAZeroRead(t, got, found, err, "state_corrupt")
		after := mqQAClone(before)
		after.payload = ""
		n, err := sqliteWriteMutationRequest(tx, m.ComputerID, &before, after)
		mqQACode(t, err, "state_corrupt")
		if n != 0 {
			t.Fatal("reordered children charged")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	t.Run("exact-query-plans", func(t *testing.T) {
		c, tx := interopOpen(t, f, false, sqliteio.Read)
		for _, q := range []struct{ sql, index string }{
			{"SELECT " + mqQARequests + " FROM requests WHERE request_id=?", "sqlite_autoindex_requests_1"},
			{"SELECT " + mqQAPending + " FROM pending_sync WHERE request_id=?", "sqlite_autoindex_pending_sync_1"},
			{"SELECT " + mqQARoots + " FROM pending_sync_roots WHERE request_id=? ORDER BY ordinal", "sqlite_autoindex_pending_sync_roots_1"},
		} {
			rows, _ := mqQAScan(t, tx, "EXPLAIN QUERY PLAN "+q.sql, sqliteio.Text(id))
			searched := false
			for _, r := range rows {
				if len(r) != 4 {
					t.Fatal("unexpected query plan shape")
				}
				detail, ok := r[3].(string)
				if !ok {
					t.Fatal("query plan detail type")
				}
				if strings.Contains(detail, "SCAN ") || strings.Contains(detail, "TEMP B-TREE") {
					t.Fatal("selected request query scanned/sorted retained history", detail)
				}
				searched = searched || strings.Contains(detail, "SEARCH ") && strings.Contains(detail, q.index)
			}
			if !searched {
				t.Fatal("fixed selected query lost its actual key index")
			}
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	for _, kind := range []string{"run", "reconcile"} {
		for _, limit := range []int{1, 100} {
			name := kind + "-min"
			if limit == 100 {
				name = kind + "-max"
			}
			t.Run(name, func(t *testing.T) {
				base := mqQASyncComplete(t)
				r := sqliteMutationRequestRow{ID: mqQANewID, Value: mutationRequest{PendingSync: &syncReservation{RootIDs: []string{}, SnapshotRevision: base.Revision}}}
				if kind == "run" {
					r.Value.Operation = "sync.now"
					r.Value.PendingSync.Run = &SyncRunInput{RequestID: r.ID, Limit: limit}
					r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *r.Value.PendingSync.Run)
				} else {
					r.Value.Operation = "sync.reconcile"
					r.Value.PendingSync.Reconcile = &SyncReconcileInput{RequestID: r.ID, Limit: limit}
					r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *r.Value.PendingSync.Reconcile)
				}
				base.Requests[r.ID] = r.Value
				base = mqQAComplete(t, base, true)
				f, m := mqQASave(t, base, r)
				mqQAReopen(t, f, m, r)
				c, tx := interopOpen(t, f, false, sqliteio.Read)
				got := mqQARead(t, tx, m.ComputerID, r.ID, m.Revision)
				if got.Value.PendingSync.RootIDs == nil || len(got.Value.PendingSync.RootIDs) != 0 || interopCount(t, tx, "SELECT count(*) FROM pending_sync_roots") != 0 {
					t.Fatal("empty roots collapsed or fabricated")
				}
				interopRollback(t, tx)
				interopClose(t, c)
			})
		}
	}
}

func TestSQLiteMutationRequestSingletonAndRealDeferredForeignKeyRefusal(t *testing.T) {
	st, _, id := mqQAPhase(t, "reconcile-no-effect")
	row := mqQAFrom(st, id)
	f, m := mqQASave(t, st, row)
	t.Run("otherwise-valid-pending-singleton", func(t *testing.T) {
		other := mqQAReID(row, mqQANewID)
		charge, err := sqliteMutationRequestCharge(m.ComputerID, other)
		if err != nil || charge <= 0 {
			t.Fatal("singleton positive input control", err)
		}
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		old, _ := mqQAUnit(t, tx, id)
		n, err := sqliteWriteMutationRequest(tx, m.ComputerID, nil, other)
		mqQACode(t, err, "validation")
		mqQANative(t, err, 2067)
		if n != 0 {
			t.Fatal("singleton refusal returned usable delta")
		}
		after, _ := mqQAUnit(t, tx, id)
		if !reflect.DeepEqual(old, after) {
			t.Fatal("singleton refusal changed old pending owner")
		}
		interopRollback(t, tx)
		interopClose(t, c)
		mqQAReopen(t, f, m, row)
	})
	t.Run("real-missing-outbox-target-commit", func(t *testing.T) {
		// Locally valid pending roots are not silently materialized as invented
		// outbox targets. Actual deferred FK enforcement must reject COMMIT.
		r := mqQAReID(row, mqQANewID)
		r.Value.PendingSync.RootIDs = []string{qaSyncID(997)}
		r.Value.PendingSync.Reconcile.OutboxID = ""
		r.Value.Fingerprint = mutationFingerprint(r.Value.Operation, *r.Value.PendingSync.Reconcile)
		f, c, tx, meta := mqQAStart(t, st)
		delta, err := sqliteWriteMutationRequest(tx, st.ComputerID, nil, r)
		if err != nil || delta != mqQAExpectedCharge(t, r) {
			t.Fatal("local missing-dependency stage rejected prematurely", err)
		}
		meta.LogicalBytes += delta
		if err := sqliteInsertMeta(tx, meta); err != nil {
			t.Fatal(err)
		}
		outcome, err := tx.Commit()
		var native *sqliteio.Error
		if outcome != sqliteio.Unknown || !errors.As(err, &native) || native.Code&255 != 19 || native.Phase != sqliteio.CommitPhase {
			t.Fatalf("actual deferred FK COMMIT outcome=%v error=%v", outcome, err)
		}
		if again, repeat := tx.Commit(); again != outcome || repeat != err {
			t.Fatal("terminal commit evidence changed")
		}
		if cleanup := tx.Rollback(); cleanup == nil {
			t.Fatal("checked failed COMMIT finalization evidence was erased")
		}
		interopClose(t, c)
		c, tx = interopOpen(t, f, false, sqliteio.Read)
		if interopCount(t, tx, "SELECT count(*) FROM sqlite_schema") != 0 {
			t.Fatal("deferred FK refusal retained proposed schema/domain/request/metadata")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
}

func TestSQLiteMutationRequestReaderScopeArgumentsAndTerminalOwnedValues(t *testing.T) {
	st := mqQASyncComplete(t)
	row := mqQAFrom(st, qaLinkRequest)
	for _, a := range st.Actors {
		row.Value.BindingResult.Binding.AttachedActors = append(row.Value.BindingResult.Binding.AttachedActors, a.Ref)
	}
	if len(row.Value.BindingResult.Binding.AttachedActors) == 0 {
		t.Fatal("real actor reference premise")
	}
	st.Requests[row.ID] = row.Value
	st = mqQAComplete(t, st, true)
	f, m := mqQASave(t, st, row)
	c, tx := interopOpen(t, f, false, sqliteio.Read)
	for _, args := range [][3]string{{"bad-computer", row.ID, m.Revision}, {m.ComputerID, "bad-id", m.Revision}, {m.ComputerID, row.ID, "0"}} {
		got, found, err := sqliteReadMutationRequestLocal(tx, args[0], args[1], args[2])
		mqQAZeroRead(t, got, found, err, "validation")
	}
	got, found, err := sqliteReadMutationRequestLocal(tx, qaSyncID(999), row.ID, m.Revision)
	mqQAZeroRead(t, got, found, err, "state_corrupt")
	got = mqQARead(t, tx, m.ComputerID, row.ID, m.Revision)
	got.Value.BindingResult.Binding.AttachedActors[0].Key.AgentID = "caller-only"
	if !reflect.DeepEqual(mqQARead(t, tx, m.ComputerID, row.ID, m.Revision), mqQAMaterialize(t, row)) {
		t.Fatal("attached actor backing slice not owned")
	}
	interopRollback(t, tx)
	interopClose(t, c)
	t.Run("invalid-writer-computer-is-pure-validation", func(t *testing.T) {
		proposal := mqQAReID(row, mqQANewID)
		original := mqQAClone(proposal)
		n, err := sqliteMutationRequestCharge("bad-computer", proposal)
		mqQACode(t, err, "validation")
		if n != 0 {
			t.Fatal("invalid explicit scope charged")
		}
		c, tx := interopOpen(t, f, false, sqliteio.Write)
		n, err = sqliteWriteMutationRequest(tx, "bad-computer", nil, proposal)
		mqQACode(t, err, "validation")
		if n != 0 || !reflect.DeepEqual(original, proposal) || interopCount(t, tx, "SELECT count(*) FROM requests") != 1 {
			t.Fatal("invalid scope mutated input/store")
		}
		interopRollback(t, tx)
		interopClose(t, c)
	})
	for _, id := range []string{qaSyncID(1), qaSyncID(710), qaSyncID(711)} {
		t.Run(id, func(t *testing.T) {
			r := mqQAFrom(st, id)
			if r.Value.SyncConfigurationResult != nil {
				r.Value.SyncConfigurationResult.Configuration.Clock = mqQAString("")
			}
			if r.Value.MutationResult != nil {
				r.Value.MutationResult.AffectedIDs = []string{mqQANewID}
				r.Value.MutationResult.EntityRevision = mqQAString("opaque")
			}
			copy := asQAJSON(t, st)
			copy.Requests[id] = r.Value
			copy = mqQAComplete(t, copy, true)
			f, m := mqQASave(t, copy, r)
			c, tx := interopOpen(t, f, false, sqliteio.Read)
			got := mqQARead(t, tx, m.ComputerID, id, m.Revision)
			if got.Value.SyncConfigurationResult != nil {
				*got.Value.SyncConfigurationResult.Configuration.Clock = "caller-only"
			}
			if got.Value.SyncRun != nil {
				got.Value.SyncRun.AttemptedIDs[0] = mqQANewID
				got.Value.SyncRun.ResolvedIDs[0] = mqQANewID
			}
			if got.Value.MutationResult != nil {
				got.Value.MutationResult.AffectedIDs[0] = qaSyncID(998)
				*got.Value.MutationResult.EntityRevision = "caller-only"
			}
			if !reflect.DeepEqual(mqQARead(t, tx, m.ComputerID, id, m.Revision), mqQAMaterialize(t, r)) {
				t.Fatal("terminal nested data not owned")
			}
			interopRollback(t, tx)
			interopClose(t, c)
			mqQAReopen(t, f, m, r)
		})
	}
}
