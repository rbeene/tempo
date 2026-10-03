//go:build (darwin || linux) && (amd64 || arm64)

package activity

import (
	"encoding/json"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/rbeene/tempo/internal/activity/sqliteio"
)

// Fixed installed DDL catalog. Only the bounded schema is inspected; no domain
// history is loaded. These literal bytes accompany sqlite_schema.sql changes.
// Root-owned native acceptance verifies them against the pinned SQLite engine.
var sqliteCaptureCatalog = [...][4]string{
	{`index`, `actor_binding`, `actors`, `CREATE INDEX actor_binding ON actors(binding_id,actor_key)`},
	{`index`, `actor_clock`, `actors`, `CREATE INDEX actor_clock ON actors(computer_id,state,actor_key) WHERE health = 'continuous'`},
	{`index`, `actor_live`, `actors`, `CREATE INDEX actor_live ON actors(computer_id,state,actor_key,generation)
WHERE state NOT IN ('finished','interrupted')`},
	{`index`, `actor_parent`, `actors`, `CREATE INDEX actor_parent ON actors(parent_key,parent_generation)`},
	{`index`, `actor_segment`, `actors`, `CREATE INDEX actor_segment ON actors(segment_id)`},
	{`index`, `actor_timer`, `actors`, `CREATE INDEX actor_timer ON actors(computer_id,account_id,project_id,state,health,actor_key)`},
	{`index`, `actor_uncertainty_member`, `actor_uncertainties`, `CREATE INDEX actor_uncertainty_member ON actor_uncertainties(actor_key,uncertainty_id,ordinal)`},
	{`index`, `actor_uncertainty_ref`, `actor_uncertainties`, `CREATE INDEX actor_uncertainty_ref ON actor_uncertainties(uncertainty_id,actor_key)`},
	{`index`, `attempt_request_ref`, `sync_attempts`, `CREATE INDEX attempt_request_ref ON sync_attempts(request_id)`},
	{`index`, `binding_active_locator`, `bindings`, `CREATE UNIQUE INDEX binding_active_locator ON bindings(kind,locator) WHERE record_present = 1 AND deleted = 0`},
	{`index`, `binding_timer`, `bindings`, `CREATE INDEX binding_timer ON bindings(computer_id,account_id,project_id,active)`},
	{`index`, `epoch_latest`, `epochs`, `CREATE INDEX epoch_latest ON epochs(computer_id,account_id,user_id,project_id,task_id,timezone,creation_ordinal DESC)`},
	{`index`, `event_actor_ref`, `event_receipts`, `CREATE INDEX event_actor_ref ON event_receipts(actor_key,actor_generation)`},
	{`index`, `event_segment_ref`, `event_receipts`, `CREATE INDEX event_segment_ref ON event_receipts(segment_id)`},
	{`index`, `frontier_group_end`, `union_frontier`, `CREATE INDEX frontier_group_end ON union_frontier(computer_id,account_id,user_id,project_id,task_id,timezone,end_sec,end_nsec,start_sec,start_nsec)`},
	{`index`, `frontier_group_start`, `union_frontier`, `CREATE INDEX frontier_group_start ON union_frontier(computer_id,account_id,user_id,project_id,task_id,timezone,start_sec,start_nsec,end_sec,end_nsec)`},
	{`index`, `frontier_timer_end`, `union_frontier`, `CREATE INDEX frontier_timer_end ON union_frontier(computer_id,account_id,project_id,end_sec,end_nsec,component_id)`},
	{`index`, `frontier_timer_start`, `union_frontier`, `CREATE INDEX frontier_timer_start ON union_frontier(computer_id,account_id,project_id,start_sec,start_nsec,component_id)`},
	{`index`, `generation_latest`, `actor_generations`, `CREATE INDEX generation_latest ON actor_generations(actor_key,generation DESC)`},
	{`index`, `host_actor_latest`, `host_receipts`, `CREATE INDEX host_actor_latest ON host_receipts(actor_key,actor_generation,snapshot_revision DESC,receipt_id DESC)`},
	{`index`, `host_listing`, `host_receipts`, `CREATE INDEX host_listing ON host_receipts(source,native_session,snapshot_revision,receipt_id)`},
	{`index`, `host_session_root`, `host_sessions`, `CREATE INDEX host_session_root ON host_sessions(root_turn_key)
WHERE root_turn_key<>''`},
	{`index`, `interval_timer_end`, `intervals`, `CREATE INDEX interval_timer_end ON intervals(computer_id,account_id,project_id,end_sec DESC,end_nsec DESC)`},
	{`index`, `interval_timer_start`, `intervals`, `CREATE INDEX interval_timer_start ON intervals(computer_id,account_id,project_id,start_sec,start_nsec,end_sec,end_nsec)`},
	{`index`, `outbox_retry_ref`, `outbox`, `CREATE INDEX outbox_retry_ref ON outbox(retry_request_id)`},
	{`index`, `outbox_run_ref`, `outbox`, `CREATE INDEX outbox_run_ref ON outbox(run_request_id)`},
	{`index`, `outbox_state`, `outbox`, `CREATE INDEX outbox_state ON outbox(state,id)`},
	{`index`, `part_attachment_ref`, `sync_parts`, `CREATE INDEX part_attachment_ref ON sync_parts(attachment_request_id)`},
	{`index`, `pending_order`, `pending_finalization`, `CREATE INDEX pending_order ON pending_finalization(group_order,start_sec,start_nsec,end_sec,end_nsec,component_id)`},
	{`index`, `pending_outbox_ref`, `pending_sync_roots`, `CREATE INDEX pending_outbox_ref ON pending_sync_roots(outbox_id)`},
	{`index`, `recovery_request_ref`, `recovery_decisions`, `CREATE INDEX recovery_request_ref ON recovery_decisions(request_id)`},
	{`index`, `segment_actor`, `segments`, `CREATE INDEX segment_actor ON segments(actor_key,actor_generation,start_sec,start_nsec,segment_id)`},
	{`index`, `segment_epoch`, `segments`, `CREATE INDEX segment_epoch ON segments(epoch_id)`},
	{`index`, `segment_uncertainty`, `segments`, `CREATE INDEX segment_uncertainty ON segments(uncertainty_id)`},
	{`index`, `tool_pending`, `host_tools`, `CREATE INDEX tool_pending ON host_tools(turn_key,name,tool_id) WHERE phase = 'pre'`},
	{`index`, `turn_actor`, `host_turns`, `CREATE INDEX turn_actor ON host_turns(actor_key,actor_generation,turn_key)`},
	{`index`, `turn_incarnation`, `host_turns`, `CREATE INDEX turn_incarnation ON host_turns(incarnation,source,native_session)`},
	{`index`, `turn_incarnation_actor`, `host_turns`, `CREATE INDEX turn_incarnation_actor ON host_turns(incarnation)
WHERE actor_key IS NOT NULL`},
	{`index`, `turn_native`, `host_turns`, `CREATE INDEX turn_native ON host_turns(source,native_session,turn_id,agent_id,incarnation)`},
	{`index`, `turn_native_incarnation`, `host_turns`, `CREATE INDEX turn_native_incarnation ON host_turns(source,native_session,incarnation)`},
	{`index`, `turn_tool_target`, `host_turns`, `CREATE INDEX turn_tool_target ON host_turns(source,native_session,turn_id,incarnation,agent_id)`},
	{`index`, `uncertainty_actor`, `uncertainties`, `CREATE INDEX uncertainty_actor ON uncertainties(actor_key,actor_generation,state,lower_bound_sec,lower_bound_nsec)`},
	{`index`, `uncertainty_open_actor`, `uncertainties`, `CREATE INDEX uncertainty_open_actor ON uncertainties(actor_key,uncertainty_id)
WHERE state='unresolved' AND upper_bound_sec IS NULL`},
	{`index`, `uncertainty_segment`, `uncertainties`, `CREATE INDEX uncertainty_segment ON uncertainties(segment_id)`},
	{`index`, `uncertainty_timer_lower`, `uncertainties`, `CREATE INDEX uncertainty_timer_lower ON uncertainties(computer_id,account_id,project_id,lower_bound_sec,lower_bound_nsec) WHERE state = 'unresolved'`},
	{`index`, `uncertainty_timer_open`, `uncertainties`, `CREATE INDEX uncertainty_timer_open ON uncertainties(computer_id,account_id,project_id,lower_bound_sec,lower_bound_nsec)
WHERE state='unresolved' AND upper_bound_sec IS NULL`},
	{`index`, `uncertainty_timer_upper`, `uncertainties`, `CREATE INDEX uncertainty_timer_upper ON uncertainties(computer_id,account_id,project_id,upper_bound_sec,upper_bound_nsec) WHERE state = 'unresolved'`},
	{`table`, `actor_generations`, `actor_generations`, `CREATE TABLE actor_generations (
    actor_key TEXT NOT NULL,
    generation BLOB NOT NULL,
    computer_id TEXT NOT NULL,
    source TEXT NOT NULL,
    session_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    CHECK (generation IS NULL OR (length(generation) = 8 AND generation != X'0000000000000000')),
    PRIMARY KEY (actor_key,generation),
    CHECK (source IN ('codex','claude','manual-test'))
) STRICT`},
	{`table`, `actor_uncertainties`, `actor_uncertainties`, `CREATE TABLE actor_uncertainties (
    actor_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    uncertainty_id TEXT NOT NULL,
    CHECK (ordinal >= 0),
    PRIMARY KEY (actor_key,ordinal),
    FOREIGN KEY (actor_key) REFERENCES actors(actor_key) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (uncertainty_id) REFERENCES uncertainties(uncertainty_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `actors`, `actors`, `CREATE TABLE actors (
    actor_key TEXT NOT NULL,
    id TEXT NOT NULL,
    revision BLOB NOT NULL,
    generation BLOB NOT NULL,
    sequence BLOB NOT NULL,
    state TEXT NOT NULL,
    health TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    binding_revision BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    parent_key TEXT,
    parent_generation BLOB,
    segment_id TEXT,
    last_evidence_capability TEXT NOT NULL,
    last_evidence_wall_sec INTEGER NOT NULL,
    last_evidence_wall_nsec INTEGER NOT NULL,
    last_evidence_wall_json TEXT NOT NULL,
    last_evidence_epoch TEXT NOT NULL,
    last_evidence_elapsed_raw TEXT NOT NULL,
    last_evidence_awake_raw TEXT NOT NULL,
    last_evidence_elapsed BLOB NOT NULL,
    last_evidence_awake BLOB NOT NULL,
    CHECK (revision IS NULL OR (length(revision) = 8 AND revision != X'0000000000000000')),
    CHECK (generation IS NULL OR (length(generation) = 8 AND generation != X'0000000000000000')),
    CHECK (sequence IS NULL OR (length(sequence) = 8 AND sequence != X'0000000000000000')),
    CHECK (binding_revision IS NULL OR (length(binding_revision) = 8 AND binding_revision != X'0000000000000000')),
    CHECK (parent_generation IS NULL OR (length(parent_generation) = 8 AND parent_generation != X'0000000000000000')),
    CHECK ((parent_key IS NULL) = (parent_generation IS NULL)),
    FOREIGN KEY (parent_key,parent_generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    CHECK (last_evidence_wall_nsec IS NULL OR (last_evidence_wall_nsec >= 0 AND last_evidence_wall_nsec < 1000000000)),
    CHECK (last_evidence_elapsed IS NULL OR (length(last_evidence_elapsed) = 8)),
    CHECK (last_evidence_awake IS NULL OR (length(last_evidence_awake) = 8)),
    CHECK (last_evidence_capability IS NULL OR last_evidence_capability = 'available'),
    CHECK (last_evidence_elapsed IS NULL OR last_evidence_elapsed <= X'7FFFFFFFFFFFFFFF'),
    CHECK (last_evidence_awake IS NULL OR last_evidence_awake <= X'7FFFFFFFFFFFFFFF'),
    PRIMARY KEY (actor_key),
    UNIQUE (id),
    CHECK (state IN ('working','wait_user','wait_permission','wait_children','interrupted','finished')),
    CHECK (health IN ('continuous','stale','order_blocked')),
    CHECK ((state = 'working' AND segment_id IS NOT NULL) OR (state != 'working' AND segment_id IS NULL)),
    FOREIGN KEY (actor_key,generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `bindings`, `bindings`, `CREATE TABLE bindings (
    binding_id TEXT NOT NULL,
    revision BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    active INTEGER NOT NULL,
    record_present INTEGER NOT NULL,
    kind TEXT,
    locator TEXT,
    deleted INTEGER,
    CHECK (revision IS NULL OR (length(revision) = 8 AND revision != X'0000000000000000')),
    CHECK (active IN (0,1)),
    CHECK (record_present IN (0,1)),
    CHECK (deleted IN (0,1)),
    PRIMARY KEY (binding_id),
    CHECK ((record_present = 0 AND active = 1 AND kind IS NULL AND locator IS NULL AND deleted IS NULL) OR (record_present = 1 AND kind IN ('repository','directory') AND kind IS NOT NULL AND locator IS NOT NULL AND deleted IS NOT NULL AND active = 1-deleted))
) STRICT`},
	{`table`, `component_segments`, `component_segments`, `CREATE TABLE component_segments (
    component_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    PRIMARY KEY (component_id,segment_id),
    UNIQUE (segment_id),
    FOREIGN KEY (component_id) REFERENCES union_frontier(component_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `epochs`, `epochs`, `CREATE TABLE epochs (
    epoch_id TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    creation_ordinal INTEGER NOT NULL,
    group_order TEXT NOT NULL,
    anchor_capability TEXT NOT NULL,
    anchor_wall_sec INTEGER NOT NULL,
    anchor_wall_nsec INTEGER NOT NULL,
    anchor_wall_json TEXT NOT NULL,
    anchor_epoch TEXT NOT NULL,
    anchor_elapsed_raw TEXT NOT NULL,
    anchor_awake_raw TEXT NOT NULL,
    anchor_elapsed BLOB NOT NULL,
    anchor_awake BLOB NOT NULL,
    CHECK (creation_ordinal >= 0),
    CHECK (anchor_wall_nsec IS NULL OR (anchor_wall_nsec >= 0 AND anchor_wall_nsec < 1000000000)),
    CHECK (anchor_elapsed IS NULL OR (length(anchor_elapsed) = 8)),
    CHECK (anchor_awake IS NULL OR (length(anchor_awake) = 8)),
    CHECK (anchor_capability IS NULL OR anchor_capability = 'available'),
    CHECK (anchor_elapsed IS NULL OR anchor_elapsed <= X'7FFFFFFFFFFFFFFF'),
    CHECK (anchor_awake IS NULL OR anchor_awake <= X'7FFFFFFFFFFFFFFF'),
    PRIMARY KEY (epoch_id),
    UNIQUE (creation_ordinal)
) STRICT`},
	{`table`, `event_ids`, `event_ids`, `CREATE TABLE event_ids (
    event_id TEXT NOT NULL,
    event_key TEXT NOT NULL,
    PRIMARY KEY (event_id),
    UNIQUE (event_key),
    FOREIGN KEY (event_key) REFERENCES event_receipts(event_key) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `event_receipt_uncertainties`, `event_receipt_uncertainties`, `CREATE TABLE event_receipt_uncertainties (
    event_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    uncertainty_id TEXT NOT NULL,
    CHECK (ordinal >= 0),
    PRIMARY KEY (event_key,ordinal),
    FOREIGN KEY (event_key) REFERENCES event_receipts(event_key) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `event_receipts`, `event_receipts`, `CREATE TABLE event_receipts (
    event_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    contract_version INTEGER NOT NULL,
    snapshot_revision BLOB NOT NULL,
    disposition TEXT NOT NULL,
    actor_key TEXT NOT NULL,
    actor_generation BLOB NOT NULL,
    segment_id TEXT,
    CHECK (snapshot_revision IS NULL OR (length(snapshot_revision) = 8)),
    CHECK (actor_generation IS NULL OR (length(actor_generation) = 8 AND actor_generation != X'0000000000000000')),
    FOREIGN KEY (actor_key,actor_generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    PRIMARY KEY (event_key),
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (length(CAST(fingerprint AS BLOB)) = 64 AND fingerprint NOT GLOB '*[^0-9a-fA-F]*'),
    CHECK (contract_version = 1 AND disposition = 'applied')
) STRICT`},
	{`table`, `host_receipts`, `host_receipts`, `CREATE TABLE host_receipts (
    receipt_key TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    error_code TEXT NOT NULL,
    contract_version INTEGER NOT NULL,
    snapshot_revision BLOB NOT NULL,
    receipt_id TEXT NOT NULL,
    source TEXT NOT NULL,
    native_session TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    tool_id TEXT NOT NULL,
    disposition TEXT NOT NULL,
    ordering TEXT NOT NULL,
    diagnostic_code TEXT NOT NULL,
    durability TEXT NOT NULL,
    origin TEXT NOT NULL,
    profile_basis TEXT NOT NULL,
    profile_revision BLOB NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    actor_key TEXT,
    actor_generation BLOB,
    observed_at_sec INTEGER NOT NULL,
    observed_at_nsec INTEGER NOT NULL,
    observed_at_json TEXT NOT NULL,
    CHECK (snapshot_revision IS NULL OR (length(snapshot_revision) = 8 AND snapshot_revision != X'0000000000000000')),
    CHECK (profile_revision IS NULL OR (length(profile_revision) = 8)),
    CHECK (actor_generation IS NULL OR (length(actor_generation) = 8 AND actor_generation != X'0000000000000000')),
    CHECK ((actor_key IS NULL) = (actor_generation IS NULL)),
    FOREIGN KEY (actor_key,actor_generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    CHECK (observed_at_nsec IS NULL OR (observed_at_nsec >= 0 AND observed_at_nsec < 1000000000)),
    PRIMARY KEY (receipt_key),
    CHECK (length(CAST(receipt_key AS BLOB)) = 64 AND length(CAST(input_fingerprint AS BLOB)) = 64),
    CHECK (contract_version = 1),
    CHECK (source IN ('codex','claude')),
    CHECK (disposition IN ('applied','stale','review_required')),
    CHECK (ordering IN ('supported','review_required','unavailable')),
    CHECK (durability = 'committed' AND origin = 'unverified'),
    CHECK ((profile_basis = 'operator_declared' AND profile_revision != X'0000000000000000' AND length(CAST(policy_fingerprint AS BLOB)) = 64) OR (profile_basis = 'none' AND profile_revision = X'0000000000000000' AND policy_fingerprint = '' AND disposition = 'review_required')),
    CHECK (error_code IN ('','clock_unavailable','clock_conflict','event_gap','event_conflict','invalid_transition'))
) STRICT`},
	{`table`, `host_sessions`, `host_sessions`, `CREATE TABLE host_sessions (
    source TEXT NOT NULL,
    native_session TEXT NOT NULL,
    session_key TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    cwd TEXT NOT NULL,
    root_turn_key TEXT NOT NULL,
    PRIMARY KEY (source,native_session),
    UNIQUE (session_key),
    CHECK (source IN ('codex','claude'))
) STRICT`},
	{`table`, `host_tools`, `host_tools`, `CREATE TABLE host_tools (
    turn_key TEXT NOT NULL,
    tool_id TEXT NOT NULL,
    name TEXT NOT NULL,
    phase TEXT NOT NULL,
    PRIMARY KEY (turn_key,tool_id),
    FOREIGN KEY (turn_key) REFERENCES host_turns(turn_key) DEFERRABLE INITIALLY DEFERRED,
    CHECK (phase IN ('pre','post','failed'))
) STRICT`},
	{`table`, `host_turns`, `host_turns`, `CREATE TABLE host_turns (
    turn_key TEXT NOT NULL,
    source TEXT NOT NULL,
    native_session TEXT NOT NULL,
    incarnation TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    cwd TEXT NOT NULL,
    actor_key TEXT,
    actor_generation BLOB,
    stopped INTEGER NOT NULL,
    CHECK (actor_generation IS NULL OR (length(actor_generation) = 8 AND actor_generation != X'0000000000000000')),
    CHECK ((actor_key IS NULL) = (actor_generation IS NULL)),
    FOREIGN KEY (actor_key,actor_generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    CHECK (stopped IN (0,1)),
    PRIMARY KEY (turn_key),
    UNIQUE (incarnation,turn_id,agent_id),
    CHECK (source IN ('codex','claude'))
) STRICT`},
	{`table`, `interval_components`, `interval_components`, `CREATE TABLE interval_components (
    interval_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    PRIMARY KEY (interval_id,component_id),
    UNIQUE (component_id),
    FOREIGN KEY (interval_id) REFERENCES intervals(interval_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `interval_segments`, `interval_segments`, `CREATE TABLE interval_segments (
    interval_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    segment_id TEXT NOT NULL,
    CHECK (ordinal >= 0),
    PRIMARY KEY (interval_id,ordinal),
    UNIQUE (segment_id),
    FOREIGN KEY (interval_id) REFERENCES intervals(interval_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `intervals`, `intervals`, `CREATE TABLE intervals (
    interval_id TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    group_order TEXT NOT NULL,
    start_sec INTEGER NOT NULL,
    start_nsec INTEGER NOT NULL,
    start_json TEXT NOT NULL,
    end_sec INTEGER NOT NULL,
    end_nsec INTEGER NOT NULL,
    end_json TEXT NOT NULL,
    duration_ns BLOB NOT NULL,
    creation_ordinal INTEGER NOT NULL,
    CHECK (start_nsec IS NULL OR (start_nsec >= 0 AND start_nsec < 1000000000)),
    CHECK (end_nsec IS NULL OR (end_nsec >= 0 AND end_nsec < 1000000000)),
    CHECK (duration_ns IS NULL OR (length(duration_ns) = 8 AND duration_ns != X'0000000000000000')),
    CHECK (creation_ordinal >= 0),
    PRIMARY KEY (interval_id),
    UNIQUE (creation_ordinal),
    CHECK (end_sec > start_sec OR (end_sec = start_sec AND end_nsec > start_nsec))
) STRICT`},
	{`table`, `outbox`, `outbox`, `CREATE TABLE outbox (
    interval_id TEXT NOT NULL,
    id TEXT NOT NULL,
    revision BLOB NOT NULL,
    state TEXT NOT NULL,
    correlation TEXT NOT NULL,
    entry_id TEXT,
    failure_category TEXT,
    retry_request_id TEXT,
    run_request_id TEXT,
    plan_present INTEGER NOT NULL,
    CHECK (revision IS NULL OR (length(revision) = 8 AND revision != X'0000000000000000')),
    CHECK (plan_present IN (0,1)),
    PRIMARY KEY (interval_id),
    UNIQUE (id),
    FOREIGN KEY (interval_id) REFERENCES intervals(interval_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (retry_request_id) REFERENCES requests(request_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_request_id) REFERENCES requests(request_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (state IN ('queued','submitting','synced','rejected','unknown','needs_attention')),
    CHECK (correlation = 'tempo:' || interval_id)
) STRICT`},
	{`table`, `pending_finalization`, `pending_finalization`, `CREATE TABLE pending_finalization (
    component_id TEXT NOT NULL,
    group_order TEXT NOT NULL,
    start_sec INTEGER NOT NULL,
    start_nsec INTEGER NOT NULL,
    start_json TEXT NOT NULL,
    end_sec INTEGER NOT NULL,
    end_nsec INTEGER NOT NULL,
    end_json TEXT NOT NULL,
    CHECK (start_nsec IS NULL OR (start_nsec >= 0 AND start_nsec < 1000000000)),
    CHECK (end_nsec IS NULL OR (end_nsec >= 0 AND end_nsec < 1000000000)),
    PRIMARY KEY (component_id),
    FOREIGN KEY (component_id) REFERENCES union_frontier(component_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `pending_sync`, `pending_sync`, `CREATE TABLE pending_sync (
    request_id TEXT NOT NULL,
    singleton INTEGER NOT NULL,
    kind TEXT NOT NULL,
    effect_committed INTEGER NOT NULL,
    snapshot_revision BLOB NOT NULL,
    limit_count INTEGER,
    outbox_id TEXT,
    entry_id TEXT,
    if_revision TEXT,
    retry_rejected INTEGER,
    confirmed INTEGER,
    CHECK (effect_committed IN (0,1)),
    CHECK (snapshot_revision IS NULL OR (length(snapshot_revision) = 8 AND snapshot_revision != X'0000000000000000')),
    CHECK (retry_rejected IN (0,1)),
    CHECK (confirmed IN (0,1)),
    PRIMARY KEY (request_id),
    UNIQUE (singleton),
    FOREIGN KEY (request_id) REFERENCES requests(request_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (singleton = 1),
    CHECK ((kind = 'run' AND effect_committed = 0 AND limit_count BETWEEN 1 AND 100 AND limit_count IS NOT NULL AND outbox_id IS NULL AND entry_id IS NULL AND if_revision IS NULL AND retry_rejected IS NULL AND confirmed IS NULL) OR (kind = 'reconcile' AND limit_count BETWEEN 1 AND 100 AND limit_count IS NOT NULL AND outbox_id IS NOT NULL AND entry_id IS NULL AND if_revision IS NULL AND retry_rejected IS NULL AND confirmed IS NULL) OR (kind = 'resolve' AND limit_count IS NULL AND outbox_id IS NOT NULL AND entry_id IS NOT NULL AND if_revision IS NOT NULL AND retry_rejected IS NOT NULL AND confirmed = 1 AND confirmed IS NOT NULL))
) STRICT`},
	{`table`, `pending_sync_roots`, `pending_sync_roots`, `CREATE TABLE pending_sync_roots (
    request_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    outbox_id TEXT NOT NULL,
    CHECK (ordinal >= 0),
    PRIMARY KEY (request_id,ordinal),
    UNIQUE (request_id,outbox_id),
    FOREIGN KEY (request_id) REFERENCES pending_sync(request_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (outbox_id) REFERENCES outbox(id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `recovery_decisions`, `recovery_decisions`, `CREATE TABLE recovery_decisions (
    uncertainty_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    previous_revision BLOB NOT NULL,
    resolution_end_sec INTEGER NOT NULL,
    resolution_end_nsec INTEGER NOT NULL,
    resolution_end_json TEXT NOT NULL,
    discarded INTEGER NOT NULL,
    reason TEXT NOT NULL,
    observed_capability TEXT,
    observed_wall_sec INTEGER,
    observed_wall_nsec INTEGER,
    observed_wall_json TEXT,
    observed_epoch TEXT,
    observed_elapsed_raw TEXT,
    observed_awake_raw TEXT,
    discarded_start_sec INTEGER,
    discarded_start_nsec INTEGER,
    discarded_start_json TEXT,
    discarded_end_sec INTEGER,
    discarded_end_nsec INTEGER,
    discarded_end_json TEXT,
    CHECK (previous_revision IS NULL OR (length(previous_revision) = 8 AND previous_revision != X'0000000000000000')),
    CHECK (resolution_end_nsec IS NULL OR (resolution_end_nsec >= 0 AND resolution_end_nsec < 1000000000)),
    CHECK (discarded IN (0,1)),
    CHECK (observed_wall_nsec IS NULL OR (observed_wall_nsec >= 0 AND observed_wall_nsec < 1000000000)),
    CHECK ((observed_wall_sec IS NULL AND observed_wall_nsec IS NULL AND observed_wall_json IS NULL) OR (observed_wall_sec IS NOT NULL AND observed_wall_nsec IS NOT NULL AND observed_wall_json IS NOT NULL)),
    CHECK ((observed_capability IS NULL AND observed_wall_json IS NULL AND observed_epoch IS NULL AND observed_elapsed_raw IS NULL AND observed_awake_raw IS NULL) OR (observed_capability IS NOT NULL AND observed_wall_json IS NOT NULL)),
    CHECK (discarded_start_nsec IS NULL OR (discarded_start_nsec >= 0 AND discarded_start_nsec < 1000000000)),
    CHECK ((discarded_start_sec IS NULL AND discarded_start_nsec IS NULL AND discarded_start_json IS NULL) OR (discarded_start_sec IS NOT NULL AND discarded_start_nsec IS NOT NULL AND discarded_start_json IS NOT NULL)),
    CHECK (discarded_end_nsec IS NULL OR (discarded_end_nsec >= 0 AND discarded_end_nsec < 1000000000)),
    CHECK ((discarded_end_sec IS NULL AND discarded_end_nsec IS NULL AND discarded_end_json IS NULL) OR (discarded_end_sec IS NOT NULL AND discarded_end_nsec IS NOT NULL AND discarded_end_json IS NOT NULL)),
    PRIMARY KEY (uncertainty_id),
    FOREIGN KEY (uncertainty_id) REFERENCES uncertainties(uncertainty_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (request_id) REFERENCES requests(request_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK ((discarded_start_sec IS NULL) = (discarded_end_sec IS NULL)),
    CHECK (discarded = 1 OR observed_wall_sec IS NOT NULL)
) STRICT`},
	{`table`, `requests`, `requests`, `CREATE TABLE requests (
    request_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    outcome_kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (request_id),
    CHECK (length(CAST(fingerprint AS BLOB)) = 64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
    CHECK (outcome_kind IN ('error','binding_result','mutation_result','sync_configuration_result','sync_run','pending_sync')),
    CHECK (operation IN ('bindings.link','bindings.repair','bindings.unlink','activity.resolve','activity.interrupt','activity.observe_source','activity.observe_clock','activity.observe_host','sync.configure','sync.pause','sync.resume','sync.now','sync.reconcile','sync.resolve'))
) STRICT`},
	{`table`, `segment_events`, `segment_events`, `CREATE TABLE segment_events (
    segment_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    event_reference TEXT NOT NULL,
    CHECK (ordinal >= 0),
    PRIMARY KEY (segment_id,ordinal),
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `segments`, `segments`, `CREATE TABLE segments (
    segment_id TEXT NOT NULL,
    actor_key TEXT NOT NULL,
    actor_generation BLOB NOT NULL,
    binding_id TEXT NOT NULL,
    binding_revision BLOB NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    group_order TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    start_sample_capability TEXT NOT NULL,
    start_sample_wall_sec INTEGER NOT NULL,
    start_sample_wall_nsec INTEGER NOT NULL,
    start_sample_wall_json TEXT NOT NULL,
    start_sample_epoch TEXT NOT NULL,
    start_sample_elapsed_raw TEXT NOT NULL,
    start_sample_awake_raw TEXT NOT NULL,
    start_sample_elapsed BLOB NOT NULL,
    start_sample_awake BLOB NOT NULL,
    confirmed_sample_capability TEXT NOT NULL,
    confirmed_sample_wall_sec INTEGER NOT NULL,
    confirmed_sample_wall_nsec INTEGER NOT NULL,
    confirmed_sample_wall_json TEXT NOT NULL,
    confirmed_sample_epoch TEXT NOT NULL,
    confirmed_sample_elapsed_raw TEXT NOT NULL,
    confirmed_sample_awake_raw TEXT NOT NULL,
    confirmed_sample_elapsed BLOB NOT NULL,
    confirmed_sample_awake BLOB NOT NULL,
    start_sec INTEGER NOT NULL,
    start_nsec INTEGER NOT NULL,
    start_json TEXT NOT NULL,
    confirmed_sec INTEGER NOT NULL,
    confirmed_nsec INTEGER NOT NULL,
    confirmed_json TEXT NOT NULL,
    end_sec INTEGER,
    end_nsec INTEGER,
    end_json TEXT,
    uncertainty_id TEXT,
    finalized INTEGER NOT NULL,
    CHECK (actor_generation IS NULL OR (length(actor_generation) = 8 AND actor_generation != X'0000000000000000')),
    FOREIGN KEY (actor_key,actor_generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    CHECK (binding_revision IS NULL OR (length(binding_revision) = 8 AND binding_revision != X'0000000000000000')),
    CHECK (start_sample_wall_nsec IS NULL OR (start_sample_wall_nsec >= 0 AND start_sample_wall_nsec < 1000000000)),
    CHECK (start_sample_elapsed IS NULL OR (length(start_sample_elapsed) = 8)),
    CHECK (start_sample_awake IS NULL OR (length(start_sample_awake) = 8)),
    CHECK (start_sample_capability IS NULL OR start_sample_capability = 'available'),
    CHECK (start_sample_elapsed IS NULL OR start_sample_elapsed <= X'7FFFFFFFFFFFFFFF'),
    CHECK (start_sample_awake IS NULL OR start_sample_awake <= X'7FFFFFFFFFFFFFFF'),
    CHECK (confirmed_sample_wall_nsec IS NULL OR (confirmed_sample_wall_nsec >= 0 AND confirmed_sample_wall_nsec < 1000000000)),
    CHECK (confirmed_sample_elapsed IS NULL OR (length(confirmed_sample_elapsed) = 8)),
    CHECK (confirmed_sample_awake IS NULL OR (length(confirmed_sample_awake) = 8)),
    CHECK (confirmed_sample_capability IS NULL OR confirmed_sample_capability = 'available'),
    CHECK (confirmed_sample_elapsed IS NULL OR confirmed_sample_elapsed <= X'7FFFFFFFFFFFFFFF'),
    CHECK (confirmed_sample_awake IS NULL OR confirmed_sample_awake <= X'7FFFFFFFFFFFFFFF'),
    CHECK (start_nsec IS NULL OR (start_nsec >= 0 AND start_nsec < 1000000000)),
    CHECK (confirmed_nsec IS NULL OR (confirmed_nsec >= 0 AND confirmed_nsec < 1000000000)),
    CHECK (end_nsec IS NULL OR (end_nsec >= 0 AND end_nsec < 1000000000)),
    CHECK ((end_sec IS NULL AND end_nsec IS NULL AND end_json IS NULL) OR (end_sec IS NOT NULL AND end_nsec IS NOT NULL AND end_json IS NOT NULL)),
    CHECK (finalized IN (0,1)),
    PRIMARY KEY (segment_id),
    FOREIGN KEY (epoch_id) REFERENCES epochs(epoch_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (uncertainty_id) REFERENCES uncertainties(uncertainty_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (confirmed_sec > start_sec OR (confirmed_sec = start_sec AND confirmed_nsec >= start_nsec))
) STRICT`},
	{`table`, `store_meta`, `store_meta`, `CREATE TABLE store_meta (
    singleton INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    legacy_schema_version INTEGER NOT NULL,
    computer_id TEXT NOT NULL,
    revision BLOB NOT NULL,
    sync_enabled INTEGER NOT NULL,
    durability_nonce BLOB NOT NULL,
    state_basename TEXT NOT NULL,
    database_basename TEXT NOT NULL,
    migration_id TEXT,
    backup_sha256 TEXT,
    logical_bytes INTEGER NOT NULL CHECK (logical_bytes >= 0),
    CHECK (revision IS NULL OR (length(revision) = 8 AND revision != X'0000000000000000')),
    CHECK (sync_enabled IN (0,1)),
    PRIMARY KEY (singleton),
    CHECK (singleton = 1),
    CHECK (schema_version = 1),
    CHECK (legacy_schema_version = 1),
    CHECK (length(durability_nonce) = 16),
    CHECK ((migration_id IS NULL) = (backup_sha256 IS NULL)),
    CHECK (length(state_basename) > 0 AND instr(state_basename,'/') = 0),
    CHECK (length(database_basename) > 0 AND instr(database_basename,'/') = 0)
) STRICT`},
	{`table`, `sync_attempts`, `sync_attempts`, `CREATE TABLE sync_attempts (
    interval_id TEXT NOT NULL,
    part_ordinal INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    request_id TEXT NOT NULL,
    id TEXT NOT NULL,
    number TEXT NOT NULL,
    state TEXT NOT NULL,
    entry_id TEXT,
    failure_category TEXT,
    CHECK (part_ordinal >= 0),
    CHECK (ordinal >= 0),
    PRIMARY KEY (interval_id,part_ordinal,ordinal),
    UNIQUE (id),
    FOREIGN KEY (request_id) REFERENCES requests(request_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (interval_id,part_ordinal) REFERENCES sync_parts(interval_id,ordinal) DEFERRABLE INITIALLY DEFERRED,
    CHECK (number = CAST(ordinal+1 AS TEXT)),
    CHECK (state IN ('submitting','synced','rejected','unknown','needs_attention'))
) STRICT`},
	{`table`, `sync_configurations`, `sync_configurations`, `CREATE TABLE sync_configurations (
    config_key TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    revision BLOB NOT NULL,
    mode TEXT NOT NULL,
    duration_policy TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    clock TEXT,
    declared INTEGER NOT NULL,
    declared_at_sec INTEGER NOT NULL,
    declared_at_nsec INTEGER NOT NULL,
    declared_at_json TEXT NOT NULL,
    source TEXT NOT NULL,
    CHECK (revision IS NULL OR (length(revision) = 8 AND revision != X'0000000000000000')),
    CHECK (declared IN (0,1)),
    CHECK (declared_at_nsec IS NULL OR (declared_at_nsec >= 0 AND declared_at_nsec < 1000000000)),
    CHECK (mode IN ('duration','timestamp') AND duration_policy IN ('exact','nearest-hundredth-hour')),
    CHECK (policy_version = duration_policy || '-v1' AND declared = 1 AND source = 'user_declared'),
    CHECK ((mode = 'duration' AND (clock IS NULL OR clock = '')) OR (mode = 'timestamp' AND duration_policy = 'exact' AND clock IN ('12h','24h') AND clock IS NOT NULL)),
    PRIMARY KEY (config_key),
    UNIQUE (account_id,user_id)
) STRICT`},
	{`table`, `sync_parts`, `sync_parts`, `CREATE TABLE sync_parts (
    interval_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    id TEXT NOT NULL,
    spent_date TEXT NOT NULL,
    duration_ns INTEGER NOT NULL,
    start_sec INTEGER NOT NULL,
    start_nsec INTEGER NOT NULL,
    start_json TEXT NOT NULL,
    end_sec INTEGER NOT NULL,
    end_nsec INTEGER NOT NULL,
    end_json TEXT NOT NULL,
    planned_hours TEXT NOT NULL,
    planned_duration_ns INTEGER NOT NULL,
    planned_residual_ns INTEGER NOT NULL,
    started_time TEXT,
    ended_time TEXT,
    correlation TEXT NOT NULL,
    notes TEXT NOT NULL,
    state TEXT NOT NULL,
    entry_id TEXT,
    failure_category TEXT,
    returned_hours TEXT,
    rounded_hours TEXT,
    confirmed_duration_ns INTEGER,
    provider_delta_ns INTEGER,
    total_residual_ns INTEGER,
    attachment_request_id TEXT,
    attachment_entry_id TEXT,
    CHECK (ordinal >= 0),
    CHECK (start_nsec IS NULL OR (start_nsec >= 0 AND start_nsec < 1000000000)),
    CHECK (end_nsec IS NULL OR (end_nsec >= 0 AND end_nsec < 1000000000)),
    PRIMARY KEY (interval_id,ordinal),
    UNIQUE (id),
    FOREIGN KEY (interval_id) REFERENCES sync_plans(interval_id) DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (attachment_request_id) REFERENCES requests(request_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (ordinal < 100 AND duration_ns > 0 AND planned_duration_ns > 0),
    CHECK (state IN ('queued','submitting','synced','rejected','unknown','needs_attention')),
    CHECK (correlation = 'tempo:v1:' || id),
    CHECK ((attachment_request_id IS NULL) = (attachment_entry_id IS NULL)),
    CHECK (end_sec > start_sec OR (end_sec = start_sec AND end_nsec > start_nsec))
) STRICT`},
	{`table`, `sync_plans`, `sync_plans`, `CREATE TABLE sync_plans (
    interval_id TEXT NOT NULL,
    company_source TEXT NOT NULL,
    config_account_id TEXT NOT NULL,
    config_user_id TEXT NOT NULL,
    config_revision BLOB NOT NULL,
    config_mode TEXT NOT NULL,
    config_duration_policy TEXT NOT NULL,
    config_policy_version TEXT NOT NULL,
    config_clock TEXT,
    config_declared INTEGER NOT NULL,
    config_declared_at_sec INTEGER NOT NULL,
    config_declared_at_nsec INTEGER NOT NULL,
    config_declared_at_json TEXT NOT NULL,
    config_source TEXT NOT NULL,
    CHECK (config_revision IS NULL OR (length(config_revision) = 8 AND config_revision != X'0000000000000000')),
    CHECK (config_declared IN (0,1)),
    CHECK (config_declared_at_nsec IS NULL OR (config_declared_at_nsec >= 0 AND config_declared_at_nsec < 1000000000)),
    CHECK (config_mode IN ('duration','timestamp') AND config_duration_policy IN ('exact','nearest-hundredth-hour')),
    CHECK (config_policy_version = config_duration_policy || '-v1' AND config_declared = 1 AND config_source = 'user_declared'),
    CHECK ((config_mode = 'duration' AND (config_clock IS NULL OR config_clock = '')) OR (config_mode = 'timestamp' AND config_duration_policy = 'exact' AND config_clock IN ('12h','24h') AND config_clock IS NOT NULL)),
    PRIMARY KEY (interval_id),
    FOREIGN KEY (interval_id) REFERENCES outbox(interval_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (company_source IN ('company_verified','user_declared_fallback'))
) STRICT`},
	{`table`, `uncertainties`, `uncertainties`, `CREATE TABLE uncertainties (
    uncertainty_id TEXT NOT NULL,
    revision BLOB NOT NULL,
    actor_key TEXT NOT NULL,
    actor_generation BLOB NOT NULL,
    segment_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    lower_bound_sec INTEGER NOT NULL,
    lower_bound_nsec INTEGER NOT NULL,
    lower_bound_json TEXT NOT NULL,
    upper_bound_sec INTEGER,
    upper_bound_nsec INTEGER,
    upper_bound_json TEXT,
    reason TEXT NOT NULL,
    state TEXT NOT NULL,
    resolution_end_sec INTEGER,
    resolution_end_nsec INTEGER,
    resolution_end_json TEXT,
    discarded INTEGER NOT NULL,
    CHECK (revision IS NULL OR (length(revision) = 8 AND revision != X'0000000000000000')),
    CHECK (actor_generation IS NULL OR (length(actor_generation) = 8 AND actor_generation != X'0000000000000000')),
    FOREIGN KEY (actor_key,actor_generation) REFERENCES actor_generations(actor_key,generation) DEFERRABLE INITIALLY DEFERRED,
    CHECK (lower_bound_nsec IS NULL OR (lower_bound_nsec >= 0 AND lower_bound_nsec < 1000000000)),
    CHECK (upper_bound_nsec IS NULL OR (upper_bound_nsec >= 0 AND upper_bound_nsec < 1000000000)),
    CHECK ((upper_bound_sec IS NULL AND upper_bound_nsec IS NULL AND upper_bound_json IS NULL) OR (upper_bound_sec IS NOT NULL AND upper_bound_nsec IS NOT NULL AND upper_bound_json IS NOT NULL)),
    CHECK (resolution_end_nsec IS NULL OR (resolution_end_nsec >= 0 AND resolution_end_nsec < 1000000000)),
    CHECK ((resolution_end_sec IS NULL AND resolution_end_nsec IS NULL AND resolution_end_json IS NULL) OR (resolution_end_sec IS NOT NULL AND resolution_end_nsec IS NOT NULL AND resolution_end_json IS NOT NULL)),
    CHECK (discarded IN (0,1)),
    PRIMARY KEY (uncertainty_id),
    FOREIGN KEY (segment_id) REFERENCES segments(segment_id) DEFERRABLE INITIALLY DEFERRED,
    CHECK (state IN ('unresolved','resolved')),
    CHECK (reason IN ('source_lost','ordering_unavailable','suspend','clock_changed','restart_unknown','event_gap','superseded')),
    CHECK (upper_bound_sec IS NULL OR upper_bound_sec > lower_bound_sec OR (upper_bound_sec = lower_bound_sec AND upper_bound_nsec >= lower_bound_nsec)),
    CHECK ((state = 'unresolved' AND resolution_end_sec IS NULL AND discarded = 0) OR (state = 'resolved' AND resolution_end_sec IS NOT NULL))
) STRICT`},
	{`table`, `uncertainty_evidence`, `uncertainty_evidence`, `CREATE TABLE uncertainty_evidence (
    uncertainty_id TEXT NOT NULL,
    detection_capability TEXT NOT NULL,
    detection_wall_sec INTEGER NOT NULL,
    detection_wall_nsec INTEGER NOT NULL,
    detection_wall_json TEXT NOT NULL,
    detection_epoch TEXT,
    detection_elapsed_raw TEXT,
    detection_awake_raw TEXT,
    last_confirmed_capability TEXT NOT NULL,
    last_confirmed_wall_sec INTEGER NOT NULL,
    last_confirmed_wall_nsec INTEGER NOT NULL,
    last_confirmed_wall_json TEXT NOT NULL,
    last_confirmed_epoch TEXT NOT NULL,
    last_confirmed_elapsed_raw TEXT NOT NULL,
    last_confirmed_awake_raw TEXT NOT NULL,
    last_confirmed_elapsed BLOB NOT NULL,
    last_confirmed_awake BLOB NOT NULL,
    bound_capability TEXT,
    bound_wall_sec INTEGER,
    bound_wall_nsec INTEGER,
    bound_wall_json TEXT,
    bound_epoch TEXT,
    bound_elapsed_raw TEXT,
    bound_awake_raw TEXT,
    bound_elapsed BLOB,
    bound_awake BLOB,
    missing_from TEXT NOT NULL,
    missing_through TEXT NOT NULL,
    CHECK (detection_wall_nsec IS NULL OR (detection_wall_nsec >= 0 AND detection_wall_nsec < 1000000000)),
    CHECK (last_confirmed_wall_nsec IS NULL OR (last_confirmed_wall_nsec >= 0 AND last_confirmed_wall_nsec < 1000000000)),
    CHECK (last_confirmed_elapsed IS NULL OR (length(last_confirmed_elapsed) = 8)),
    CHECK (last_confirmed_awake IS NULL OR (length(last_confirmed_awake) = 8)),
    CHECK (last_confirmed_capability IS NULL OR last_confirmed_capability = 'available'),
    CHECK (last_confirmed_elapsed IS NULL OR last_confirmed_elapsed <= X'7FFFFFFFFFFFFFFF'),
    CHECK (last_confirmed_awake IS NULL OR last_confirmed_awake <= X'7FFFFFFFFFFFFFFF'),
    CHECK (bound_wall_nsec IS NULL OR (bound_wall_nsec >= 0 AND bound_wall_nsec < 1000000000)),
    CHECK ((bound_wall_sec IS NULL AND bound_wall_nsec IS NULL AND bound_wall_json IS NULL) OR (bound_wall_sec IS NOT NULL AND bound_wall_nsec IS NOT NULL AND bound_wall_json IS NOT NULL)),
    CHECK (bound_elapsed IS NULL OR (length(bound_elapsed) = 8)),
    CHECK (bound_awake IS NULL OR (length(bound_awake) = 8)),
    CHECK (bound_capability IS NULL OR bound_capability = 'available'),
    CHECK (bound_elapsed IS NULL OR bound_elapsed <= X'7FFFFFFFFFFFFFFF'),
    CHECK (bound_awake IS NULL OR bound_awake <= X'7FFFFFFFFFFFFFFF'),
    CHECK ((bound_capability IS NULL AND bound_wall_sec IS NULL AND bound_wall_nsec IS NULL AND bound_wall_json IS NULL AND bound_epoch IS NULL AND bound_elapsed_raw IS NULL AND bound_awake_raw IS NULL AND bound_elapsed IS NULL AND bound_awake IS NULL) OR (bound_capability IS NOT NULL AND bound_wall_sec IS NOT NULL AND bound_wall_nsec IS NOT NULL AND bound_wall_json IS NOT NULL AND bound_epoch IS NOT NULL AND bound_elapsed_raw IS NOT NULL AND bound_awake_raw IS NOT NULL AND bound_elapsed IS NOT NULL AND bound_awake IS NOT NULL)),
    PRIMARY KEY (uncertainty_id),
    FOREIGN KEY (uncertainty_id) REFERENCES uncertainties(uncertainty_id) DEFERRABLE INITIALLY DEFERRED
) STRICT`},
	{`table`, `union_frontier`, `union_frontier`, `CREATE TABLE union_frontier (
    component_id TEXT NOT NULL,
    computer_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    timezone TEXT NOT NULL,
    group_order TEXT NOT NULL,
    start_sec INTEGER NOT NULL,
    start_nsec INTEGER NOT NULL,
    start_json TEXT NOT NULL,
    end_sec INTEGER NOT NULL,
    end_nsec INTEGER NOT NULL,
    end_json TEXT NOT NULL,
    CHECK (start_nsec IS NULL OR (start_nsec >= 0 AND start_nsec < 1000000000)),
    CHECK (end_nsec IS NULL OR (end_nsec >= 0 AND end_nsec < 1000000000)),
    PRIMARY KEY (component_id),
    CHECK (end_sec > start_sec OR (end_sec = start_sec AND end_nsec > start_nsec))
) STRICT`},
	{`trigger`, `outbox_id_insert`, `outbox`, `CREATE TRIGGER outbox_id_insert
BEFORE INSERT ON outbox
WHEN EXISTS (SELECT 1 FROM sync_parts WHERE id = NEW.id) OR EXISTS (SELECT 1 FROM sync_attempts WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT,'activity identity conflict');
END`},
	{`trigger`, `outbox_id_update`, `outbox`, `CREATE TRIGGER outbox_id_update
BEFORE UPDATE OF id ON outbox
WHEN EXISTS (SELECT 1 FROM sync_parts WHERE id = NEW.id) OR EXISTS (SELECT 1 FROM sync_attempts WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT,'activity identity conflict');
END`},
	{`trigger`, `sync_attempts_id_insert`, `sync_attempts`, `CREATE TRIGGER sync_attempts_id_insert
BEFORE INSERT ON sync_attempts
WHEN EXISTS (SELECT 1 FROM outbox WHERE id = NEW.id) OR EXISTS (SELECT 1 FROM sync_parts WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT,'activity identity conflict');
END`},
	{`trigger`, `sync_attempts_id_update`, `sync_attempts`, `CREATE TRIGGER sync_attempts_id_update
BEFORE UPDATE OF id ON sync_attempts
WHEN EXISTS (SELECT 1 FROM outbox WHERE id = NEW.id) OR EXISTS (SELECT 1 FROM sync_parts WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT,'activity identity conflict');
END`},
	{`trigger`, `sync_parts_id_insert`, `sync_parts`, `CREATE TRIGGER sync_parts_id_insert
BEFORE INSERT ON sync_parts
WHEN EXISTS (SELECT 1 FROM outbox WHERE id = NEW.id) OR EXISTS (SELECT 1 FROM sync_attempts WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT,'activity identity conflict');
END`},
	{`trigger`, `sync_parts_id_update`, `sync_parts`, `CREATE TRIGGER sync_parts_id_update
BEFORE UPDATE OF id ON sync_parts
WHEN EXISTS (SELECT 1 FROM outbox WHERE id = NEW.id) OR EXISTS (SELECT 1 FROM sync_attempts WHERE id = NEW.id)
BEGIN
    SELECT RAISE(ABORT,'activity identity conflict');
END`},
}

// One extra row preserves the refusal of foreign catalog entries. The type
// guard prevents JSON coercion from hiding a non-TEXT native catalog value.
const sqliteCaptureCatalogSQL = `SELECT json_group_array(
  CASE WHEN typeof(type)='text' AND typeof(name)='text'
         AND typeof(tbl_name)='text' AND typeof(sql)='text'
       THEN json_array(type,name,tbl_name,sql)
       ELSE NULL END
  ORDER BY type,name
)
FROM (
  SELECT type,name,tbl_name,sql
  FROM sqlite_schema
  WHERE name NOT GLOB 'sqlite_*'
  ORDER BY type,name
  LIMIT ?
)`

func sqliteCheckCaptureCatalogJSON(value string) error {
	if !utf8.ValidString(value) {
		return failure("state_corrupt")
	}
	d := json.NewDecoder(strings.NewReader(value))
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return failure("state_corrupt")
	}
	for _, expected := range sqliteCaptureCatalog {
		token, err = d.Token()
		if err != nil || token != json.Delim('[') {
			return failure("state_corrupt")
		}
		for _, want := range expected {
			token, err = d.Token()
			got, text := token.(string)
			if err != nil || !text || got != want {
				return failure("state_corrupt")
			}
		}
		token, err = d.Token()
		if err != nil || token != json.Delim(']') {
			return failure("state_corrupt")
		}
	}
	token, err = d.Token()
	if err != nil || token != json.Delim(']') {
		return failure("state_corrupt")
	}
	if _, err = d.Token(); err != io.EOF {
		return failure("state_corrupt")
	}
	return nil
}

func sqliteReadCaptureSchema(tx *sqliteio.Tx, authority, database string) (result sqliteStoreMeta, err error) {
	s, err := tx.Prepare(sqliteCaptureCatalogSQL, sqliteio.Integer(int64(len(sqliteCaptureCatalog)+1)))
	if err != nil {
		return result, err
	}
	row, err := s.Step()
	if err == nil && (!row || s.ColumnCount() != 1) {
		err = failure("state_corrupt")
	}
	if err == nil {
		kind, e := s.Kind(0)
		err = e
		if err == nil && kind != sqliteio.TextKind {
			err = failure("state_corrupt")
		}
	}
	if err == nil {
		value, e := s.Text(0)
		err = e
		if err == nil {
			err = sqliteCheckCaptureCatalogJSON(value)
		}
	}
	if err == nil {
		row, e := s.Step()
		err = e
		if err == nil && row {
			err = failure("state_corrupt")
		}
	}
	if err = sqliteCloseMetaStatement(s, err); err != nil {
		return sqliteStoreMeta{}, err
	}
	result, err = sqliteReadMeta(tx, authority, database)
	if err != nil {
		return sqliteStoreMeta{}, err
	}
	if result.MigrationID != nil || result.BackupSHA256 != nil {
		return sqliteStoreMeta{}, failure("state_corrupt")
	}
	return result, nil
}
