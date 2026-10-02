# Architecture and boundaries

Tempo is a small Go executable with golang.org/x/term and x/sys for terminal ownership. macOS builds use cgo for the system Security/CoreFoundation credential bridge and native awake/suspend clock counters. No runtime daemon, Go installation, external library download, external credential utility, or package manager is required to run the built binary. Stripping uses normal Go linker flags, never executable packers.

`cmd/tempo` supplies cancellation and a two-minute finite-command deadline. Eligible interactive setup/link sessions have no command-wide timeout; each API action remains bounded. `internal/cli` owns validation, account/user scope and command behavior. `internal/harvest.Provider` is the small injected provider seam; the Harvest adapter owns HTTP, pagination and safe errors. `internal/auth.Store` owns credential persistence, separate from account configuration. A future tracker can implement the small provider boundary and map its data at the command boundary; no registry, generic plugin system or speculative framework is present.

## Credentials

`HARVEST_TOKEN` overrides the native Keychain store. Token input is limited to 16 KiB, trims surrounding whitespace and rejects embedded whitespace/control characters. Tokens cannot be supplied by argument. macOS storage uses native Security.framework APIs under service `io.beene.tempo`, account `harvest-token`; the library never shells out. A denied/locked store is an error, never plaintext fallback. Non-macOS and non-cgo builds require environment authentication.

Configuration contains only `account_id`. It is validated strictly, written atomically with mode 0600, and newly created directories use 0700. Existing parent permissions are not changed. Login validates the token and selected Harvest account before changing local storage, prepares configuration, then replaces the token; storage failure restores the previous account where possible. Partial local persistence failures report the affected step without secrets. Logout removes the saved token and account, but cannot unset a parent environment or revoke a Harvest token.

Development and CI use injected synthetic stores and mock servers. No test calls the actual Keychain. The native bridge is compiled and vetted, while real user credential provisioning remains a manual acceptance step; helper operations never open OS permission prompts.

## API and reliability

Production origins are fixed: `api.harvestapp.com/v2` and `id.getharvest.com/api/v2`. Redirects are denied. Pagination follows `links.next`, accepting only the same origin and exact endpoint path; it rejects loops and incomplete metadata. Continuations must preserve every initial filter and value; only pagination control parameters may change. Limits are 100 pages and 8 MiB per response. Limits cause failure, never silent truncation or partial success. Requests have at most 30 seconds per HTTP attempt and the command has a two-minute deadline.

Only GET requests retry, at most three attempts. Retry-After delays are capped at five seconds. POST/PATCH/DELETE are never automatically replayed. A transport failure, invalid success response, or server failure can leave a write's outcome unknown: exit 8 and `uncertain_write` require read-only reconciliation before a manual retry. API bodies and raw transport errors never enter diagnostics; the CLI adds a second fixed-message boundary for provider errors.

Current user identity comes from `/users/me`, not Harvest ID's accounts response. ID-based time operations verify ownership even with administrator credentials. Time lists and running-timer discovery are scoped to that user. Timer preflight spans all dates and refuses incomplete discovery. Same running timer is a no-op; a different running timer requires an explicit stop command. There is no multi-write auto-switch. Harvest does not provide an atomic preflight-and-write primitive; another client can race the checks. Read back timer status when working across clients.

Ordinary create always supplies completed duration or a complete timestamp pair. Company timer mode is checked before time-mode writes. Omitted hours and explicit zero are distinct. Relative dates use the computer's local calendar; numeric dates are sent unchanged. Time-of-day values use the Harvest account's interpretation, not UTC conversion. Same-day ranges only; split overnight entries. DST is resolved by Harvest, so inspect returned hours on transition dates.

## Verification and infrastructure

Public PR verification uses one ephemeral GitHub-hosted Ubuntu job, read-only token and no matrix. The former private-repository Titan CI workflow is disabled. The dedicated Titan service is disabled/stopped; editable workflow conditions are not an isolation boundary. Each push to `main` triggers one hosted macOS build followed by one hosted Ubuntu build/publish job. The workflow automatically chooses a stable version, tags the triggering commit after validation, and publishes the complete release. No release job depends on Titan. See [distribution.md](distribution.md). Both platforms pin Go 1.27.1 and GoReleaser 2.18.2. No AWS resources are involved.

## Planned local agent activity

The [agent activity contract](agent-contracts.md) specifies the next implementation slices: one local union timer per computer/account/project, durable actor history and uncertainty, immutable attribution, and shared CLI/UI actions. Its [operation catalog](agent-operations.json) and [acceptance vectors](agent-acceptance.json) are design contracts, not shipped features. They preserve the existing Harvest provider and direct time/timer commands. The finite foreground sync service/CLI and guided setup/link lifetimes are implemented; the optional worker is implemented and the terminal dashboard remains planned.

## Durable local activity

`internal/activity.Service` is the shared local operation boundary. `activity status` and normalized `activity event` execute before account configuration, credential stores or Harvest providers. Construction is lazy. The default private state is the OS user config directory under `tempo/activity-state.json`; `TEMPO_STATE` or an injected service isolates tests. Explicit linking initializes computer identity and validated binding snapshots; unlinked ingress is a no-op. Actor attribution is fixed on first work and later callbacks never re-resolve their current directory.

The v1 store retains actor segments, accepted identity fingerprints, generation high-water state, uncertainty evidence, immutable union intervals and one queued intent per interval. A stable advisory lock serializes processes; default acquisition is bounded to 250ms and lifecycle CLI work has a one-second deadline. Writes use a private same-directory temporary file, file sync, atomic rename and directory sync. A duplicate receipt is re-synchronized before success so a prior `local_write_unknown` does not become a false durability acknowledgment. Callback failure aborts; durable quarantine followed by a lifecycle error is an explicit committed rejection. State decoding/encoding is bounded at 32MiB; reaching capacity fails safely without deleting history. Newer/corrupt state, symlink or replacement hazards, unsafe permissions and invalid references are preserved for review.

Actor ranges use a shared elapsed-coordinate/UTC anchor per project attribution and continuous clock epoch, avoiding overlapping-actor drift from tolerated wall differences. macOS reads `mach_continuous_time`, `mach_absolute_time` and boot identity; Linux reads `CLOCK_BOOTTIME`, `CLOCK_MONOTONIC` and boot ID without cgo. Missing capability never falls back to wall subtraction. Sleep, epoch changes and divergent clocks quarantine unknown tails; normal silence does not. Status does not extend evidence or write health. Unresolved ranges reserve their possible time, including endpoint contact, so only disconnected safe closed union components finalize. Recovery and binding management share this engine; source bridges remain separate dependent operations. Queued output submits only through an explicit sync pass or the optional worker, never during capture.


## Local binding operations

`internal/activity.Service` owns `Link`, `ListBindings`, `ShowBinding`, `Unlink` and `RepairBinding`. `DiscoverLocation` is a read-only scope preview shared with linking. Git discovery uses one bounded, sanitized `rev-parse` observation for common directory, checkout root and worktree status. Linking checks that observation again after remote validation and inside the committing transaction. Ordinary directory lookup selects the nearest ancestor only outside Git.

Link takes lazy account-resolution and account-scoped provider factories. Explicit account intent is fingerprinted separately from inferred configuration; persisted request replay happens before either callback or filesystem discovery. Remote reads complete before acquiring the mutation lock. The lock then rechecks request identity, revision, active actors and attribution compatibility. The first successful transaction initializes a random local computer ID. Typed request receipts and binding metadata commit with the mutation; replay includes the existing engine durability barrier. Unlink retains a tombstone and removes only the active compatibility entry; historical actor/segment/interval snapshots are independent.

Binding and receipt decoding validates finite operation/result combinations, canonical IDs/revisions, absolute scope locators and agreement between live records and active snapshots. No credentials, provider payloads or Git output are stored. First-store concurrency uses the same pinned-directory and no-follow lock boundary as other engine transactions. Local inspection/mutation commands bypass account configuration and authentication; only new link validation invokes Harvest reads.


## Audited recovery and positive observations

`Service.Review`, `Preview`, `Resolve` and `Interrupt` are shared CLI/UI operations. Preview performs pure projection with the same bound/union checks used under the resolution lock. Original segment confirmed samples remain immutable; an audited resolved end is stored separately and supplies effective union support. Decode/precommit validation requires the matching decision, uncertainty, end, request receipt and immutable interval coverage. Resolved history is retained, while review filters only unresolved ranges. Discarding an unknown tail can use persisted evidence without inventing a current-time bound.

`ObserveSource` accepts positive source loss, ordering unavailability or unknown restart continuity for an exact actor generation. `ObserveClock` persists computer-wide discontinuities. Neither extends confirmed work from silence or clock polling; constructing a new Service does not by itself imply source loss. Both use the common typed request ledger, including no-op receipts, before sampling or inspecting newer work. Host-specific detection and integration remain separate.

Valid mutating recovery requests may discover a discontinuity before rejecting the requested time decision. The transaction then preserves quarantine and an exclusive safe error receipt; it does not claim successful detachment or resolution. Store durability errors take precedence, and same-ID replay checks durability before returning the original typed result/error. Pure syntax, revision and bounds failures without newly discovered safety evidence do not mutate state. A source observation's detection timestamp remains diagnostic evidence; erroneous forward jumps do not become permanent recovery bounds after the clock is corrected.

## Shared authentication and terminal setup

`internal/auth.Service` supplies login, logout, account selection, status and provider construction to CLI and future UI callers. Native access runs in an owned same-binary helper: bounded private inherited request/reply pipes carry at most 32 KiB of strict JSON; token input is capped at 16 KiB. No token is placed in arguments, inherited environment, files or ordinary output. The helper disables native UI. A five-second deadline closes pipes, kills/reaps the child and joins all I/O workers; a complete validated reply wins raced cancellation.

All authentication/config mutations acquire the same per-user credential-namespace lock, independent of config/state overrides. The inherited lock descriptor remains owned by the helper if its parent dies. The helper validates its secure expected identity and establishes/retains the lock. Login captures its rollback baseline under this lock, saves account configuration, then stores the token. Only a definite token rejection permits restoring the baseline. Logout deletes first, then clears config without parsing malformed old config. Ambiguous post-dispatch failures report `credential_write_unknown`, affected-resource effects, and exit 8. This does not prevent a late effect already accepted by the OS; reconciliation never claims causal certainty.

`internal/setup.Service` orchestrates separate confirmed operations and returns safe completed steps on later failures. `internal/terminal.Session` exclusively owns raw mode, close-on-exec duplicated descriptors and a bounded input pump. Raw Ctrl-C cancels the shared action context even while no prompt is waiting. Close joins the pump, restores original flags/termios and returns before process exit. Selection labels strip terminal controls. Linux/unsupported native backends guide users to environment authentication before collecting secrets. Tests use synthetic helper subprocesses, injected stores/providers and temporary PTYs only.


### Durable foreground synchronization

The activity service owns configuration, outbox claims, finite request receipts,
reconciliation and explicit attachment/rejected-retry controls. A separate private
process lock covers remote sync passes; ordinary store locks stay short so capture,
status and pause remain available. Account-bound provider construction uses the
bounded auth service only after exact replay and offline guards. No remote write
retries are introduced.

A complete daily plan precedes each POST intent. Exact captured nanoseconds,
explicitly represented planned time and acknowledged provider time are separate.
Nearest-hundredth-hour consent rounds each local day part once; exact policy keeps
an integer-nanosecond decimal round trip. Canonical versioned correlation markers
include immutable identity/bounds/policy/planned values. Returned hours use exact
rational comparison, with bounded nanosecond accounting and signed residuals.

The run lock owns pending-request/orphan recovery before pause/empty checks.
Acknowledgement loss retains unknown effect; a fully saved result can finalize
locally. Reconciliation follows complete pagination and checks collision candidates
without hiding them behind project/date/task filters. Manual attachment performs a
complete current-user marker scan. Definite rejected retry preserves the frozen
plan, successful parts and attempt audit. Setup distinguishes capture mapping from
explicit upload consent; no worker or terminal dashboard is installed here.


## Automatic worker ownership

`internal/worker` schedules the existing activity sync engine; it owns no second
outbox or provider. The process-lifetime worker lock, finite sync lock and short
control-journal lock serve different owners. Runtime pending UUIDs are durable
before SyncNow and recover before scheduling/paused checks. Controllers never
rewrite runtime metadata. A private Unix datagram endpoint coalesces wake hints,
rate-limits explicit rechecks and cancels Stop independently of wake capacity.
Read-only activity/sync/doctor views compose worker evidence after releasing the
activity transaction, with a 250 ms budget and no manager/credential access.

Service definitions are pinned to canonical state paths and exact executable and
config paths. Typed control intents precede manager effects; same-input replay
returns saved outcomes. Initial publication is atomic without replacement. macOS
persistent disablement precedes publication; Linux installation reloads an
unenabled user unit. Explicit Start enables startup, Stop disables it, and removal
preserves local activity and replay journals. Native commands are bounded and raw
manager diagnostics never become user-visible error messages.

## Native hook ingress and retained policy

`internal/hooks` decodes bounded native payloads into allowlisted metadata. `activity.Service.IngestHost` persists native incarnation/turn/tool mappings and receipts in the same transaction as the existing reducer. Root and child identities are separate; registered followups retain their context across cwd changes or link removal. `ObserveHost` resolves exactly one recorded native tuple and calls the same transaction-local recovery helper as `ObserveSource`. It rejects reused ambiguous identities without guessing a newer actor. Host receipts do not assert native delivery.

Codex and Claude share this transaction with source-specific event and tool rules.
Claude prompt IDs and optional child IDs retain exact identity. Pending
`AskUserQuestion` phases distinguish question waits from completed-turn
`wait_user`; both use the existing reducer, while lost question completion retains
a capture review without billing waiting time. Native failure kinds remain
distinct in receipts. Task callbacks carry no timing evidence. Ambiguous child
restart and session-exit identities cannot allocate arrival-based generations or
mutate a newer actor. See [Claude lifecycle capture](claude-hooks.md).

`internal/hookstate.Service` owns a separate private 4 MiB hooks metadata file, one lock and a typed request ledger. It initializes retained operator-declared capture policy without initializing computer identity. Preview hashes bounded no-follow regular artifacts without retaining content. Confirmation, revocation and durable invalidation use revision-sensitive fingerprints and atomic commits; identical request replay reconciles uncertain durability without resurrection. This is the single metadata domain for later hook installation manifests and request types. It is not another timing store. See [Codex lifecycle capture](codex-hooks.md) for policy sampling races, supported hosts and residual delivery limits.
