# Architecture and boundaries

Tempo is a standard-library Go executable. macOS builds use cgo solely to call the system Security and CoreFoundation frameworks. No runtime daemon, Go installation, external library download, subprocess credential command, or package manager is required to run the built binary. Stripping uses normal Go linker flags, never executable packers.

`cmd/tempo` supplies cancellation and a two-minute command deadline. `internal/cli` owns validation, account/user scope and command behavior. `internal/harvest.Provider` is the small injected provider seam; the Harvest adapter owns HTTP, pagination and safe errors. `internal/auth.Store` owns credential persistence, separate from account configuration. A future tracker can implement the small provider boundary and map its data at the command boundary; no registry, generic plugin system or speculative framework is present.

## Credentials

`HARVEST_TOKEN` overrides the native Keychain store. Token input is limited to 16 KiB, trims surrounding whitespace and rejects embedded whitespace/control characters. Tokens cannot be supplied by argument. macOS storage uses native Security.framework APIs under service `io.beene.tempo`, account `harvest-token`; the library never shells out. A denied/locked store is an error, never plaintext fallback. Non-macOS and non-cgo builds require environment authentication.

Configuration contains only `account_id`. It is validated strictly, written atomically with mode 0600, and newly created directories use 0700. Existing parent permissions are not changed. Login validates the token and selected Harvest account before changing local storage, prepares configuration, then replaces the token; storage failure restores the previous account where possible. Partial local persistence failures report the affected step without secrets. Logout removes the saved token and account, but cannot unset a parent environment or revoke a Harvest token.

Development and CI use injected synthetic stores and mock servers. No test calls the actual Keychain. The native bridge is compiled and vetted, but a real user's login and OS permission prompts remain a manual acceptance step.

## API and reliability

Production origins are fixed: `api.harvestapp.com/v2` and `id.getharvest.com/api/v2`. Redirects are denied. Pagination follows `links.next`, accepting only the same origin and exact endpoint path; it rejects loops and incomplete metadata. Continuations must preserve every initial filter and value; only pagination control parameters may change. Limits are 100 pages and 8 MiB per response. Limits cause failure, never silent truncation or partial success. Requests have at most 30 seconds per HTTP attempt and the command has a two-minute deadline.

Only GET requests retry, at most three attempts. Retry-After delays are capped at five seconds. POST/PATCH/DELETE are never automatically replayed. A transport failure, invalid success response, or server failure can leave a write's outcome unknown: exit 8 and `uncertain_write` require read-only reconciliation before a manual retry. API bodies and raw transport errors never enter diagnostics; the CLI adds a second fixed-message boundary for provider errors.

Current user identity comes from `/users/me`, not Harvest ID's accounts response. ID-based time operations verify ownership even with administrator credentials. Time lists and running-timer discovery are scoped to that user. Timer preflight spans all dates and refuses incomplete discovery. Same running timer is a no-op; a different running timer requires an explicit stop command. There is no multi-write auto-switch. Harvest does not provide an atomic preflight-and-write primitive; another client can race the checks. Read back timer status when working across clients.

Ordinary create always supplies completed duration or a complete timestamp pair. Company timer mode is checked before time-mode writes. Omitted hours and explicit zero are distinct. Relative dates use the computer's local calendar; numeric dates are sent unchanged. Time-of-day values use the Harvest account's interpretation, not UTC conversion. Same-day ranges only; split overnight entries. DST is resolved by Harvest, so inspect returned hours on transition dates.

## Verification and infrastructure

Public PR verification uses one ephemeral GitHub-hosted Ubuntu job, read-only token and no matrix. The former private-repository Titan CI workflow is disabled. The dedicated Titan service is disabled/stopped; editable workflow conditions are not an isolation boundary. Each push to `main` triggers one hosted macOS build followed by one hosted Ubuntu build/publish job. The workflow automatically chooses a stable version, tags the triggering commit after validation, and publishes the complete release. No release job depends on Titan. See [distribution.md](distribution.md). Both platforms pin Go 1.27.1 and GoReleaser 2.18.2. No AWS resources are involved.

## Planned local agent activity

The [agent activity contract](agent-contracts.md) specifies the next implementation slices: one local union timer per computer/account/project, durable actor history and uncertainty, immutable attribution, and shared CLI/UI actions. Its [operation catalog](agent-operations.json) and [acceptance vectors](agent-acceptance.json) are design contracts, not shipped features. They preserve the existing Harvest provider and direct time/timer commands. The planned independent sync worker and interactive lifetimes apply only when those features ship; the current executable behavior described above is unchanged.
