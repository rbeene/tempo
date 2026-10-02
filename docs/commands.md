# Command reference

All commands accept `--json`, `--account ID`, `--yes`, and `--non-interactive` before or after positional words. `setup` and `link` may prompt only when both stdin and stdout are terminals. `--json`, `--non-interactive`, or either redirected stream forces finite machine output with no implicit input. `--help`/`-h` displays offline help. Flag values are literal strings, including notes that begin with `--`. Use `--notes=''` to clear notes. Unknown, duplicate or inapplicable options fail before credentials are accessed.

IDs must be canonical positive decimal integers without leading zeroes, within signed 64-bit range. Account precedence is command flag, `HARVEST_ACCOUNT_ID`, then saved config. Token precedence is `HARVEST_TOKEN`, then macOS Keychain. `TEMPO_CONFIG` overrides the config file path; the default is the OS user config directory under `tempo/config.json` (macOS: `~/Library/Application Support/tempo/config.json`).

## Authentication and configuration

| Command | Behavior |
|---|---|
| `auth login --token-stdin [--account ID]` | Read one token up to 16 KiB from stdin, validate accessible accounts, save securely. With no selected account, auto-select only a single Harvest account. |
| `auth status` | Inspect credential availability and source locally; does not verify server validity. Uses bounded native storage access with OS prompts disabled. |
| `auth status --check` | Verify token and selected account remotely. |
| `auth logout --yes` | Delete saved credential and account choice. Does not revoke token or clear environment. |
| `accounts list` | List accessible Harvest accounts, excluding Forecast. |
| `accounts use ID` | Validate access and save default account. Environment/flag overrides still take precedence. |
| `config show` | Show path, saved/effective account and nonsecret metadata, without reading credentials. |
| `config set-account ID` | Alias for account selection. |

Explicit `auth login` input is deliberately stdin-only; interactive `setup` also offers hidden token entry. See README for a local hidden-input helper or supply a secret-manager pipe. Never place the token in an argument. Linux/Windows/non-cgo builds use environment authentication; login persistence returns an unsupported-store error.

## Discovery

| Command | Behavior |
|---|---|
| `projects list [--all]` | Default returns your project-assignment objects, including nested project, client and tasks. `--all` requests the account-wide project catalog (permission required). |
| `projects show ID` | Return the matching current-user project assignment. |
| `tasks list [--project ID | --all]` | Default returns `{project,task_assignment}` rows from your assignments. `--project` filters them. `--all` requests the account-wide task catalog (permission required). |
| `clients list [--all]` | Default deduplicates clients from your project assignments. `--all` requests the account-wide client catalog (permission required). |

Global catalog permission errors are distinct from authentication failures. Use assignment discovery for ordinary member accounts.

## Time entries

| Command | Flags and behavior |
|---|---|
| `time list` | `--from DATE --to DATE --project ID --task ID --client ID --running true/false`, all optional. Current user's entries only, inclusive dates, every page. No date filter means all dates. |
| `time show ID` | Fetch one entry and verify current-user ownership. |
| `time create` | Required: `--project ID --task ID --date DATE` and one of `--duration DURATION`, `--hours HOURS`, or a complete `--start HH:MM --end HH:MM` pair. Optional `--notes TEXT`. Creates completed time. |
| `time update ID` | At least one of `--project ID --task ID --date DATE --duration DURATION --hours HOURS --start HH:MM --end HH:MM --notes TEXT`. Timestamp edits require the complete pair. Omitted fields remain unchanged. |
| `time delete ID --yes` | Verify ownership, then delete exactly that entry. No bulk delete. |

`DATE` is `YYYY-MM-DD`, `today` or `yesterday`; relative dates use the computer's local calendar. Durations accept decimal hours, `H:MM`, or Go duration units such as `1h30m`/`30s`. Range is 0–24 hours, finite, with zero explicitly preserved. Both duration flags use this parser. Fractional seconds/decimal hours are sent as numeric hours; Harvest controls stored precision/rounding.

Start/end input is 24-hour `HH:MM`, normalized to Harvest's documented clock format. End must follow start on the same day; split overnight entries. Dates/times do not carry a timezone offset. They are interpreted by Harvest; inspect resulting hours around daylight-saving transitions. Company timer mode must match the supplied fields: duration accounts accept hours, timestamp accounts accept start/end. Notes-only updates do not need a company-mode lookup.

## Timers

| Command | Behavior |
|---|---|
| `timer status` | Return every current-user running entry across all dates. |
| `timer start --project ID --task ID [--date DATE] [--notes TEXT] [--start HH:MM]` | Create running time. Date defaults to local today. `--start` is for timestamp accounts; omitting it lets Harvest choose current time. |
| `timer start ID` | Restart an owned stopped entry. If that exact entry is already the single running timer, return it without writing. Cannot combine ID with new-entry flags. |
| `timer stop [ID]` | Stop the selected owned running entry. With no ID, stop the single running timer; multiple running timers require an ID. Already stopped/no running timer succeeds without another write. |

Starting a different timer while any timer runs returns conflict. Stop it explicitly; there is no automatic switch. Timer discovery failures or malformed/out-of-scope results prevent mutation. Preflight checks are not atomic with Harvest writes, so other clients can race them.

## Output and automation

`help`, `schema`, `version` are offline and do not read credential storage. `schema` always emits machine-readable JSON. Normal output is indented data JSON; `--json` adds the stable envelope and compact encoding. List shapes are arrays, including empty arrays.

Success, stdout:

```json
{"schema_version":1,"data":{"id":300,"is_running":false}}
```

Failure with `--json`, stderr (stdout empty):

```json
{"schema_version":1,"error":{"code":"uncertain_write","message":"write outcome is uncertain; inspect time list/show or timer status before retrying manually","retryable":false,"uncertain":true}}
```

| Exit | Codes / meaning |
|---|---|
| 0 | success |
| 1 | internal, config, keychain, state_corrupt, clock_unavailable |
| 2 | usage, validation, input_required, invalid_transition, recovery_bounds, unsupported_contract |
| 3 | auth |
| 4 | forbidden |
| 5 | not_found, binding_unavailable, actor_not_found, uncertainty_not_found |
| 6 | conflict, confirmation_required, attribution_conflict, binding_in_use, revision_conflict, request_conflict, event_conflict, event_gap, clock_conflict, state_busy |
| 7 | network, api, rate_limit, response |
| 8 | uncertain_write, local_write_unknown, credential_write_unknown |
| 130 / 143 | Ctrl-C or SIGINT / SIGTERM, unless an uncertain dispatched mutation requires exit 8 |

`retryable` describes a read/rejection that may succeed later; it does not authorize blindly repeating a mutation. Remote `uncertain_write` requires read-only reconciliation. For local `local_write_unknown`, inspect local state and retry only the exact same request/event identity, as described below. Upstream validation bodies are deliberately suppressed because they may echo sensitive input. Check the supplied IDs, project/task assignment, account mode and fields.

Examples for an agent (substitute actual discovered IDs):

```sh
tempo projects list --json --non-interactive
tempo tasks list --project 100 --json --non-interactive
tempo time create --project 100 --task 200 --date today --duration 45m --notes 'Planning' --json --non-interactive
tempo time update 300 --notes='' --json --non-interactive
tempo timer status --json --non-interactive
```

The complete generated interface is [cli-schema.json](cli-schema.json). `make schema` regenerates it, and CI rejects schema drift.

## Planned agent activity interface

The [agent activity contract](agent-contracts.md) and [planned operation catalog](agent-operations.json) define `tempo link [PROJECT_ID]`, local `activity` status/recovery, setup, hooks, worker, sync and themes for the agent timing epic. They also define equal CLI/UI access, searchable arrow-key pickers and forced finite JSON output. `activity status` and `activity event --input-stdin` are now shipped as described below. Explicit linking, link inspection/mutation and local recovery are also shipped. Guided setup, project/task pickers and finite doctor diagnostics are shipped. Automatic sync and the optional worker are shipped below. Native hook management is available below. The activity dashboard provides timers, timer details, links, sync status, setup readiness and reviewed link controls. Other dashboard mutation controls and themes remain planned. The generated schema describes available commands. `timer …` retains its Harvest meaning; `activity …` describes local computer activity.

## Local agent activity

| Command | Behavior |
|---|---|
| `tempo` / `ui` | Timer dashboard with explicit reviewed link controls when both streams are terminals; otherwise one finite JSON snapshot. `--json` and `--non-interactive` force a snapshot. |
| `activity status` | Finite local snapshot; no credentials, configuration reads or network. `--json` and `--non-interactive` emit the versioned envelope. An absent store returns null computer ID, revision `"0"` and empty collections without creating files. `--watch` uses the same dashboard only with both terminals and neither forcing flag. |
| `activity event --input-stdin` | Accept exactly one normalized v1 lifecycle JSON event, at most 16KiB. Adapter/internal operation; no prompts or arbitrary attribution fields. Uses the persisted computer identity and a linked path or binding ID/revision for tracked work; an unlinked location returns `untracked` without starting a timer. |

Local activity is independent of Harvest `timer` commands. `TEMPO_STATE` selects an absolute private state-file path for isolated operation; default is the OS user config directory under `tempo/activity-state.json`. No public fake-binding/init command is provided. Explicit linking is available below; supported host bridges remain separate, and these commands do not install or automatically capture agent callbacks. The dashboard uses Up/Down to select a project, `r` to refresh, and `q` or Escape to quit. It reads shared local status once per second with at most one bounded read outstanding. A failed read freezes the last value and marks it stale. Resize adapts the frame; very small terminals show a resize notice. Intentional dashboard/watch lifetimes have no command-wide timeout. Opening, refreshing and quitting do not authenticate, contact Harvest, install or control services, pause sync, or interrupt capture.

Enter opens the selected timer's captured observation and attribution history. `2` opens searchable local Links, `3` opens Sync and its queue, comma opens Setup readiness, and `?` opens Help. Each explicit view uses a bounded shared local read; periodic timer refresh continues beneath the modal without changing its captured target or search. Up/Down scroll full details, Enter closes a detail view, and Escape returns to the dashboard or cancels a pending view read. `q` is search text inside a picker and does not quit from a detail view. Paste never submits a form. Forms require at least 40 columns and 8 rows.

`l` opens Links actions: create a link through the shared guided account/project/task/timezone flow, repair a selected binding location, or unlink a selected mapping. Create and repair require an absolute project path. The UI captures the binding UUID, full scope and revision before confirmation; timer refresh cannot change that intent. Repair and unlink require reviewing the full warning before selecting Yes and pressing Enter. Historical attribution remains retained. A revision conflict requires a fresh read, new confirmation and new request ID.

An uncertain link write retains its exact inputs and request ID. The recovery menu offers explicit same-intent replay, read-only status, or Back; reopening `l` returns to that recovery without creating a new intent. Status does not establish whether the write applied. Quitting or losing terminal input preserves `local_write_unknown` and its safe request ID after all owned work joins and the terminal restores.

Every accepted event has stable source/actor/generation/sequence identity. Retry the exact same event after an uncertain local write; a conflicting payload returns `event_conflict`. `state_busy` is retryable; `local_write_unknown` exits 8 with uncertainty set. State and clock failures use safe errors, never raw payloads. Binding, ordering and attribution conflicts use exit 6; unsupported contract/input/transition uses exit 2. Local `--non-interactive` errors use one stderr JSON envelope and empty stdout.

Working actors contribute to one union per computer/account/project; separate projects run concurrently. Waits close only that actor's segment. Parent termination never stops a child. Reliable closed unions appear with queued counts, and capture itself makes no Harvest requests; explicit sync or the optional worker submits eligible completed intervals. Uncertainties remain separate and cannot become queued time through an ordinary later stop or resume.

`ActivitySnapshot.project_timers` is the authoritative display projection grouped by computer/account/project. It reports provisional union and confirmed closed durations separately, with grouped actor references, unresolved IDs and queue counts. Existing `projects` retains full task/user/timezone attribution details. Both projections use the same status sample and resolved endpoints; the UI never sums actor durations or advances time itself. Live attribution compatibility and relink guards prevent conflicting overlapping attribution, and finalization rejects overlapping immutable closed intervals for a project timer. Sequential attribution epochs remain one project timer while retaining distinct accounting history. An absent store returns `project_timers: []` without creating state.

## Project and directory links

| Command | Behavior |
|---|---|
| `link PROJECT_ID --task ID --timezone IANA [--path PATH]` | Validate accessible account, authenticated current user and active assigned project/task, then persist a nonsecret binding. Path defaults to cwd. Account precedence: `--account`, environment, saved configuration. |
| `links list` | List live local bindings, their scope, attribution, revision and attached actors, without credentials or network. |
| `links show [BINDING_ID] [--path PATH]` | Inspect one binding by UUID or path, never both; defaults to cwd. Missing mapping returns `not_found`; a missing stored locator returns `binding_unavailable`. |
| `link PROJECT_ID … --if-revision REV` | Update a mapping using its current revision. Without a revision, a different mapping conflicts; an identical mapping is a no-op. |
| `links unlink BINDING_ID --if-revision REV --yes` | Remove the live mapping and retain a tombstone and all historical attribution. |
| `links repair BINDING_ID --path PATH --if-revision REV --yes` | Explicitly replace a moved locator while preserving binding identity/attribution. The new location must have the same scope kind and no conflicting binding. |

Inside Git, `link` applies to the **entire local repository and all its worktrees**. Results show `kind: "repository"` and its canonical absolute Git common-directory locator. A subdirectory or symlink resolves to that same repository; remote URL, project name and checkout basename are never identities. Separate clones, independent nested repositories and submodules require their own explicit links. In an ordinary directory, `kind: "directory"` applies to descendants: the nearest linked ancestor wins, and any Git repository boundary stops inheritance. Git discovery failures never fall back to ordinary-directory scope. Git must be installed for discovery; inherited Git path/configuration overrides are ignored.

`--task` may be omitted only when the existing binding has a still-active saved task for the same account/project or exactly one active assigned task is available. `--timezone` may use a previously verified choice on that binding; there is no machine-timezone fallback. Missing project or unresolved choices return `input_required` with safe required field names. Eligible interactive sessions resolve missing project/task/account/timezone choices and show the full target before confirmation. Explicit values skip their corresponding prompts.

All local mutations accept `--request-id UUID`. The CLI generates one when omitted; automation should supply and retain it. After `local_write_unknown` (exit 8), use the same arguments and request ID from the error details: an exact replay returns the original result after checking local durability, even if the original path or saved credentials/account selection changed. Changed explicit intent with the same ID returns `request_conflict`. This is local replay, never permission to retry a Harvest write.

Relink, unlink and repair reject `binding_in_use` while working, waiting or stale actors remain attached. Identical linking remains a no-op. Compatible idle bindings for the same account/project must agree on user, task and timezone; disagreement returns `attribution_conflict`. Historical actors, segments, uncertainties and intervals keep their captured attribution after later permitted changes. Existing-generation stop/wait events use that history, regardless of their current directory.

`links list/show/unlink/repair` use only local state. Link performs read-only Harvest validation through current-user assignments with complete pagination, without administrator project/task catalogs. New `link`/`links` commands with `--non-interactive` always emit one finite JSON envelope; failures use stderr and leave stdout empty. No host hooks or background sync worker are installed by linking.


## Inspect and recover local activity

| Command | Behavior |
|---|---|
| `activity review [--account ID] [--project ID]` | List unresolved uncertainty, including actor/session/generation references, using only local state. |
| `activity preview UNCERTAINTY_ID --end UTC` | Preview an explicit continuous-work assertion through the chosen UTC end. |
| `activity preview UNCERTAINTY_ID --discard-tail` | Preview keeping only the original confirmed prefix. |
| `activity resolve UNCERTAINTY_ID --end UTC --if-revision REV --yes [--reason TEXT] [--request-id UUID]` | Commit the asserted end after rechecking the uncertainty revision and bounds. |
| `activity resolve UNCERTAINTY_ID --discard-tail --if-revision REV --yes [--reason TEXT] [--request-id UUID]` | Discard unverified time, retaining the confirmed prefix and audit history. Works without clock capability. |
| `activity interrupt ACTOR_ID --generation G --if-revision REV --yes [--request-id UUID]` | Detach exactly that generation. Waiting adds no time; working preserves uncertainty for subsequent recovery. Children and peers remain independent. |

Recovery `--end` accepts a UTC RFC3339 timestamp ending in `Z`, for example `2026-10-02T10:00:00Z`, with up to nanosecond precision. This is separate from the `HH:MM` format used by Harvest `time` commands. Choose exactly one of `--end` or `--discard-tail`. A rationale may contain up to 512 UTF-8 bytes without control characters. These commands never access credentials or Harvest; `--account` is only a local filter on review and is rejected on preview, resolve and interrupt.

Inspect the preview before committing. It returns the original confirmed prefix, proposed end, excluded suffix when bounded, confirmed/resolved union ranges before and after, and remaining uncertainty IDs. Open peer work is provisional and does not appear as confirmed time. Use the preview's uncertainty revision in resolve; concurrent evidence may require a fresh preview. Reads never save health or uncertainty. Resolve keeps original samples alongside its audited assertion and finalizes only safe disconnected union components. If A starts at 09:00, B at 11:00, and A is recovered through 10:00, the 10:00–11:00 gap stays unbilled while B continues.

An end cannot precede confirmed evidence, follow subsequent resume/terminal bounds or trusted current time, or touch/overlap finalized output. Ends outside the confirmed/current/later-event bounds return `recovery_bounds`; conflicting clock evidence or finalized output returns `clock_conflict`. A stale entity revision returns `revision_conflict`. Original unknown time remains unresolved after normal finish, resume or a newer generation. Resolving an older segment does not stop newer work. Resolving the current stale segment detaches its generation; delayed callbacks cannot reopen it.

Supply and retain `--request-id` for reliable automated replay. Repeating the same intent returns its original result even after restart, without resampling the clock. Changed intent with the same ID returns `request_conflict`. An interruption that discovers an unavailable clock may durably quarantine the tail and return `clock_unavailable` without detaching; inspect status and use a new reviewed request for the next action. An uncertain local commit returns `local_write_unknown` with the request ID: retry that exact request first. No automatic Harvest correction or upload occurs.

## Guided setup and diagnostics

`tempo setup [--path PATH] [--host codex|claude|both] [--scope user|project]` verifies authentication, offers hidden token entry and account selection when needed, and guides project linking. The host/scope choices select the shared hook lifecycle menu, including preview and separately confirmed installation. Every authentication save and binding change has its own confirmation. Setup reports completed steps if a later step fails or is cancelled; earlier confirmed changes remain applied. Authentication alone never reports hooks or automatic upload as ready.

`tempo link [PROJECT_ID]` offers searchable active assigned projects, Up/Down selection and Enter, then resolves the task, account and IANA timezone. Type to filter; `q` is ordinary search text. Escape or EOF cancels; Ctrl-C cancels even during an API request. Saved parent-directory choices may supply defaults for a new child mapping; only an exact target mapping supplies an update revision. Repository mappings cover all Git worktrees.

Finite `setup` inspects local account configuration and the target binding without reading credentials or contacting Harvest. `doctor` reports local configuration and binding-state problems plus unverified capabilities; `doctor --check` additionally checks credentials and account access. Finite setup and doctor do not install hooks or enable uploads. Interactive setup offers separately confirmed shared hook operations.

Native credential operations disable OS prompts and run with a five-second helper budget. Login, logout and account selection share a per-user lock even across different config paths. If a dispatched credential/config write has no conclusive reply, `credential_write_unknown` exits 8 with safe `details.effects`; inspect `auth status` and `config show` before an explicit replacement. Do not automatically replay the operation. A killed helper cannot guarantee an already accepted OS operation will not complete later. Unsupported secure storage is reported before interactive secret collection; use a securely supplied `HARVEST_TOKEN` instead.

## Safe activity synchronization

`sync status` is offline and shows the saved account/current-user consent, outbox
plans, exact local capture, planned upload amounts, confirmed returned amounts,
and signed residuals. Missing confirmed time is `null`, not zero. Capture can be
ready while uploads still need a tracking-mode and representation choice.

First link a directory/project, then explicitly declare the account's setting
(check Harvest Settings if `/company` is unavailable to a non-administrator):

```sh
tempo sync configure --account 123 --mode duration --duration-policy nearest-hundredth-hour --if-revision 0 --yes --json
tempo sync resume --json
tempo sync now --limit 20 --json
tempo sync status --json
```

Revision `0` creates consent for the freshly verified account/current-user pair;
editing uses that pair's current revision from status. Configuration never changes
the default account, credentials, captured attribution, or the enabled flag.
Only a specifically classified Company `403` permits the declaration fallback;
invalid, inactive, conflicting or unavailable identity/mode evidence blocks writes.
Ordinary sync commands always use saved account identity and reject `--account`.

The nearest-hundredth-hour policy is an explicit Tempo billing representation:
each local calendar-day part rounds independently to 36-second units, with an
18-second tie rounded up. A captured `137.482` seconds plans `0.04` hours (`144`
seconds), leaving `-6.518` seconds of planned residual. This does not alter the
immutable capture. A positive part below 18 seconds blocks the entire root before
any POST. Residuals can accumulate across parts. `exact` instead encodes decimal
hours with an integer-nanosecond round trip; it does not promise Harvest arbitrary
precision. Returned hours must equal the saved planned decimal as an exact
rational. `rounded_hours` is informational and never proves a match.

Timestamp accounts require `--mode timestamp --duration-policy exact --clock 24h`
(or `12h`). Endpoints must be whole minutes on the same local date, agree with the
verified user's timezone, and avoid ambiguous clock folds, transitions and a
midnight ending. Tempo never rounds timestamp endpoints. Duration day boundaries
use IANA timezone transitions, including short/long and skipped calendar days.
A root exceeding 100 daily parts remains for review with zero POST.

`sync pause` stops future claims while capture continues. A pass has a two-minute
bound and selects at most 100 roots (default 20). It holds a separate process lock;
status and pause remain available. The optional background worker uses this same
service; the terminal Sync UI remains separate.

Each attempt records its originating run request ID; `attempted_ids` counts only
intents committed by that run, including interrupted claims, never historical
rejections from an earlier run. Every mutation accepts `--request-id UUID` (generated if absent). Preserve the ID
when retrying an uncertain local save: exact completed replay returns its saved
result without credential access. A pending request retains its original targets;
restart never turns it into a fresh batch. Recovery under the run lock runs before
paused/empty checks. An interrupted submitting claim becomes unknown. A saved
successful part can finalize locally. Partial interrupted roots never resume
unposted parts automatically.

Use `sync reconcile [OUTBOX_ID] --json` to search the complete saved correlation
scope. It never creates or edits Harvest entries. Zero, partial, mismatched or
multiple matches remain blocked; absence cannot authorize another write. Review
an existing stopped entry and use `sync resolve OUTBOX_ID --entry ENTRY_ID
--if-revision REV --yes --json` for explicit attachment. Notes-only manual entries
require a full current-user collision scan. Complete only a known never-attempted
remainder manually; never create a replacement for an unknown attempt based merely
on a failed search.

A conclusive rejection can be explicitly authorized with `sync resolve OUTBOX_ID
--retry-rejected --if-revision REV --yes --json`, followed by a fresh `sync now`.
Authorization itself performs no POST. It preserves the frozen plan, successful
parts and prior attempt audit; unknown effects and mismatched acknowledged entries
cannot use this route. Tempo never automatically retries POST, including `429`.
Unexpected success statuses, redirects, `408`, server errors, malformed responses
and transport failures are ambiguous. A confirmed entry with changed duration
retains its ID and stays visible for review without rebilling.

Provider behavior and mappings follow the official [time-entry API](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/),
[company API](https://help.getharvest.com/api-v2/company-api/company/company/), and
[supported timezone table](https://help.getharvest.com/api-v2/introduction/overview/supported-timezones/).

## Optional automatic sync worker

`tempo worker run` owns a foreground lifetime until stopped or signalled, including
with `--json` or `--non-interactive`. It does not install a service or enable sync.
SIGINT exits 130 and SIGTERM exits 143 after cleanup. One worker owns each resolved
`TEMPO_STATE`; a duplicate returns `state_busy`. Manual `sync now` shares the sync
engine's separate lock. Credentials use the existing bounded, non-prompting reader.

```sh
tempo worker install --yes --json
tempo worker status --json
tempo worker start --json
tempo worker stop --json
tempo worker uninstall --yes --json
```

Install creates an owned user LaunchAgent on macOS or a systemd user unit on Linux
(systemd 247 or later), without starting it. macOS installation establishes and
verifies persistent disablement before publishing the plist. Start explicitly
enables login startup and launches the worker. Stop disables startup and waits for
worker ownership to end. Uninstall removes only the unchanged owned definition;
activity, pending sync identity and control receipts remain. No root service,
`sudo`, automatic linger or credential provisioning is used. Service directories
are `~/Library/LaunchAgents` or the user's config directory `systemd/user`.

Finite controls accept `--request-id UUID`, generated when omitted. Retain that ID
and repeat the same input after `local_write_unknown` (exit 8, uncertain): a failed
manager mutation may already have applied, just as a failed save may be visible.
The original pending intent is retained for recovery. Read-only manager probe
failures remain ordinary `manager` errors. Completed replay causes no new manager
effects. Installation and removal require `--yes`. Foreign or edited
service definitions are preserved with `revision_conflict`. Binaries and selected
state/config paths are pinned in the definition; moving them requires deliberate
service repair. Lifecycle commands never grant account or rounding consent.

Each pass selects at most 20 eligible queued roots and lasts at most two minutes.
Progress waits at least one second between passes; idle/paused polling is every
30 seconds. Transient failures back off from five seconds to five minutes, and
auth failures wait five minutes. Local wakeups are bounded hints with polling
fallback. Successful capture emits an ordinary wake; successful resume, explicit
sync configuration and credential/account changes request a rate-limited recheck.
Ordinary wakes never bypass failure backoff, and failed notification delivery never
changes a successful durable command result. Unknown writes and rejected entries remain for explicit review; the
worker does not reconcile or retry them automatically. `sync pause` stops future
claims while capture continues independently.

Status is read-only and uses the activity snapshot's counts. `installed` and
`instance_mode` are nullable; a live instance lock, rather than manager success or
a saved PID, establishes running state. Edited definitions and pending controls
remain visible. `last_success` is a locally persisted confirmation time and can
lag after a crash; receipt replay never invents a new delivery time. Runtime and
control journals have separate writers and locks. Control history is bounded to
4096 receipts and 16 MiB; exhaustion preserves existing replay and refuses new
controls rather than silently pruning them.

Native launchd/systemd behavior requires isolated manual acceptance. Automated
coverage uses synthetic service directories, fake managers, parsed definitions,
owned child processes and local mock HTTP; it does not install personal services.

## Native Codex callbacks

`hook codex --input-stdin` consumes one bounded native callback and returns exactly `{}` for the host. It runs locally without credentials or prompts; capture diagnostics and durability appear only on stderr. `--json` and `--non-interactive` preserve that host protocol. Account overrides and confirmation flags are inapplicable. Capture requires an existing link and retained eligible hook policy. This command does not install or trust hooks. See [Codex lifecycle capture](codex-hooks.md) for the supported runtime, identity rules and delivery limits.

## Native Claude callbacks

`hook claude --input-stdin` consumes one bounded Claude callback with empty stdout.
Capture failures exit zero and report a fixed diagnostic and durability on stderr;
invalid arguments remain usage errors. The same 64 KiB input limit, 900 ms budget,
offline capture and retained eligibility requirements apply. `--json` and
`--non-interactive` preserve empty host stdout. See [Claude lifecycle
capture](claude-hooks.md) for prompt/child identity, question waits, failure and
delivery limits. Neither hook command installs configuration or grants trust.

### Native hook lifecycle

`tempo hooks preview|install|status|verify|repair|uninstall|confirm-profile|revoke-profile` are finite local operations available through the shared setup controls. Supply `--host codex|claude|both`, `--scope user|project` and an absolute project `--path` where required. Preview also requires `--operation install|repair|uninstall`. Mutations require the reviewed fingerprint (or current revision for revoke) and `--yes`; `--request-id` is generated when omitted and exposed for replay. Profile confirmation takes one host, an explicit project context, and the current declaration version. See [hook installation](hook-installation.md) for the full trust, retained policy, ownership and uncertain-write recovery sequence. `hooks verify` is read-only and cannot prove delivery by invoking a test callback.
