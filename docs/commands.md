# Command reference

All commands accept `--json`, `--account ID`, `--yes`, and `--non-interactive` before or after positional words. Commands never prompt; `--non-interactive` documents agent intent. `--help`/`-h` displays offline help. Flag values are literal strings, including notes that begin with `--`. Use `--notes=''` to clear notes. Unknown, duplicate or inapplicable options fail before credentials are accessed.

IDs must be canonical positive decimal integers without leading zeroes, within signed 64-bit range. Account precedence is command flag, `HARVEST_ACCOUNT_ID`, then saved config. Token precedence is `HARVEST_TOKEN`, then macOS Keychain. `TEMPO_CONFIG` overrides the config file path; the default is the OS user config directory under `tempo/config.json` (macOS: `~/Library/Application Support/tempo/config.json`).

## Authentication and configuration

| Command | Behavior |
|---|---|
| `auth login --token-stdin [--account ID]` | Read one token up to 16 KiB from stdin, validate accessible accounts, save securely. With no selected account, auto-select only a single Harvest account. |
| `auth status` | Inspect credential availability and source locally; does not verify server validity. May request OS Keychain access. |
| `auth status --check` | Verify token and selected account remotely. |
| `auth logout --yes` | Delete saved credential and account choice. Does not revoke token or clear environment. |
| `accounts list` | List accessible Harvest accounts, excluding Forecast. |
| `accounts use ID` | Validate access and save default account. Environment/flag overrides still take precedence. |
| `config show` | Show path, saved/effective account and nonsecret metadata, without reading credentials. |
| `config set-account ID` | Alias for account selection. |

Login input is deliberately stdin-only. See README for a local hidden-input helper or supply a secret-manager pipe. Never place the token in an argument. Linux/Windows/non-cgo builds use environment authentication; login persistence returns an unsupported-store error.

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
| 2 | usage, validation, input_required, invalid_transition, unsupported_contract |
| 3 | auth |
| 4 | forbidden |
| 5 | not_found, binding_unavailable |
| 6 | conflict, confirmation_required, attribution_conflict, binding_in_use, revision_conflict, request_conflict, event_conflict, event_gap, clock_conflict, state_busy |
| 7 | network, api, rate_limit, response |
| 8 | uncertain_write, local_write_unknown |

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

The [agent activity contract](agent-contracts.md) and [planned operation catalog](agent-operations.json) define `tempo link [PROJECT_ID]`, local `activity` status/recovery, setup, hooks, worker, sync and themes for the agent timing epic. They also define equal CLI/UI access, searchable arrow-key pickers and forced finite JSON output. `activity status` and `activity event --input-stdin` are now shipped as described below. Explicit linking and link inspection/mutation are also shipped. Recovery, setup pickers, hooks, worker, sync, themes and interactive activity views remain planned. The generated schema describes available commands. `timer …` retains its Harvest meaning; `activity …` describes local computer activity.

## Local agent activity

| Command | Behavior |
|---|---|
| `activity status` | Finite local snapshot; no credentials, configuration reads or network. `--json` and `--non-interactive` emit the versioned envelope. An absent store returns null computer ID, revision `"0"` and empty collections without creating files. |
| `activity event --input-stdin` | Accept exactly one normalized v1 lifecycle JSON event, at most 16KiB. Adapter/internal operation; no prompts or arbitrary attribution fields. Uses the persisted computer identity and a linked path or binding ID/revision for tracked work; an unlinked location returns `untracked` without starting a timer. |

Local activity is independent of Harvest `timer` commands. `TEMPO_STATE` selects an absolute private state-file path for isolated operation; default is the OS user config directory under `tempo/activity-state.json`. No public fake-binding/init command is provided. Explicit linking is available below; supported host bridges remain separate, and these commands do not install or automatically capture agent callbacks. Interactive watch and recovery commands are not yet shipped.

Every accepted event has stable source/actor/generation/sequence identity. Retry the exact same event after an uncertain local write; a conflicting payload returns `event_conflict`. `state_busy` is retryable; `local_write_unknown` exits 8 with uncertainty set. State and clock failures use safe errors, never raw payloads. Binding, ordering and attribution conflicts use exit 6; unsupported contract/input/transition uses exit 2. Local `--non-interactive` errors use one stderr JSON envelope and empty stdout.

Working actors contribute to one union per computer/account/project; separate projects run concurrently. Waits close only that actor's segment. Parent termination never stops a child. Reliable closed unions appear with queued counts, but this slice makes no Harvest requests and has no active synchronization worker. Uncertainties remain separate and cannot become queued time through an ordinary later stop or resume.

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

`--task` may be omitted only when the existing binding has a still-active saved task for the same account/project or exactly one active assigned task is available. `--timezone` may use a previously verified choice on that binding; there is no machine-timezone fallback. Missing project or unresolved choices return `input_required` with safe required field names. Interactive selection arrives with guided setup; current commands never prompt.

All local mutations accept `--request-id UUID`. The CLI generates one when omitted; automation should supply and retain it. After `local_write_unknown` (exit 8), use the same arguments and request ID from the error details: an exact replay returns the original result after checking local durability, even if the original path or saved credentials/account selection changed. Changed explicit intent with the same ID returns `request_conflict`. This is local replay, never permission to retry a Harvest write.

Relink, unlink and repair reject `binding_in_use` while working, waiting or stale actors remain attached. Identical linking remains a no-op. Compatible idle bindings for the same account/project must agree on user, task and timezone; disagreement returns `attribution_conflict`. Historical actors, segments, uncertainties and intervals keep their captured attribution after later permitted changes. Existing-generation stop/wait events use that history, regardless of their current directory.

`links list/show/unlink/repair` use only local state. Link performs read-only Harvest validation through current-user assignments with complete pagination, without administrator project/task catalogs. New `link`/`links` commands with `--non-interactive` always emit one finite JSON envelope; failures use stderr and leave stdout empty. No host hooks or background sync worker are installed by linking.
