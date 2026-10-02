# Codex lifecycle capture

`tempo hook codex --input-stdin` accepts one native Codex callback and applies it synchronously to the existing local activity service. It never reads Harvest credentials, calls Harvest, prompts, or installs hooks. An unlinked directory creates no activity identity, hook policy, or directory. Hook installation, repair, policy confirmation/revocation and the packaged host skill belong to the installation workflow.

## Supported profile and evidence

The decoder targets Codex CLI **0.159.3**, using the [official hook contract](https://developers.openai.com/codex/hooks). Local command hooks require a supported local host, synchronous callbacks, native trust approval, and the configured Tempo executable. Cloud-orchestrated Work tasks do not gain local hook support merely because their commands execute locally. An installed CLI or hook definition is not evidence that callbacks are arriving.

Capture eligibility is an explicitly retained `operator_declared` policy, scoped to a canonical project directory and runtime version. The declaration acknowledges the effective hook configuration, including dynamic sources that cannot be completely discovered. In particular, competing `UserPromptSubmit` vetoes, blocking Stop continuations, asynchronous callbacks, or untrusted definitions prevent the normal profile. No arbitrary hook code is executed to establish eligibility. This declaration has provenance and residual risk; it is never relabeled `host_observed`.

The shared `internal/hookstate` service previews actual runtime, Tempo executable and hook definition hashes; confirmation checks the current revision and artifacts again. Its single private metadata file and typed request ledger are authoritative for later installation operations. Known drift or explicit revocation invalidates eligibility durably. Restoring old bytes or replaying an earlier confirmation does not reactivate it. The most specific applicable declaration wins even when revoked or invalidated. An independent nested Git repository stops inheritance.

A declaration persists across fresh source sessions; it grants eligibility, not session continuity, successful delivery, or resolution of uncertain time. No per-turn or per-session confirmation is needed. Repository links still inherit across sibling Git worktrees through the common directory, but a project declaration does not attest to another checkout's different hook configuration. Each such context needs retained setup eligibility.

`TEMPO_STATE` and `TEMPO_HOOK_STATE` override the ordinary activity and hook metadata paths. The defaults are `activity-state.json` and `hooks-state.json` in the OS configuration directory's `tempo` folder. They are separate because confirming capture policy must not create activity identity. Policy sampling precedes the activity lock; each receipt records the sampled policy revision and fingerprint. A completed revoke before sampling prevents admission. A callback overlapping revoke may complete under its already sampled revision; no atomicity across the two files is claimed.

## Identity and causal ordering

The bridge retains allowlisted identity metadata only:

| Callback | Identity used |
|---|---|
| All | `hook_event_name`, `session_id`, `cwd` |
| SessionStart | `source` (`startup`, `resume`, `clear`, `compact`) |
| Turn callbacks | `turn_id` |
| SubagentStart / SubagentStop | Native child `agent_id` and its own `turn_id` |
| PreToolUse / PostToolUse | `tool_use_id`, `tool_name` |
| PermissionRequest | `tool_name`; the documented payload lacks a reliable tool ID |
| Stop / SubagentStop | `stop_hook_active` |

Root and child turns share a native session ID but have distinct actor identities and may have different turn IDs. Tagged internal root/child namespaces prevent a child literally named `root` from colliding with the root actor. Native IDs are opaque; lexical order is never chronology. Prompts, transcripts, tool arguments/results, model output and referenced files are not retained or hashed. Unknown fields are ignored, while duplicate JSON keys, malformed UTF-8, invalid required fields and oversized input are rejected with fixed safe diagnostics.

An eligible SessionStart establishes an internal incarnation. A synchronous prompt or child start maps its native tuple to one exact actor generation before returning. Mapping, normalized lifecycle reduction and the capture receipt commit together under the activity lock. Replays reconcile that receipt before consulting clocks or repeating effects. Stop fences a turn and enters `wait_user`; it is not an unconditional final-finish claim. A terminal arriving before its start leaves a tombstone. Old terminals and delayed tool phases cannot close or resume a newer prompt.

Child start does not imply a parent wait. Exact built-in tool names `wait_agent` and `multi_agent_v1wait_agent` have source-verified, awaited pre/post phases with the same native call ID in Codex 0.159.3. A pending wait pauses only its invoking actor when no ordinary tool phase remains active. Overlapping waits remain paused until their last matching post; an ordinary tool active during that phase counts as work. No tool arguments, spawn relationship or child lineage are needed. Other names do not infer waiting. Native tool callbacks omit child `agent_id`, so they must identify one recorded native turn; ambiguous identity is reviewable. Missing permission correlation and Stop continuation ambiguity quarantine known working tails. A later tool completion never repairs that uncertainty automatically.

A terminal with a missing wait post preserves the proven working prefix and leaves a non-reconstructable `capture_reviews` warning (`incomplete_wait`). Source, permission, policy or shared clock loss while already waiting similarly fences automatic resumption (`source_loss_while_waiting`). These retained receipts are not end-resolvable timing uncertainties: Tempo cannot reconstruct an unknown restart time or safely bill the known wait. Resume, supersession and explicit interruption retain the same warning before detaching the old actor. A late post cannot heal that generation. A later distinct proven turn can still capture normally; manual correction guidance belongs to the review UI.

A known compaction preserves continuity. Resume/clear creates a new root incarnation and preserves the prior uncertain tail. A repeated boundary before new work is a replay. Because Codex supplies no unique resume-operation ID, a resume after new work is conservatively another boundary even if it could be delayed duplicate delivery. Resume does not prove child termination: historical child actors remain independently reviewable until their own identified terminal or recovery observation. SessionEnd refers only to the explicitly registered root, never all children. SessionEnd after an already interrupted/stopped root is stale evidence without a timing effect.

## Failures and delivery limits

Recognized hook invocations return exactly `{}` on stdout, including capture failures, with no control, permission or model-context fields. Diagnostics use bounded fixed categories on stderr and distinguish `committed`, `not_committed` and `unknown` durability. Capture failures exit zero to avoid vetoing the host; invalid command-line arguments remain usage errors. After an error-free committed applied or duplicate capture, the CLI sends a best-effort local worker wake within the same hook deadline. Missing or failed notification leaves capture and host output unchanged; worker polling remains the fallback. This hint reads no credentials, calls no provider or service manager, and never enables uploads. Input is limited to 64 KiB and the operation budget is 900 ms. An inherited open stdin pipe has a finite deadline. In-process callers must supply a finite buffer or a deadline-capable file; opaque readers are rejected without reading.

A committed source-loss observation can quarantine an exact recorded generation. A later reliable terminal can cap that range, but cannot resolve or bill it automatically. Local store uncertainty is reconciled by retrying the same semantic event or typed request. A failure before commit can leave no evidence: a future hook process cannot detect a lost decoder/lock/deadline/write attempt from silence. The normal synchronous profile therefore has a disclosed residual risk of undetectable callback loss, rather than a fabricated universal delivery sequence or heartbeat.

`activity.Service.HostReceipts` is a pure read interface for installation verification and native acceptance tooling. Receipts always retain `origin=unverified`: synthetic input can call the same decoder and cannot prove host delivery. Real delivery evidence must correlate an actual supported runtime with those receipts independently.

## Verification scope

Independent tests use temporary artifacts, genuine policy preview/confirmation, genuine mocked-provider linking, scripted clocks and the production activity/recovery transactions. They cover quiet turns, child independence, replay, reorder fences, permission and continuation ambiguity, policy loss, exact source observations and storage faults. They include exact-name matched and overlapping wait phases. They do not prove native runtime support.

The existing GitHub-hosted CI job also contains a pinned actual Codex smoke with a loopback synthetic provider, fixture-authored normal hook configuration and the real trust UI. It never copies credentials, rewrites HOME/CODEX_HOME, bypasses host trust, starts a local Mac Codex runtime or touches real Harvest. Its allowlisted artifact reports the exact tested head, runtime/executable integrity and event-specific observations. Source review or synthetic test success is not a passed native run; inspect that exact-head CI evidence before claiming delivery support.

The built-in wait mapping is verified against pinned Codex source and independent synthetic phase tests. The current native smoke does not exercise `wait_agent`; its successful run alone must not be described as native wait-phase coverage.
