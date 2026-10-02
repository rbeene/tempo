# Claude lifecycle capture

`tempo hook claude --input-stdin` accepts one native callback, with a 64 KiB input
limit and a 900 ms total budget. Valid invocations keep stdout empty and exit zero
on capture failure; fixed diagnostic categories and durability appear on stderr.
Invalid command arguments remain ordinary usage errors. `--json` and
`--non-interactive` preserve this host protocol. Capture never accesses credentials
or Harvest. An accepted local callback can send a bounded wake hint to an existing
worker; an absent worker does not change the durable result.

The declared runtime is Claude Code 2.1.286. An existing link and retained scoped
`operator_declared` hook policy are required. This command does not install hooks,
grant trust, or prove native delivery. Known conflicts, policy drift and revocation
block new capture and fence identified live work. The declaration may cover
undiscoverable dynamic handlers as an explicit operator assumption; it is not proof
that prompt-vetoing or continuing Stop handlers are absent. New sessions inherit
eligibility, never continuity. Receipts retain `origin: unverified`.

The decoder keeps only session/prompt/child/tool identifiers, absolute cwd, event
kind, session-start source and the Stop boolean. It rejects malformed, duplicate,
deeply nested or invalid UTF-8 JSON, and drops prompts, transcript paths, tool
arguments/results, task content and error details. Transcript paths are never
opened. Native `prompt_id` supplies the turn identity; `turn_id` is not a fallback.
Root and child namespaces cannot collide, and no parent relationship is guessed.

| Native callback | Capture behavior |
| --- | --- |
| SessionStart | Registers a session without starting work; startup/resume/clear/compact/fork are recognized. |
| UserPromptSubmit | Starts one root generation per admitted prompt; supplied child metadata is rejected. |
| SubagentStart | Starts independent child work. Repeating a stopped child/prompt has no reliable cycle identity and creates a separate review receipt. |
| Stop / SubagentStop | Ends the identified working segment into `wait_user`; it never ends children or proves a further Stop cycle. |
| PreToolUse / PostToolUse / PostToolUseFailure | Tracks exact native tool phases for the root or optional child. Failure remains a distinct receipt and cannot imply global interruption. |
| PermissionRequest | Lacks proven native tool correlation; reviews the identified actor without guessing from tool names or arguments. |
| StopFailure | Preserves the exact actor's unconfirmed tail and supplies its failure boundary. |
| SessionEnd | Detaches a completed waiting root without extra time; a working root has an uncertain tail, not confirmed work through exit. Independent child tails stay independent. |
| TaskCreated / TaskCompleted | Retains an identified observation without sampling the clock or changing membership. Missing prompt identity gives an unsupported diagnostic. |

Other events, including Elicitation, ElicitationResult, TeammateIdle and
Notification, produce safe unsupported diagnostics. Claude does not acquire the
Codex Interrupt or compaction event contract. A session-only exit after native
session ID reuse cannot select an incarnation and records an ordering review
without mutating the newer actor.

Only the exact Claude tool name `AskUserQuestion` establishes a question wait.
The actor waits only while every outstanding tool is such a question; another
active tool keeps it working. Matching successful or failed tool completion can
resume healthy work. Codex wait-tool names and Claude `Agent` do not imply this
wait. Child membership remains independent throughout.

Missing question completion, positively observed loss, clock failure or a
contradictory continuation retains a capture review and fences late callbacks.
Known waiting time does not become a fabricated continuous uncertainty interval.
Ordinary completed-turn `wait_user` is distinct from a pending question. Working
tails use the existing timed uncertainty/recovery model. Neither later Stop nor a
new prompt resolves retained uncertainty or erases capture reviews.

Exact committed replay reconciles durability before consulting the clock or live
policy, including after `local_write_unknown`. An input error, killed hook or
definite precommit failure can leave no durable observation; the next callback
cannot promise to discover it. Host exit zero is not a durable-capture receipt.

Automated tests use synthetic callbacks, isolated stores and injected clocks.
Actual-host acceptance is a separate exact-head hosted Linux gate; fixture tests
alone do not verify delivery. Local macOS credential access, interactive Claude
trust and arbitrary dynamic hook configurations are outside this evidence.

The hosted print-mode fixture uses production preview/install for the project
settings and bundled skill, then production status/confirmation for the exact
project context. It checks all 13 installed events separately from the measured
lifecycle subset. The direct installed command is unchanged; hook stderr evidence
is explicitly unavailable because the host manages that stream. Committed
receipts, exact actor effects, queue/review checks and unchanged complete profile
inventory after production status are required for acceptance.
