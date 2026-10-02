# Installing native hooks

Tempo installs synchronous local hooks and an instruction-only skill for Codex 0.159.3 and Claude Code 2.1.286. Installation does not grant host trust, capture eligibility, account access or upload consent. Other runtime versions and remote surfaces require a separately verified adapter contract.

Use one explicit host (`codex`, `claude`, or `both`) and scope (`user` or `project`). Project scope requires an absolute `--path`. User installation still needs an explicit project context before capture policy can be confirmed. An installed declaration applies only to the exact canonical working directory reviewed; descendants and other worktrees need their own declaration.

1. Run `tempo hooks preview --host codex --scope project --path /absolute/project --operation install --json`.
2. Review every target, exact rendered command and host approval step. Run `tempo hooks install` with the same selector, returned `--fingerprint`, a fresh canonical `--request-id UUID`, and `--yes`.
3. Open the host normally in that project. Review its workspace trust and actual `/hooks` list, including plugin and dynamically registered handlers. Reload as the host requires. Required Tempo handlers must be synchronous and enabled; competing prompt veto/continuation handlers are incompatible with the clean profile.
4. Run `tempo hooks status` with the selector and `--json`. Review the current profile fingerprint and declaration version. Explicitly opt in with `tempo hooks confirm-profile`, that selector, `--fingerprint`, `--declaration-version`, a fresh `--request-id`, and `--yes`.
5. Use `tempo hooks verify` with the selector to inspect evidence. Verification does not fire a synthetic event. Installed files and a retained declaration do not prove a real host delivered anything.

Interactive `tempo setup` exposes the same preview, install, repair, uninstall, status, verify, confirm and revoke operations after linking. Installation confirmation and capture-policy confirmation are distinct choices. Declining either leaves that action unapplied. Worker readiness is displayed separately; installing hooks never installs or starts a user service and never enables uploads.

## Retained declaration and delivery

The profile basis is `operator_declared`, never machine-verified absence of dynamic hooks. It records an explicit clean-profile decision against the runtime, executable, project context, definitions, skill and complete inspected configuration fingerprints. Unexported plugin/session changes remain a disclosed risk of inaccurate timing. Review changed host behavior and revoke when the declaration no longer applies. Known static admission/continuation or async conflicts prevent confirmation.

New sessions can inherit eligibility only. A declaration does not create actor continuity, transfer source generations, resolve uncertainty or establish delivery. Status can report `not_installed`, `approval_required`, `awaiting_real_event`, `unsupported`, or `needs_repair`. It reports drift as an invalidated profile without writing it. Adapter admission persists known invalidation before allowing capture. Callback receipts have unverified origin; this version therefore keeps `last_real_event` null and does not infer `receiving` from manual callbacks or local fixture traffic. Native acceptance evidence comes from the separately bounded supported-host audit, not a status probe.

`tempo hooks revoke-profile` takes the exact single-host selector, current `--if-revision`, a fresh request ID and `--yes`. It blocks later adapter admission without deleting activity history.

## Ownership and recovery

Preview is read-only and creates no directory, lock, state, backup or tracking identity. It binds current file content, modes, runtime/executable and packaged skill bytes. Mutations recheck under the shared metadata lock and destination locks before changing files. Stale previews fail with `revision_conflict`.

Only uniquely matched Tempo-owned elements are replaced or removed. Other JSON spans, unknown fields, number spelling, permissions and unrelated files stay intact. Identical-looking unowned handlers or skills are not adopted. An edited or ambiguous owned resource conflicts and stays intact; uninstall never restores a whole historical configuration over later user edits. The bundled [skill](../internal/hookstate/skill/SKILL.md) registers no dynamic hooks and delegates clocks, project union and synchronization to Tempo.

Each changed existing target has an exact private backup before replacement. A durable journal records safe paths and before/after hashes, while raw configuration lives only in private backup/staging files. Replacement is atomic per file; several files cannot form one OS-level atomic rename. An interruption after a replacement returns `local_write_unknown` (exit 8) and retains the journal. Preserve the original full request and repeat only that same request ID. Replay reconciles exact before/after states, refuses intervening edits, and does not duplicate backups. A new request ID is not recovery. Completed replay returns the original result before runtime discovery.

`repair` and `uninstall` use a fresh preview with their exact operation and the same explicit confirmation protocol. Uninstall retains metadata, backups, local timing/history, account settings and worker state. JSON/noninteractive/redirected hook commands are finite and never prompt or access credentials.

## Codex startup settings and linked worktrees

Codex install previews an owned `tui.show_tooltips = false` change in the selected `.codex/config.toml`. This disables startup tooltips and prevents the pinned host's normal model-availability NUX counter write after normal project trust/reload. Tempo keeps full configuration hashing: another context, runtime override or actual configuration change can still invalidate the declaration. Project install never silently changes the user-level tooltip setting. A preexisting false value remains unowned; uninstall restores a previous true value or removes an unchanged owned insertion while preserving other settings. Ambiguous target TOML syntax is rejected with no write.

For a linked Git worktree, pinned Codex loads hook definitions from the matching relative directory in the main checkout. Preview therefore shows that effective shared hooks destination explicitly, while the selected worktree's skill and tooltip configuration remain local. Claude project hooks stay in the selected checkout. The capture declaration remains bound to the selected project context plus the effective definition path, Git directory identity and linkage files. Repointing or recreating the repository invalidates evidence; ordinary commits do not change the directory identity.

CLI and setup generate a canonical request ID when omitted. Mutation results include `request_id`; unknown errors include it in safe error details. Pending status exposes `pending` with the original request ID, fingerprint and intent, so an interrupted UI action can be resumed exactly. Do not obtain a fresh preview or use a new repair request to overwrite an unresolved transaction.

Installed declarations apply only to the exact canonical working directory reviewed by `confirm-profile`. A descendant or another worktree needs its own reviewed declaration. The lower-level policy API retains its existing scope contract; installer-produced contexts carry `inventory_version: tempo-installed-static-v1`.

Static inspection supports ordinary default host roots on the pinned local runtimes. Codex records user files, both configuration and hooks in each applicable Git project layer, corresponding main-checkout layers for linked worktrees, and system configuration, hooks, requirements, and managed configuration. Claude records user and selected project/local settings plus fixed managed settings. Every candidate includes an absence fingerprint. A present Claude `managed-settings.d`, custom project-root/profile selection, or nondefault host-root environment override is unsupported by this narrow inspector. Inventories are bounded to 128 artifacts and 32 ancestor steps; unsafe paths and oversized files fail closed. TOML structure is read with a maintained decoder, while edits retain original bytes. Normal trust hashes do not establish host trust.

Known foreign prompt admission or stop continuation handlers, required asynchronous callbacks, and managed-only policy prevent declaration. Missing, edited, or ambiguous Tempo callbacks or skill files require repair and cannot be overridden by confirmation. Unknown runtime, SDK, remote, MDM, plugin, and session overrides remain explicitly operator-declared residual risks. Ordinary static inspection does not claim to discover them.

An absent profile has revision `0`, empty artifact/conflict arrays, and a context with the selected host/scope; its path may be empty for a user-scope query without a project. It carries no fingerprint or declaration eligibility.
