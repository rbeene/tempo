# Verification evidence

2026-10-01, Robert's Mac Studio (Apple Silicon). Personal owner verified with `gh api user`: `rbeene` / Robert Beene. Repository is private: `rbeene/tempo`. Integration checkout: `/Users/rbeene/src/beene/Tempo`; implementation is in a separate issue-linked feature worktree, branch `feature/1-harvest-cli`.

## Local checks

- Current official Go 1.27.1. Initial checks used a checksum-verified task-scoped toolchain; final checks use mise as requested. The project pins the same version in `mise.toml` and `go.mod`.
- `go vet ./...`: pass.
- `go test -race -coverpkg=./internal/... -coverprofile=coverage.out ./...`: pass; **79.2% statement coverage** across internal packages.
- Native stripped `go build -trimpath -ldflags='-s -w'`: pass. Mach-O arm64 executable, **6,412,882 bytes** (about 6.1 MiB).
- Compiled binary help/schema/version smoke checks: pass. Checked-in schema matches generated output.
- Auth package tests, non-cgo tests and Linux cross-build also passed earlier in the implementation cycle. Final deliverable targets native macOS arm64.
- Test HTTP servers required loopback permission; they made no real Harvest requests. All credentials are synthetic and stores injected. Tests never invoke the real Keychain.

## Independent verification and refactor

A separate QA instance authored 29 top-level behavioral tests (plus table-driven cases), including validation with no side effects, scope, timer transitions, explicit deletion, JSON/exit behavior, note parsing and wrong-entry safeguards. Separate auth tests cover token/account precedence, failure preservation and rollback. Transport tests cover cursor pagination, headers, large IDs, redaction, retry limits, cancellation, rejected continuation scope and unknown write outcomes.

A fresh review initially requested changes. Fixed findings:

1. Pinned all original filters through pagination to prevent scope changes or hidden running timers.
2. Rejected noncanonical numeric IDs before side effects to avoid JSON failure after a successful delete.
3. Validated timer state before restart and verified returned entry identity before mutation.
4. Fixed option-like notes being interpreted as help/output flags.

The refactor consolidated ID validation across CLI/config and timer-state validation across start/stop. The fresh reviewer approved the revised code for a draft PR. The subsequent Codex output review found recovery gaps: post-rename config failure, cancellation during token input, and logout with damaged config. These were fixed with fault-injection and regression tests; focused re-review passed with the deliberate live-auth gap. SIGINT and SIGTERM open-pipe executable tests both exit promptly with code 7. Post-refactor tests, race checks, vet and build passed. Test-first evidence includes missing-implementation failures, malformed configuration and pagination regressions, and independent literal-note failures before fixes.

## Startup benchmark

Compiled native Go 1.27.1 binary; ten warmups, then 100 new-process launches per command with output discarded. Measurements include process spawning and OS scheduling; they do not measure network or credential latency.

| Command | Median | p95 |
|---|---:|---:|
| version | 7.081 ms | 8.995 ms |
| help | 8.566 ms | 17.746 ms |
| schema | 7.900 ms | 18.739 ms |

## CI and remaining acceptance

CI is one lightweight Linux job on Titan with no matrix. It runs on pull requests and integration pushes, avoiding duplicate feature-push/PR jobs. At first there were no repository runners; Robert subsequently explicitly authorized creating one. The new repository-scoped runner is `titan-tempo-1`, service `tempo-runner.service`, directory `/home/rbeene/gh-runners/tempo-1`, labels `self-hosted, Linux, X64, tempo, titan`. It uses the official checksum-verified runner 2.337.0, a two-core CPU quota and 3 GiB memory cap. The five existing Titan runners remained active. No organization permissions or AWS resources were changed. Private Linux artifacts are retained for seven days; the local macOS binary is separate.

Final commit and CI result are reported in the draft PR and delivery response, rather than embedded into a file whose commit would change that same SHA.

Live Harvest login and Keychain access prompts remain a manual acceptance step for Robert. The native bridge compiles, but those live interactions are deliberately not claimed as tested. Harvest permissions and account configuration are checked at runtime. No real credentials were created, copied or read, and no real Harvest data was mutated. No merge, release, deployment, AWS work or public publication was performed.
