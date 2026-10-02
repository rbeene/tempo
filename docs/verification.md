# Verification evidence

Initial implementation verification: 2026-10-01, Apple Silicon macOS. The initial private repository is now public, with a separate public-distribution change described in [distribution.md](distribution.md). Historical measurements below describe the original CLI build.

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

Initial private CI passed on one dedicated Titan Linux runner, with a two-core CPU quota and 3 GiB memory cap. During public conversion, that workflow was disabled and its runner parked. Public PR verification now uses a hosted Ubuntu runner. New release packaging and installer evidence is recorded below; native Keychain integration remains a manual acceptance gap.

Final commit and CI result are reported in the draft PR and delivery response, rather than embedded into a file whose commit would change that same SHA.

Live Harvest login and Keychain access prompts remain a manual acceptance step for Robert. The native bridge compiles, but those live interactions are deliberately not claimed as tested. Harvest permissions and account configuration are checked at runtime. No real credentials were created, copied or read, and no real Harvest data was mutated. The initial implementation was subsequently merged by the owner. This distribution change creates no tag, release, deployment or AWS resources.

## Public distribution verification

GoReleaser 2.18.2 `check` validates both configurations. Snapshot release builds with `--skip=publish --parallelism=1` produced macOS amd64/arm64 with cgo and Linux amd64/arm64 without cgo, four versioned tar archives and platform SHA256 lists. The Mac arm64 snapshot runs offline and links Security/CoreFoundation; other targets have the expected executable architecture. Installer regression tests use temporary fixtures, injected download tools and no network; they cover platform detection, pinned versions, corruption/missing/duplicate checksums, archive links/traversal/unknown entries, literal whitespace paths, atomic preservation and no execution of downloaded binaries.

Final distribution checks also passed formatting, vet, ordinary and race tests, schema comparison, seven installer test groups, and actionlint 1.7.12. The Linux snapshot was additionally built without Mac staging files, matching hosted CI. Independent installer QA, fresh distribution review and the Codex output gate passed; actual release upload and live authentication remain untested.

The public-content audit reviewed tracked history and metadata, issues/PRs/comments and existing artifacts. No secret-pattern findings or private client records were found; test tokens are synthetic. Repository visibility is verified public. External workflow approval policy is `all_external_contributors`. The old self-hosted CI workflow is disabled, and the Tempo runner service is disabled/stopped. Following the requested main-branch rename, release builds and automatic publication use hosted Mac and Ubuntu runners; Titan is no longer a release dependency. Its persistent service remains parked pending approved isolation. Nine independent version-selection tests pass, including retry reuse, canonical versions and unchanged Git state. The previous revision passed hosted CI on commit `081112a`; the final revision requires its own exact-head check.

Automatic-release follow-up: a synthetic temporary repository built the normal stable `v0.1.0` archives for all four targets with publishing disabled. No actual Tempo tag was created. Release asset tests reject missing, duplicate, unexpected, wrong-version and empty assets before publication. GoReleaser is explicitly pinned to the selected tag and configured to resume partial draft uploads. Workflow lint passes with one narrowly excluded outdated-schema diagnostic: actionlint 1.7.12 does not yet recognize GitHub's documented `concurrency.queue: max` field ([GitHub changelog](https://github.blog/changelog/2026-05-07-github-actions-concurrency-groups-now-allow-larger-queues/)).
