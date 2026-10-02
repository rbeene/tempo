# Tempo contributor guide

Tempo is a public, personal Harvest CLI. Use Go and keep the runtime small.

- Work only in an issue-linked feature worktree; main is the clean base. Outside explicitly authorized delivery work, do not merge without Robert's approval. For the core agent-timing epic #5 (issues #6–#18), Robert has authorized the coordinator to merge after meaningful checks, independent QA, a fresh-instance refactor review and passing CI on the exact PR head. Implementers do not merge. Optional macOS issues #19/#20 are outside this authorization. A merge to main triggers automatic tagging and release publication.
- Read docs/architecture.md and docs/commands.md before changing contracts.
- No real Harvest credentials, Keychain access, or real account mutations in development/tests. Use injected stores and mock HTTP servers. Never log secrets, headers, API error bodies, or raw transport errors.
- Native macOS Keychain stores tokens; config contains only account ID. Environment tokens override the store. No plaintext token fallback or token flags.
- Follow Harvest API v2 primary documentation. Preserve current-user scope and pagination security. Never retry writes, even after transport failure.
- Define meaningful behavioral tests before each implementation slice; use targeted consistency/review checks for documentation-only contracts instead of file-existence tests. Run gofmt, go vet ./..., go test ./..., go test -race ./... and build after changes.
- Require independent QA and a fresh-instance refactor review before opening a ready PR; completed PRs must not remain drafts. Link the implementation issue with Closes #N; do not close epic #5 before all core work is complete. The coordinator maintains native issue blockers and readiness labels after merges. CI is one lightweight job, no matrix. Do not change runners, infrastructure, or AWS.
- Machine contract: versioned JSON envelopes, stable codes/exits, offline schema/help. Keep docs and schema synchronized.

- Public PR workflows must use GitHub-hosted runners. Never run fork code on Titan or use pull_request_target for checkout/build. Keep the dedicated Titan service parked until a separately approved server-side isolation boundary is implemented; see docs/distribution.md.
- GoReleaser changes require snapshot builds on a Mac to preserve native Keychain support. Do not manually create tags, releases or publish artifacts without explicit approval. Once merged, the approved main-branch workflow automatically tags and publishes releases.
