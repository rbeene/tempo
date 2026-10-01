# Tempo contributor guide

Tempo is a private, personal Harvest CLI. Use Go and keep the runtime small.

- Work only in an issue-linked feature worktree; integration is the clean base. Do not merge or release without Robert's approval.
- Read docs/architecture.md and docs/commands.md before changing contracts.
- No real Harvest credentials, Keychain access, or real account mutations in development/tests. Use injected stores and mock HTTP servers. Never log secrets, headers, API error bodies, or raw transport errors.
- Native macOS Keychain stores tokens; config contains only account ID. Environment tokens override the store. No plaintext token fallback or token flags.
- Follow Harvest API v2 primary documentation. Preserve current-user scope and pagination security. Never retry writes, even after transport failure.
- Define meaningful behavioral tests before each implementation slice; run gofmt, go vet ./..., go test ./..., go test -race ./... and build after changes.
- Require independent QA and a fresh-instance refactor review before a draft PR. CI is one lightweight job, no matrix. Do not change runners, infrastructure, or AWS.
- Machine contract: versioned JSON envelopes, stable codes/exits, offline schema/help. Keep docs and schema synchronized.
