# Install, update and release Tempo

Tempo's source is public at https://github.com/rbeene/tempo. No first release or tag has been published yet. Until a release is explicitly approved and published, build from source; the release installer will correctly fail to find an unpublished version.

## Install a published version

Download `scripts/install.sh` from a reviewed repository revision, inspect the script, then execute the saved file with a pinned version. Do not paste a literal Harvest token into commands. No `curl | sh` pipeline is required.

```sh
sh install.sh --version v0.1.0
```

This example becomes usable only after v0.1.0 is published. The installer detects macOS/Linux and arm64/amd64, downloads the exact versioned archive and platform checksum file from GitHub Releases over HTTPS, verifies SHA256 before extracting, and atomically replaces `$HOME/.local/bin/tempo`. It refuses root execution and never uses sudo or runs the downloaded binary. `--bin-dir` selects another writable directory. Put that directory on PATH, then inspect `tempo version` and `tempo help` yourself.

To update, run the same saved/reviewed installer with the explicitly chosen new version. A download/checksum/archive-validation failure leaves the previous executable in place. Checksums detect corruption and mismatched files; they do not provide an independent authenticity channel if the GitHub release account itself is compromised.

Archives are `tempo_VERSION_OS_ARCH.tar.gz`, with OS `darwin` or `linux` and architecture `arm64` or `amd64`. Checksum files are `checksums-darwin.txt` and `checksums-linux.txt`. Each archive contains one `tempo` executable plus README and command documentation.

## Authentication

macOS release binaries use cgo and Apple's native Security.framework Keychain bridge. Linux binaries are static Go builds with environment-token authentication. No token is bundled. Use `auth login --token-stdin` on macOS or securely provide `HARVEST_TOKEN` and `HARVEST_ACCOUNT_ID`; see README and docs/commands.md. Login and Keychain permission prompts remain user-controlled.

The macOS archives are not Developer ID signed or notarized. No Apple signing credentials have been provisioned. Builds preserve the standard Go/Apple linker signing behavior; do not describe these archives as notarized.

## Local snapshot verification

Go 1.27.1 and GoReleaser 2.18.2 are pinned in mise.toml. GoReleaser's standard open-source edition is sufficient; no Pro split/merge or prebuilt-import feature is used.

On a Mac with the developer tools installed:

```sh
mise install
mise exec -- goreleaser check --config .goreleaser.macos.yaml
mise exec -- goreleaser release --config .goreleaser.macos.yaml --snapshot --clean --skip=publish --parallelism=1
mkdir -p release-macos
cp dist/macos/tempo_*_darwin_*.tar.gz dist/macos/checksums-darwin.txt release-macos/
mise exec -- goreleaser check
mise exec -- goreleaser release --snapshot --clean --skip=publish --parallelism=1
sh scripts/test-install.sh
```

The first config builds Intel and Apple Silicon binaries natively against Apple's SDK. The second builds Linux amd64/arm64 with `CGO_ENABLED=0`. `dist/` and the copied Mac staging directory are ignored by Git. Snapshot suffixes intentionally differ from installable stable release versions; no tag, GitHub Release or upload is created by these commands.

## Release workflow and approvals

A future release requires a separately approved merge, version tag, workflow dispatch, and publication. This change performs none of them. After the reviewed configuration reaches integration and the owner approves an existing canonical `vX.Y.Z` tag:

1. Dispatch **Prepare versioned release** from `integration`, as the repository owner, with that tag. The workflow verifies the tag is an ancestor of integration and uses exactly that commit.
2. One GitHub-hosted macOS job tests and creates native archives without publishing. One subsequent Titan job builds Linux and attaches both platforms to a **draft** GitHub release. Each build uses parallelism one and Go parallelism two. There is no matrix and only one Titan job.
3. Inspect the draft's four archives, both checksums, version output and notes. Publish only after separate approval.

The persistent Tempo runner is **disabled and stopped** while the repository is public. Before the Titan job is permitted, a maintainer must establish an approved server-side execution boundary (for example, an isolated disposable runner without access to the host’s credentials/services, or a private release-control repository). An editable workflow condition or a reviewed tag alone is insufficient. The existing persistent service must remain disabled until that boundary is approved and in place. No runner service changes are made from a workflow supplied by a PR.

## Public repository trust boundary

The checked-in PR verification workflow runs on ephemeral GitHub-hosted Ubuntu runners with a read-only token and credential persistence disabled. The previous private-repository self-hosted CI workflow has been disabled in GitHub and removed from this branch. Every outside contributor's workflow requires maintainer approval. Do not approve a fork workflow that requests any self-hosted runner or changes the release path; inspect the proposed workflow as code.

The release workflow has no pull-request event and no `pull_request_target` use. Owner/default-branch gates and tag ancestry checks reduce accidental dispatch; they are not a sandbox against malicious workflow edits. Personal repository runners cannot be restricted to selected workflow files with organization runner-group policy. Therefore keeping Titan parked is the effective protection; the Titan packaging job is intentionally not runnable yet. Public self-hosted release execution requires a separate isolated-runner design and approval; this change does not expand organization permissions or expose other Titan services.
