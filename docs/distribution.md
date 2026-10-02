# Install, update and release Tempo

Tempo's source is public at https://github.com/rbeene/tempo. No first release or tag has been published yet. Merging the distribution workflow to `main` enables automatic releases. Until the first workflow succeeds, build from source; the release installer will correctly fail to find an unpublished version.

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

## Automatic releases from main

Every push to `main`, including a merged PR, triggers **Release**. No manual dispatch, separate tag push, or publication approval is required by the workflow. This setup change is still in a draft PR; it has not merged or published anything.

1. One GitHub-hosted macOS job selects the version, tests, and builds native Intel/Apple Silicon archives. Version tags exist only in that temporary checkout at this stage.
2. A subsequent GitHub-hosted Ubuntu job verifies the Mac checksums, tests Linux and the installer/version selector, and builds Linux amd64/arm64 archives. Both builds use parallelism one and Go parallelism two; there is no matrix.
3. After validation succeeds, the workflow creates the version tag at the exact triggering commit. GoReleaser uploads all four archives and both checksum files into a draft, then the workflow checks that exactly the four expected versioned archives and both checksum files were uploaded and automatically publishes the complete release. GitHub determines the latest release by creation date and semantic version, so an older retry does not explicitly force itself to latest.

The first automatic version is `v0.1.0`. Each untagged commit receives the next patch version after the highest canonical stable `vX.Y.Z` tag. Prerelease and malformed tags are ignored. A commit with one existing stable tag reuses it; multiple stable tags on one commit fail as ambiguous. Version selection never modifies Git state. Major/minor changes require a separately deliberate versioning change or stable tag; they are not inferred from commit messages.

Release runs are serialized with GitHub's expanded concurrency queue (up to 100 pending runs), and do not cancel active builds or replace a single pending merge. A failed upload can leave a version tag and unpublished draft. Rerun that commit's failed workflow to complete it; GoReleaser reuses the draft and replaces duplicate assets before publication. Reruns reuse that commit's tag, and a release already published by a previous attempt is left unchanged. No user-managed personal access token is needed: only the publishing job receives the repository-scoped `GITHUB_TOKEN` with `contents: write`.

The release-upload path cannot be fully exercised without creating a real release. Local snapshot builds, installer/version tests, workflow lint, and PR CI validate the nonpublishing path. Mac artifacts are not Developer ID signed or notarized.

Intermediate Mac artifacts are retained for seven days. If they have expired, rerun all workflow jobs to rebuild them, rather than only rerunning the failed publishing job.

## Public repository trust boundary

PR verification and releases use ephemeral GitHub-hosted runners. PR tokens are read-only and checkout credential persistence is disabled. The release workflow triggers only on pushes to `main`, never on a PR or `pull_request_target`. Review changes before merging: code merged into `main` is trusted release code and can use the publishing job's token.

The former private-repository self-hosted CI workflow is disabled. Every outside contributor's workflow requires maintainer approval. The dedicated persistent Tempo runner on Titan remains disabled/stopped; other Titan services were left unchanged. Release execution no longer depends on Titan. Future self-hosted builds require a separately approved isolated execution environment: editable workflow conditions do not protect a persistent server from malicious workflow edits.
