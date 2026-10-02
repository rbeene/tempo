# Tempo

Fast, personal Harvest time tracking from a single Go binary. Built for personal tracking and predictable agent use: explicit commands, complete pagination, stable JSON, and no interactive surprises.

Tempo supports secure authentication and account selection, assigned projects/tasks/clients, time entry CRUD, and running timers. It intentionally scopes time operations to the authenticated user. Harvest's invoicing, expense and account-administration APIs are outside this time-tracking CLI.

## Install and update

Release archives and a checksum-verifying installer are configured with GoReleaser. No first release has been published yet. See [distribution and installation](docs/distribution.md) for the pinned-version installer, platform support, and release process.

## Build

Go 1.27.1 is pinned in `mise.toml` and `go.mod`. Use `mise install` to set up the project toolchain. A normal macOS build needs Apple's command-line developer tools for the native Keychain bridge.

```sh
make build
bin/tempo help
bin/tempo schema
```

`bin/tempo` is the complete executable. It links only to standard OS frameworks on macOS. `make build VERSION=...` embeds a version and strips normal debug symbols. No installation into your PATH is performed automatically. Non-macOS or `CGO_ENABLED=0` builds support environment authentication and cannot persist tokens.

## Authenticate

Create your own personal access token on [Harvest's developer page](https://id.getharvest.com/developers). Tempo does not register an OAuth application, create credentials, or request secrets in chat. Personal token authentication is appropriate for this personal CLI; browser OAuth/refresh-token management is not implemented.

Use the local hidden-input helper below if starting from a token you manually obtained. It reads from your terminal without echo and pipes directly to Tempo, avoiding shell history, command arguments, and plaintext files:

```sh
python3 -c 'import getpass; print(getpass.getpass("Harvest token: "))' | bin/tempo auth login --token-stdin --account 12345
```

Omit `--account` only when exactly one Harvest account is accessible. Login validates access before saving into macOS Keychain. For a secret manager or automated environment, provide `HARVEST_TOKEN` and `HARVEST_ACCOUNT_ID` securely; environment values override saved credentials/account. Do not put a literal token in a shell command or commit it to a file. `--account ID` has the highest account-selection precedence.

```sh
bin/tempo auth status
bin/tempo auth status --check --json
bin/tempo accounts list --json
bin/tempo accounts use 12345
bin/tempo projects list --json
bin/tempo tasks list --project 100 --json
```

## Track time

```sh
bin/tempo time create --project 100 --task 200 --date today --duration 1h30m --notes 'Tempo implementation'
bin/tempo time list --from 2026-10-01 --to 2026-10-01 --json
bin/tempo time update 300 --notes 'Updated description'
bin/tempo timer start --project 100 --task 200 --notes 'Review'
bin/tempo timer status --json
bin/tempo timer stop
bin/tempo time delete 300 --yes
```

IDs above are examples. Discover your actual IDs first. These commands write to Harvest only when you run them with your own credentials. Timestamp-mode accounts use `--start HH:MM --end HH:MM` for completed time. Destructive deletion and logout require `--yes`. Starting another timer while one runs returns a conflict; stop the current timer explicitly.

## Agent contract

Use `--json --non-interactive`, inspect exit status and consume one JSON object. Success goes to stdout and failures to stderr, always with `schema_version: 1`. JSON preserves large numeric IDs. `tempo schema` and [the checked-in schema](docs/cli-schema.json) describe commands, flags, envelopes, precedence and exit codes without loading credentials or calling the network.

Exit 8 means a write's result is uncertain. Do not blindly retry it: inspect `time list`, `time show` or `timer status` first. List commands return all pages or an error; they never report partial results as complete. API error bodies and raw transport errors are suppressed to prevent secret leaks.

See [command reference](docs/commands.md), [architecture and security boundaries](docs/architecture.md), and [verification evidence](docs/verification.md).

## Development

```sh
make check
make build
```

Tests use mock HTTP and injected credential stores, never a real Harvest account or Keychain. See [AGENTS.md](AGENTS.md). PR CI is one GitHub-hosted Linux job with no matrix. Native Mac release builds run on a hosted Mac, followed by one trusted Titan packaging job. The Titan runner stays parked until a separately approved execution-isolation boundary exists. The repository is public; no release or installation has been performed.

Primary API references: [authentication](https://help.getharvest.com/api-v2/authentication-api/authentication/authentication/), [time entries](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/), [pagination](https://help.getharvest.com/api-v2/introduction/overview/pagination/), and [user project assignments](https://help.getharvest.com/api-v2/users-api/users/project-assignments/).
