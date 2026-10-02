#!/bin/sh
# Download this script, inspect it, then run with a pinned release version.
set -eu
fail() { printf '%s\n' "tempo installer: $*" >&2; exit 1; }
version=
bin_dir=${HOME:?HOME must be set}/.local/bin
while [ "$#" -gt 0 ]; do
  case "$1" in
    --version) [ "$#" -ge 2 ] || fail '--version needs a value'; version=$2; shift 2 ;;
    --bin-dir) [ "$#" -ge 2 ] || fail '--bin-dir needs a value'; bin_dir=$2; shift 2 ;;
    --help|-h) printf '%s\n' 'Usage: sh install.sh --version vX.Y.Z [--bin-dir DIR]'; exit 0 ;;
    *) fail 'unknown argument; use --help' ;;
  esac
done
case "$version" in *[!v0-9.]*) fail 'version contains invalid characters' ;; esac
printf '%s\n' "$version" | grep -Eq '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' || fail 'a pinned --version vX.Y.Z is required'
[ "$(id -u)" -ne 0 ] || fail 'run as your normal user, without sudo'
[ -n "$bin_dir" ] || fail '--bin-dir must not be empty'
case "$bin_dir" in /*) ;; *) bin_dir=$PWD/$bin_dir ;; esac
case "$(uname -s)" in Darwin) os=darwin ;; Linux) os=linux ;; *) fail 'supported systems: macOS and Linux' ;; esac
case "$(uname -m)" in arm64|aarch64) arch=arm64 ;; x86_64|amd64) arch=amd64 ;; *) fail 'supported architectures: arm64 and amd64' ;; esac
for cmd in curl tar awk grep mktemp; do command -v "$cmd" >/dev/null 2>&1 || fail "missing required tool: $cmd"; done
if command -v sha256sum >/dev/null 2>&1; then hash_tool=sha256sum
elif command -v shasum >/dev/null 2>&1; then hash_tool=shasum
else fail 'sha256sum or shasum is required'; fi
umask 077
tmp=$(mktemp -d "${TMPDIR:-/tmp}/tempo-install.XXXXXXXX")
staged=
cleanup() { [ -z "$staged" ] || rm -f "$staged"; rm -rf "$tmp"; }
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
archive=tempo_${version#v}_${os}_${arch}.tar.gz
base=https://github.com/rbeene/tempo/releases/download/$version
fetch() { curl --fail --silent --show-error --location --proto '=https' --proto-redir '=https' --tlsv1.2 --connect-timeout 10 --max-time 120 --output "$2" "$1"; }
fetch "$base/$archive" "$tmp/archive.tar.gz" || fail 'archive download failed; confirm that the pinned release is published'
fetch "$base/checksums-$os.txt" "$tmp/checksums.txt" || fail 'checksum download failed'
expected=$(awk -v name="$archive" '$2==name {print $1}' "$tmp/checksums.txt")
printf '%s\n' "$expected" | grep -Eq '^[0-9a-fA-F]{64}$' || fail 'missing, duplicate or invalid checksum entry'
[ "$(printf '%s\n' "$expected" | wc -l | tr -d ' ')" = 1 ] || fail 'duplicate checksum entry'
if [ "$hash_tool" = sha256sum ]; then actual=$(sha256sum "$tmp/archive.tar.gz" | awk '{print $1}')
else actual=$(shasum -a 256 "$tmp/archive.tar.gz" | awk '{print $1}'); fi
[ "$(printf '%s' "$expected" | tr 'A-F' 'a-f')" = "$actual" ] || fail 'checksum mismatch; existing installation unchanged'
# Do not unpack the archive into the filesystem: stream exactly one regular
# top-level executable member. Symlinks, traversal and duplicate members fail.
tar -tzf "$tmp/archive.tar.gz" > "$tmp/members" || fail 'invalid archive'
[ "$(grep -c '^tempo$' "$tmp/members" || true)" = 1 ] || fail 'archive must contain exactly one top-level tempo file'
if grep -Eq '(^/|(^|/)\.\.(/|$))' "$tmp/members"; then fail 'unsafe archive path'; fi
if grep -Ev '^(tempo|README\.md|docs/|docs/commands\.md)$' "$tmp/members" >/dev/null; then fail 'unexpected archive member'; fi
tar -tvzf "$tmp/archive.tar.gz" tempo > "$tmp/type" || fail 'cannot inspect executable'
[ "$(wc -l < "$tmp/type" | tr -d ' ')" = 1 ] || fail 'duplicate executable'
grep -q '^-' "$tmp/type" || fail 'executable must be a regular file'
tar -xOzf "$tmp/archive.tar.gz" tempo > "$tmp/tempo" || fail 'cannot extract executable'
[ -s "$tmp/tempo" ] || fail 'executable is empty'
mkdir -p "$bin_dir" || fail 'cannot create destination directory'
[ -w "$bin_dir" ] || fail 'destination directory is not writable'
[ ! -d "$bin_dir/tempo" ] || fail 'destination tempo is a directory'
staged=$(mktemp "$bin_dir/.tempo.XXXXXXXX")
cat "$tmp/tempo" > "$staged"
chmod 755 "$staged"
mv -f "$staged" "$bin_dir/tempo"
staged=
printf '%s\n' "Installed Tempo $version to $bin_dir/tempo" 'Add the destination directory to PATH if needed. Run tempo version to inspect it.'
