"""Native application acceptance, ONLY on the existing GitHub-hosted Linux job.

No callback payloads are fabricated here. The model speaks synthetic Responses
SSE; actual Codex launches the built Tempo command. No host is run on import.
Raw requests and terminal screens stay bounded, transient, and unlogged.
"""
import argparse
import codecs
import fcntl
import hashlib
import http.server
import json
import os
from pathlib import Path
import platform
import pty
import pwd
import re
import select
import shutil
import signal
import stat
import struct
import subprocess
import sys
import tarfile
import tempfile
import termios
import threading
import time
import urllib.request

EVENTS = ("PreToolUse", "PostToolUse", "SessionStart", "SessionEnd", "UserPromptSubmit",
          "SubagentStart", "SubagentStop", "Stop", "Interrupt")
EVENT_ORDER = ("PreToolUse", "PermissionRequest", "PostToolUse", "PreCompact", "PostCompact",
               "SessionStart", "SessionEnd", "UserPromptSubmit", "SubagentStart", "SubagentStop", "Stop", "Interrupt")
INSTALLED_EVENTS = EVENT_ORDER
MODEL = "tempo-ci-fixture"
PARENT_PROMPT = "tempo-native-parent-case"
TITLE_PROMPT = ("Generate a concise, single-line task title of at most 36 characters and under five words where possible. "
                "Start with an imperative verb. Capitalize only the first word unless the user's language, proper nouns, acronyms, or code terms require otherwise. "
                "Preserve ticket references exactly. Write in the user's language. Do not use quotes, markdown, or trailing punctuation. Do not answer the request."
                "\n\nUser prompt:\n" + PARENT_PROMPT)
TITLE_FORMAT = {"type": "json_schema", "strict": True, "name": "codex_output_schema", "schema": {
    "type": "object", "properties": {"title": {"type": "string", "minLength": 1, "maxLength": 36}},
    "required": ["title"], "additionalProperties": False}}
CHILD_PROMPT = "tempo-native-child-case"
INTERRUPT_PROMPT = "tempo-native-interrupt-case"
PLAN_RESULT = "Plan updated"
TOKEN = "tempo-ci-invalid-synthetic-token"


class FixtureFailure(Exception):
    """Contains only a fixed safe category, never an underlying exception."""


def require(value, code):
    if not value:
        raise FixtureFailure(code)


def require_installed_definitions(path, tempo, host, events):
    """Inspect production output independently; never author hook definitions."""
    require(host in ("codex", "claude") and tempo.is_absolute()
            and re.fullmatch(r"[A-Za-z0-9/_-]+", str(tempo)), "unsafe_fixture_path")
    command = "'" + str(tempo) + "' hook " + host + " --input-stdin"
    require(len(command) <= 200, "unsafe_fixture_path")
    def unique_fields(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "installed_definitions_mismatch")
            result[key] = value
        return result
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
            info = os.fstat(source.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_size <= 8 << 20, "installed_definitions_mismatch")
            data = source.read((8 << 20) + 1)
        require(len(data) <= 8 << 20, "installed_definitions_mismatch")
        doc = json.loads(data, object_pairs_hook=unique_fields)
    except (OSError, UnicodeError, ValueError):
        raise FixtureFailure("installed_definitions_mismatch")
    expected = {"hooks": {event: [{"hooks": [{"type": "command", "command": command, "timeout": 2}]}]
                          for event in events}}
    require(doc == expected, "installed_definitions_mismatch")
    return command


def require_installed_profile(result, host, scope, project, version, confirmed):
    require(isinstance(result, dict) and result.get("contract_version") == 1
            and isinstance(result.get("hooks"), list) and len(result["hooks"]) == 1, "installed_profile_missing")
    row = result["hooks"][0]
    require(isinstance(row, dict) and row.get("host") == host and row.get("scope") == scope
            and row.get("path") == str(project) and row.get("runtime_version") == version
            and row.get("state") == ("awaiting_real_event" if confirmed else "approval_required")
            and row.get("ordering") == ("supported" if confirmed else "unavailable")
            and "last_real_event" in row and row["last_real_event"] is None, "installed_profile_missing")
    profile = row.get("profile")
    require(isinstance(profile, dict) and profile.get("basis") == ("operator_declared" if confirmed else "none")
            and profile.get("capture_eligible") is confirmed
            and isinstance(profile.get("fingerprint"), str) and re.fullmatch("[a-f0-9]{64}", profile["fingerprint"])
            and profile.get("declaration_version") == "tempo-native-hooks-v1", "installed_profile_missing")
    context = profile.get("context")
    require(isinstance(context, dict) and context.get("inventory_version") == "tempo-installed-static-v1"
            and context.get("host") == host and context.get("scope") == scope and context.get("path") == str(project)
            and context.get("runtime_version") == version and context.get("surface") == "local"
            and context.get("conflicts") == [], "installed_profile_missing")
    artifacts = context.get("artifacts")
    require(isinstance(artifacts, list) and 4 <= len(artifacts) <= 128, "installed_profile_missing")
    paths, roles = set(), set()
    for item in artifacts:
        require(isinstance(item, dict) and item.get("role") in ("runtime", "executable", "definitions", "skill", "configuration", "repository")
                and isinstance(item.get("path"), str) and len(item["path"]) <= 4096 and Path(item["path"]).is_absolute()
                and item["path"] not in paths and isinstance(item.get("sha256"), str)
                and (item["sha256"] == "absent" or re.fullmatch("[a-f0-9]{64}", item["sha256"])), "installed_profile_missing")
        paths.add(item["path"]); roles.add(item["role"])
    require({"runtime", "executable", "definitions", "skill"} <= roles, "installed_profile_missing")
    return profile


def require_unchanged_profile(before, after):
    # Genuine production Status resamples all files, absent sources and repository
    # identities. Compare the complete inventory, not only the installed file.
    require(before["fingerprint"] == after["fingerprint"] and before["context"] == after["context"],
            "installed_profile_drift")
    return {"available": True, "all_matches": True, "artifact_count": len(after["context"]["artifacts"]),
            "sampled_by": "production_status"}


def hosted_precondition(env, system, machine, user_home):
    require(system == "Linux" and machine == "x86_64", "hosted_platform_required")
    require(env.get("GITHUB_ACTIONS") == "true" and env.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and env.get("RUNNER_OS") == "Linux" and env.get("RUNNER_ARCH") == "X64", "hosted_runner_required")
    require("CODEX_HOME" not in env, "unexpected_codex_home")
    require(env.get("HOME") == user_home == "/home/runner", "unchanged_runner_home_required")
    require(bool(env.get("RUNNER_TEMP")) and Path(env["RUNNER_TEMP"]).is_absolute(), "runner_temp_required")


def require_absent(paths):
    for path in paths:
        # lexists observes only the path, including dangling symlinks. No opening,
        # removing, or traversing unexpected host config/auth/session contents.
        require(not os.path.lexists(path), "unexpected_host_state")


def child_environment(parent, root):
    require("CODEX_HOME" not in parent, "unexpected_codex_home")
    return {"HOME": parent["HOME"], "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "TERM": "xterm-256color", "TMPDIR": str(root / "tmp"),
            "TEMPO_STATE": str(root / "activity.json"), "TEMPO_HOOK_STATE": str(root / "hooks-state.json"),
            "TEMPO_CI_PROVIDER_TOKEN": TOKEN, "TEMPO_HOOK_DIAGNOSTICS": "1"}


def terminate_group(proc):
    if proc is None:
        return
    # Kill the owned group even if its leader has already exited (tool children).
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait(timeout=2)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def bounded_run(argv, env, cwd, timeout=8):
    proc = subprocess.Popen(argv, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    streams = {proc.stdout: bytearray(), proc.stderr: bytearray()}
    deadline = time.monotonic() + timeout
    try:
        live = list(streams)
        while live:
            require(time.monotonic() < deadline, "fixture_subprocess_deadline")
            for stream in select.select(live, [], [], .05)[0]:
                chunk = os.read(stream.fileno(), 16384)
                if not chunk:
                    live.remove(stream)
                    continue
                streams[stream].extend(chunk)
                limit = 1024 * 1024 if stream is proc.stdout else 16384
                require(len(streams[stream]) <= limit, "fixture_subprocess_output_bound")
        require(time.monotonic() < deadline, "fixture_subprocess_deadline")
        proc.wait(timeout=max(.001, deadline-time.monotonic()))
        require(proc.returncode == 0, "fixture_subprocess_failed")
        return bytes(streams[proc.stdout])
    finally:
        try:
            terminate_group(proc)
        finally:
            proc.stdout.close(); proc.stderr.close()


def prepare_diagnostic_command(tempo, path):
    require(tempo.is_absolute() and path.is_absolute() and tempo.parent == path.parent
            and all(re.fullmatch(r"[A-Za-z0-9/_-]+", str(p)) for p in (tempo, path)), "unsafe_fixture_path")
    command = str(tempo) + " hook codex --input-stdin 2>> " + str(path)
    require(len(command) <= 200, "unsafe_fixture_path")
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
    except OSError:
        raise FixtureFailure("diagnostic_setup_failed")
    return command


def diagnostic_file_matches(info, identity):
    return (stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600 and info.st_uid == os.getuid()
            and (info.st_dev, info.st_ino) == (identity.st_dev, identity.st_ino))


def reset_hook_diagnostics(path, identity):
    try:
        fd = os.open(path, os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            require(diagnostic_file_matches(os.fstat(fd), identity), "diagnostic_setup_failed")
            os.ftruncate(fd, 0)
        finally:
            os.close(fd)
    except OSError:
        raise FixtureFailure("diagnostic_setup_failed")


def hook_diagnostic_probe(path, identity):
    unavailable = {"status": "unavailable", "counts": []}
    codes = ("validation", "unsupported_contract", "state_corrupt", "state_busy", "local_write_unknown",
             "clock_unavailable", "clock_conflict", "binding_unavailable", "event_conflict", "event_gap",
             "ordering_unavailable", "profile_required", "profile_invalidated", "profile_revoked", "untracked",
             "review_required", "source_lost", "restart_unknown", "incomplete_wait", "source_loss_while_waiting", "internal")
    try:
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
            before = os.fstat(source.fileno())
            if not diagnostic_file_matches(before, identity) or before.st_size > 16384: return unavailable
            data = source.read(16385)
            after = os.fstat(source.fileno())
        if (not diagnostic_file_matches(os.lstat(path), identity) or len(data) > 16384
                or len(data) != before.st_size or (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)):
            return unavailable
        if data and not data.endswith(b"\n"): return unavailable
        lines = data.decode("ascii").split("\n")[:-1] if data else []
        if len(lines) > 128: return unavailable
        counts = {}
        for line in lines:
            match = re.fullmatch(r"tempo hook: (" + "|".join(codes) + r"); durability=(not_committed|committed|unknown)", line)
            if match is None: return unavailable
            key = match.groups()
            counts[key] = counts.get(key, 0) + 1
        return {"status": "available", "counts": [{"code": code, "durability": durability, "count": count}
                for (code, durability), count in sorted(counts.items())]}
    except (OSError, UnicodeError):
        return unavailable


def artifact_match_probe(baseline):
    result = {"available": True}
    for role, limit in (("runtime", 320 << 20), ("executable", 256 << 20), ("definitions", 8 << 20), ("configuration", 8 << 20)):
        result[role + "_matches"] = False
        path, expected = baseline[role]
        try:
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), "rb") as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > limit: raise OSError()
                hashed, size = hashlib.sha256(), 0
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    size += len(chunk)
                    if size > limit: raise OSError()
                    hashed.update(chunk)
                after = os.fstat(source.fileno())
                if size != info.st_size or (info.st_size, info.st_mtime_ns) != (after.st_size, after.st_mtime_ns): raise OSError()
            current = os.lstat(path)
            if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != (info.st_dev, info.st_ino): raise OSError()
            result[role + "_matches"] = hashed.hexdigest() == expected
        except OSError:
            result["available"] = False
    return result


def project_receipt(value):
    # Explicit projection, including enumerated provenance. No diagnostic text,
    # paths, transcript fields, arbitrary extra keys, or provider material.
    require(value.get("source") == "codex" and value.get("origin") == "unverified", "receipt_provenance")
    enums = {"kind": EVENTS, "disposition": ("applied", "duplicate", "stale", "untracked", "review_required"),
             "ordering": ("supported", "review_required", "unavailable"),
             "durability": ("committed", "not_committed", "unknown"),
             "profile_basis": ("operator_declared", "", "unknown", "unverified")}
    result = {"source": "codex", "origin": "unverified"}
    for key, allowed in enums.items():
        require(value.get(key, "") in allowed, "receipt_status")
        result[key] = value.get(key, "")
    for key in ("id", "session_id", "turn_id", "agent_id", "tool_id", "snapshot_revision", "profile_revision"):
        item = value.get(key, "")
        require(isinstance(item, str) and re.fullmatch(r"[A-Za-z0-9_.:/=-]{0,192}", item) is not None, "receipt_identifier")
        result[key] = item
    fp = value.get("fingerprint", "")
    require(re.fullmatch(r"[a-f0-9]{64}|", fp) is not None, "receipt_fingerprint")
    result["fingerprint"] = fp
    return result


def accepted(receipt):
    return receipt.get("disposition") in ("applied", "duplicate") and receipt.get("durability") == "committed" and receipt.get("profile_basis") == "operator_declared"


def receipt_gate_probe(receipts, baseline):
    # Observe the exact gate snapshot without retaining IDs, payloads or paths.
    try:
        if not isinstance(baseline, (set, frozenset)):
            return {"status": "unarmed"}
        if not isinstance(receipts, list):
            return {"status": "invalid_response"}
        if len(receipts) > 128:
            return {"status": "overflow", "receipt_count": 129}
        counts, starts, postbaseline = {}, 0, 0
        for row in receipts:
            if (not isinstance(row, dict) or not isinstance(row.get("id"), str)
                    or row.get("kind") not in INSTALLED_EVENTS
                    or row.get("disposition") not in ("applied", "duplicate", "stale", "untracked", "review_required")
                    or row.get("durability") not in ("committed", "not_committed", "unknown")):
                return {"status": "invalid_response"}
            if row["id"] in baseline:
                continue
            postbaseline += 1
            key = row["kind"], row["disposition"], row["durability"]
            counts[key] = counts.get(key, 0) + 1
            starts += int(row["kind"] == "SessionStart" and accepted(row))
        return {"status": "available", "receipt_count": len(receipts), "postbaseline_count": postbaseline,
                "accepted_session_starts": starts,
                "counts": [{"kind": kind, "disposition": disposition, "durability": durability, "count": count}
                           for (kind, disposition, durability), count in sorted(counts.items())]}
    except Exception:
        return {"status": "invalid_response"}


def failed_profile_probe(result, confirmed, project, version):
    # Production Status can return a retained context when fresh inspection
    # fails. Never label those old hashes as a sampled current inventory.
    invalid = {"status": "invalid_response"}
    try:
        require(isinstance(result, dict) and type(result.get("contract_version")) is int
                and result["contract_version"] == 1 and isinstance(result.get("hooks"), list)
                and len(result["hooks"]) == 1, "diagnostic_response_invalid")
        row = result["hooks"][0]
        require(isinstance(row, dict) and row.get("host") == "codex" and row.get("scope") == "user"
                and row.get("path") == str(project) and row.get("runtime_version", "") in ("", version)
                and row.get("state") in ("not_installed", "approval_required", "awaiting_real_event", "unsupported", "needs_repair")
                and row.get("ordering") in ("supported", "unavailable"), "diagnostic_response_invalid")
        profile = row.get("profile")
        require(isinstance(profile, dict) and profile.get("basis") in ("none", "operator_declared")
                and profile.get("state") in ("absent", "eligible", "invalidated", "revoked")
                and type(profile.get("capture_eligible")) is bool, "diagnostic_response_invalid")
        revision = profile.get("revision")
        require(isinstance(revision, str) and re.fullmatch(r"0|[1-9][0-9]{0,19}", revision)
                and int(revision) < 2**64, "diagnostic_response_invalid")
        code = profile.get("diagnostic_code", "")
        require(code in ("", "profile_required", "operator_declared_risk", "profile_invalidated", "profile_revoked"),
                "diagnostic_response_invalid")
        diagnostics = row.get("diagnostics")
        require(isinstance(diagnostics, list) and len(diagnostics) <= 16, "diagnostic_response_invalid")
        codes = []
        for item in diagnostics:
            require(isinstance(item, dict) and item.get("code") in
                    ("project_context_required", "unsupported_contract", "ordering_unavailable", "operator_declared_risk"),
                    "diagnostic_response_invalid")
            codes.append(item["code"])
        fingerprint = profile.get("fingerprint", "")
        require(isinstance(fingerprint, str) and re.fullmatch(r"[a-f0-9]{64}|", fingerprint), "diagnostic_response_invalid")
        projected = {"status": "available", "hook_state": row["state"], "ordering": row["ordering"],
                     "basis": profile["basis"], "profile_state": profile["state"],
                     "capture_eligible": profile["capture_eligible"], "revision": revision,
                     "revision_matches": revision == confirmed.get("revision"),
                     "fingerprint_matches": fingerprint == confirmed.get("fingerprint"),
                     "diagnostic_code": code, "diagnostic_codes": sorted(set(codes)),
                     "inventory": {"status": "unavailable"}}
        if (row.get("pending") is not None or row["state"] in ("not_installed", "needs_repair")
                or any(c in codes for c in ("unsupported_contract", "project_context_required"))):
            return projected
        roles = ("runtime", "executable", "definitions", "skill", "configuration", "repository")
        def inventory(value):
            require(isinstance(value, dict), "diagnostic_response_invalid")
            context = value.get("context")
            require(isinstance(context, dict) and context.get("inventory_version") == "tempo-installed-static-v1"
                    and context.get("host") == "codex" and context.get("scope") == "user"
                    and context.get("path") == str(project) and context.get("runtime_version") == version
                    and context.get("surface") == "local", "diagnostic_response_invalid")
            artifacts = context.get("artifacts")
            require(isinstance(artifacts, list) and 4 <= len(artifacts) <= 128, "diagnostic_response_invalid")
            values, paths = {}, set()
            for item in artifacts:
                require(isinstance(item, dict) and item.get("role") in roles
                        and isinstance(item.get("path"), str) and len(item["path"]) <= 4096
                        and Path(item["path"]).is_absolute() and item["path"] not in paths
                        and isinstance(item.get("sha256"), str)
                        and (item["sha256"] == "absent" or re.fullmatch(r"[a-f0-9]{64}", item["sha256"])),
                        "diagnostic_response_invalid")
                paths.add(item["path"])
                values[item["role"], item["path"]] = item["sha256"]
            require({"runtime", "executable", "definitions", "skill"} <= {key[0] for key in values},
                    "diagnostic_response_invalid")
            return values
        before, after = inventory(confirmed), inventory(profile)
        projected["inventory"] = {"status": "available", "all_matches": before == after,
            "confirmed_count": len(before), "current_count": len(after),
            "roles": [{"role": role, "confirmed_count": sum(key[0] == role for key in before),
                       "current_count": sum(key[0] == role for key in after),
                       "matching_count": sum(key[0] == role and key in after and after[key] == value
                                             for key, value in before.items())} for role in roles]}
        return projected
    except Exception:
        return invalid


def observe_failed_profile(read_status, confirmed, project, version, deadline, joined):
    # This optional read cannot replace the failure that led here or start work
    # while terminal/provider cleanup is incomplete. It never calls Eligibility.
    if joined is not True:
        return {"status": "cleanup_incomplete"}
    if confirmed is None:
        return {"status": "unconfirmed"}
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return {"status": "deadline_expired"}
        result = read_status(min(20, remaining))
        return failed_profile_probe(result, confirmed, project, version)
    except Exception:
        return {"status": "helper_failed"}


def require_prompt_barrier(receipts, session, turn):
    require(any(r["kind"] == "SessionStart" and r["session_id"] == session and accepted(r) for r in receipts), "session_start_barrier_missing")
    require(any(r["kind"] == "UserPromptSubmit" and r["session_id"] == session and r["turn_id"] == turn and accepted(r)
                and r["ordering"] == "supported" for r in receipts), "prompt_barrier_missing")


def require_completed_capture(snapshot):
    require(snapshot["queued"] > 0 and snapshot["uncertainties"] == 0 and snapshot["uncertainty_details"] == [],
            "completed_capture_effects_missing")
    require(type(snapshot.get("capture_reviews")) is int and snapshot["capture_reviews"] == 0,
            "completed_capture_effects_missing")
    return snapshot["queued"]


def require_capture_effects(snapshot, interrupt_receipt, completed_queued):
    expected = {"actor": interrupt_receipt["actor"], "reason": "source_lost", "state": "unresolved", "bounded": True,
                "resolution_present": False, "discarded": False}
    require(interrupt_receipt["actor"] is not None and completed_queued > 0 and snapshot["queued"] >= completed_queued
            and snapshot["uncertainties"] == 1 and snapshot["uncertainty_details"] == [expected], "native_capture_effects_missing")
    require(type(snapshot.get("capture_reviews")) is int and snapshot["capture_reviews"] == 0, "native_capture_effects_missing")


def require_plan_result(body):
    matches = [v for v in body.get("input", []) if v.get("type") == "function_call_output" and v.get("call_id") == "tempo-plan"]
    require(len(matches) == 1 and matches[0].get("output") == PLAN_RESULT, "actual_plan_result_missing")


def review_hook_screen(screen, event, command, source):
    lines = [line.strip() for line in screen.splitlines() if line.strip()]
    # Pinned 0.159.3 renders labels padded to 10 columns, without colons. Accept
    # colons only for the pure textual oracle's explicit test representation.
    def field(label):
        found = [re.sub(r"^" + label + r"\s*:?[ ]*", "", line) for line in lines if re.match(r"^" + label + r"(?:\s|:)", line)]
        require(len(found) == 1, "hook_review_field")
        return found[0]
    require(event + " hooks" in lines, "hook_review_event")
    require(len(re.findall(r"\[[! x]\] Hook \d+", screen)) == 1 and "[!] Hook 1" in screen, "hook_review_count")
    require(field("Event") == event and field("Command") == command, "hook_review_command")
    require(field("Source") in ("User config - " + source, "User - " + source), "hook_review_source")
    require(field("Mode") == "Sync" and field("Timeout") == "2s", "hook_review_mode")
    require(field("Trust") == "New hook - review required" and "trust ·" in screen, "hook_review_trust")
    require("Issues" not in lines, "hook_review_issues")


class Screen:
    """Small bounded VT screen used for observation, not an append-only transcript.

    Handles the cursor/erase/scroll operations emitted by the pinned Ratatui TUI.
    Unrecognized display-changing controls fail closed. Color/mode queries don't
    contribute text. Nothing from this screen is persisted or printed.
    """
    def __init__(self, rows=80, cols=240):
        self.rows, self.cols = rows, cols
        self.grid = [[" "] * cols for _ in range(rows)]
        self.row = self.col = 0
        self.scroll_top, self.scroll_bottom = 0, rows - 1
        self.saved = (0, 0)
        self.pending = ""
        self.decoder = codecs.getincrementaldecoder("utf-8")("replace")

    def text(self):
        return "\n".join("".join(row).rstrip() for row in self.grid)

    def scroll(self, count, down=False):
        for _ in range(min(count, self.scroll_bottom - self.scroll_top + 1)):
            if down:
                self.grid.pop(self.scroll_bottom)
                self.grid.insert(self.scroll_top, [" "] * self.cols)
            else:
                self.grid.pop(self.scroll_top)
                self.grid.insert(self.scroll_bottom, [" "] * self.cols)

    def linefeed(self):
        if self.row == self.scroll_bottom:
            self.scroll(1)
        else:
            self.row = min(self.rows - 1, self.row + 1)

    def feed(self, raw):
        data = self.pending + self.decoder.decode(raw)
        self.pending = ""
        i = 0
        while i < len(data):
            ch = data[i]
            if ch == "\x1b":
                if i + 1 == len(data):
                    self.pending = data[i:]
                    break
                if data[i + 1] == "[":
                    match = re.match(r"\x1b\[([0-?]*)([ -/]*)([@-~])", data[i:])
                    if not match:
                        self.pending = data[i:]
                        require(len(self.pending) < 256, "terminal_control_overflow")
                        break
                    self.csi(match[1], match[3], match[2])
                    i += len(match[0])
                    continue
                if data[i + 1] == "]":
                    match = re.search(r"\x07|\x1b\\", data[i + 2:])
                    if not match:
                        self.pending = data[i:]
                        require(len(self.pending) < 4096, "terminal_control_overflow")
                        break
                    i += 2 + match.end()
                    continue
                code = data[i + 1]
                if code == "7":
                    self.saved = self.row, self.col
                elif code == "8":
                    self.row, self.col = self.saved
                elif code == "M":
                    if self.row == self.scroll_top:
                        self.scroll(1, down=True)
                    else:
                        self.row = max(0, self.row - 1)
                elif code not in ("=", ">", "\\"):
                    kind = {"(": "character_set", ")": "character_set", "D": "index", "E": "next_line"}.get(code, "other")
                    raise FixtureFailure("unsupported_terminal_escape_" + kind)
                i += 2
                continue
            if ch == "\r":
                self.col = 0
            elif ch == "\n":
                self.linefeed()
            elif ch == "\b":
                self.col = max(0, self.col - 1)
            elif ch == "\t":
                self.col = min(self.cols - 1, (self.col // 8 + 1) * 8)
            elif ord(ch) >= 32 and ch != "\x7f":
                if self.col >= self.cols:
                    self.col = 0
                    self.linefeed()
                self.grid[self.row][self.col] = ch
                self.col += 1
            i += 1

    def csi(self, params, op, intermediate=""):
        if op in "mhlncqt":  # attributes, mode switches, terminal queries
            return
        # Pinned Codex startup queries keyboard capabilities, then pushes/pops
        # enhancement flags. These CSI-u controls don't alter displayed cells.
        # Bare CSI-u remains the ordinary saved-cursor restore below.
        if op == "u" and params in ("?", ">5", ">7", "<1", "<"):
            return
        require(re.fullmatch(r"[0-9;]*", params) is not None, "unsupported_terminal_parameters")
        p = [int(v) if v else 0 for v in params.split(";")] if params else [0]
        n = p[0] or 1
        if op in "Hf":
            self.row = min(self.rows - 1, max(0, n - 1))
            self.col = min(self.cols - 1, max(0, (p[1] if len(p) > 1 and p[1] else 1) - 1))
        elif op == "A": self.row = max(0, self.row - n)
        elif op == "B": self.row = min(self.rows - 1, self.row + n)
        elif op == "C": self.col = min(self.cols - 1, self.col + n)
        elif op == "D": self.col = max(0, self.col - n)
        elif op == "G": self.col = min(self.cols - 1, n - 1)
        elif op == "d": self.row = min(self.rows - 1, n - 1)
        elif op == "E": self.row, self.col = min(self.rows - 1, self.row + n), 0
        elif op == "F": self.row, self.col = max(0, self.row - n), 0
        elif op == "J":
            if p[0] in (2, 3):
                self.grid = [[" "] * self.cols for _ in range(self.rows)]
            elif p[0] == 0:
                self.grid[self.row][self.col:] = [" "] * (self.cols - self.col)
                for r in range(self.row + 1, self.rows): self.grid[r] = [" "] * self.cols
            elif p[0] == 1:
                for r in range(self.row): self.grid[r] = [" "] * self.cols
                self.grid[self.row][:self.col + 1] = [" "] * (self.col + 1)
        elif op == "K":
            start, end = (0, self.cols) if p[0] == 2 else ((0, self.col + 1) if p[0] == 1 else (self.col, self.cols))
            self.grid[self.row][start:end] = [" "] * (end - start)
        elif op == "S":
            self.scroll(n)
        elif op == "T":
            self.scroll(n, down=True)
        elif op == "r":
            require(not intermediate, "unsupported_terminal_scroll_region")
            bottom = p[1] if len(p) == 2 and p[1] else self.rows
            require(len(p) <= 2 and 1 <= n < bottom <= self.rows, "unsupported_terminal_scroll_region")
            self.scroll_top, self.scroll_bottom = n - 1, bottom - 1
            self.row = self.col = 0
        elif op == "s": self.saved = self.row, self.col
        elif op == "u": self.row, self.col = self.saved
        else:
            kind = {"L": "insert_lines", "M": "delete_lines", "P": "delete_characters", "@": "insert_characters",
                    "X": "erase_characters", "b": "repeat_character"}.get(op, "other")
            raise FixtureFailure("unsupported_terminal_csi_" + kind)


def command_input_probe(screen, value):
    text = screen.text()
    row = "".join(screen.grid[screen.row]).rstrip()
    exact = re.fullmatch(r"[ ]*[›»] +" + re.escape(value), row) is not None
    lines = [line.strip() for line in text.splitlines()]
    return {"is_quit": value == "/quit", "model_label_present": MODEL in text,
            "modal_present": any(title in text for title in ("Trust this folder?", "Hooks need review",
                "Lifecycle hooks from config and enabled plugins.")) or any(event + " hooks" in lines for event in EVENT_ORDER),
            "cursor_row_exact_echo": exact, "cursor_at_echo_end": exact and screen.col == len(row),
            "enter_sent": False}


TERMINAL_WAIT_LABELS = frozenset({
    "browser_input_unavailable", "child_stop_missing", "command_echo_unavailable",
    "hook_browser_not_closed", "hook_details_unavailable", "hook_inventory_navigation",
    "hook_inventory_unavailable", "hook_trust_failed", "hooks_ui_unavailable",
    "interrupt_hook_missing", "interrupt_request_missing", "normal_exit_missing",
    "parent_stop_missing", "posttrust_session_start_missing", "startup_hooks_review_unavailable",
    "startup_review_input_unavailable", "startup_ui_unavailable", "trust_host_exit",
    "workspace_input_down_unavailable", "workspace_input_up_unavailable",
    "workspace_trust_failed", "workspace_trust_mismatch", "none", "other",
})
TERMINAL_INPUT_ACTIONS = {
    b"\r": "enter", b"\x1b": "escape", b"\x1b[A": "up", b"\x1b[B": "down",
    b"\x1b[F": "end", b"\x1b[H": "home", b"1": "review_shortcut", b"t": "trust_shortcut",
}
TERMINAL_ACTION_LABELS = frozenset(TERMINAL_INPUT_ACTIONS.values()) | {"none", "other", "command_paste"}
TERMINAL_QUERY_REPLIES = (b"\x1b[1;1R", b"\x1b[?1;2c", b"\x1b[>0;0;0c")


def terminal_failure_probe(terminal):
    probe = {"source": "cached_process_status_before_cleanup", "status": "unavailable",
             "termination": "unavailable", "returncode": None, "signal": None,
             "last_wait": "none", "last_input_action": "none"}
    try:
        # Existing polling owns status collection. Cleanup may change this cache.
        returncode = terminal.proc.returncode
        last_wait = getattr(terminal, "last_wait", "none")
        last_action = getattr(terminal, "last_input_action", "none")
    except Exception:
        return probe
    probe["last_wait"] = last_wait if type(last_wait) is str and last_wait in TERMINAL_WAIT_LABELS else "other"
    probe["last_input_action"] = last_action if type(last_action) is str and last_action in TERMINAL_ACTION_LABELS else "other"
    if returncode is None:
        probe["status"] = "not_observed"
    elif type(returncode) is int and -255 <= returncode <= 255:
        probe.update(status="observed", returncode=returncode,
                     termination="signal" if returncode < 0 else "exit",
                     signal=-returncode if returncode < 0 else None)
    else:
        probe["status"] = "invalid_status"
    return probe


class Terminal:
    def __init__(self, argv, env, cwd, deadline, input_probe=None):
        self.deadline = deadline
        self.input_probe = {} if input_probe is None else input_probe
        self.last_wait = "none"
        self.last_input_action = "none"
        self.screen = Screen()
        self.master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", self.screen.rows, self.screen.cols, 0, 0))
        try:
            self.proc = subprocess.Popen(argv, env=env, cwd=cwd, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        finally:
            os.close(slave)
        self.query_tail = b""

    def send(self, data):
        # Protocol replies must not hide the last attempted navigation action.
        if type(data) is not bytes:
            self.last_input_action = "other"
        elif data not in TERMINAL_QUERY_REPLIES:
            if data.startswith(b"\x1b[200~") and data.endswith(b"\x1b[201~"):
                self.last_input_action = "command_paste"
            else:
                self.last_input_action = TERMINAL_INPUT_ACTIONS.get(data, "other")
        os.write(self.master, data)

    def pump(self, wait=.05):
        require(time.monotonic() < self.deadline, "overall_deadline")
        if not select.select([self.master], [], [], wait)[0]:
            return
        try:
            data = os.read(self.master, 65536)
        except OSError:
            require(self.proc.poll() is not None, "terminal_read_failed")
            return
        query = self.query_tail + data
        if b"\x1b[6n" in query: self.send(b"\x1b[1;1R")
        if b"\x1b[c" in query: self.send(b"\x1b[?1;2c")
        if b"\x1b[>c" in query: self.send(b"\x1b[>0;0;0c")
        self.query_tail = query[-5:]
        self.screen.feed(data)

    def until(self, predicate, code, seconds=20):
        self.last_wait = code if type(code) is str and code in TERMINAL_WAIT_LABELS else "other"
        end = min(self.deadline, time.monotonic() + seconds)
        while time.monotonic() < end:
            self.pump()
            if predicate(self.screen.text()):
                return self.screen.text()
            require(self.proc.poll() is None, "host_exited_early")
        raise FixtureFailure(code)

    def command(self, value):
        # Normal bracketed paste clears the host's paste-burst Enter window.
        # A cursor-local whole-row echo excludes history and slash popup text.
        self.input_probe.clear()
        self.input_probe.update(command_input_probe(Screen(), value))
        self.send(b"\x1b[200~" + value.encode() + b"\x1b[201~")
        def echoed(_):
            self.input_probe.clear()
            self.input_probe.update(command_input_probe(self.screen, value))
            return (not self.input_probe["modal_present"]
                    and self.input_probe["cursor_row_exact_echo"] and self.input_probe["cursor_at_echo_end"])
        self.until(echoed, "command_echo_unavailable", seconds=5)
        self.send(b"\r")
        self.input_probe["enter_sent"] = True

    def close(self):
        terminate_group(self.proc)
        os.close(self.master)


def tool_definition(body, names):
    found = []
    for tool in body.get("tools", []):
        if tool.get("type") == "function" and tool.get("name") in names:
            found.append((tool, None))
        if tool.get("type") == "namespace":
            for child in tool.get("tools", []):
                if child.get("type") == "function" and child.get("name") in names:
                    found.append((child, tool.get("name")))
    require(len(found) == 1, "required_tool_schema_unavailable")
    return found[0]


def function_item(body, names, call_id, choices):
    definition, namespace = tool_definition(body, names)
    name = definition["name"]
    args = choices[name]
    schema = definition.get("parameters", {})
    require(set(schema.get("required", [])) <= set(args) and set(args) <= set(schema.get("properties", {})), "tool_schema_mismatch")
    item = {"type": "function_call", "call_id": call_id, "name": name, "arguments": json.dumps(args)}
    if namespace: item["namespace"] = namespace
    return item


def sse_events(response_id, item):
    return [{"type": "response.created", "response": {"id": response_id}},
            {"type": "response.output_item.done", "item": item},
            {"type": "response.completed", "response": {"id": response_id, "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}}}]


def message_item(text):
    return {"type": "message", "role": "assistant", "id": "msg-" + text, "content": [{"type": "output_text", "text": text}]}


# Model-visible start diagnostics are intentional product context, not an
# invocation log. Match only whole fixed developer text; keep no raw input.
CAPTURE_CONTEXT_CODES = frozenset((
    "validation", "unsupported_contract", "state_corrupt", "state_busy",
    "local_write_unknown", "clock_unavailable", "clock_conflict", "binding_unavailable",
    "event_conflict", "event_gap", "ordering_unavailable", "profile_required",
    "profile_invalidated", "profile_revoked", "untracked", "review_required",
    "source_lost", "restart_unknown", "incomplete_wait", "source_loss_while_waiting", "internal",
))
CAPTURE_CONTEXT_PATTERN = re.compile(
    r"tempo capture: kind=(SessionStart|SubagentStart); code=([a-z_]+); durability=(committed|not_committed|unknown)")


HOOK_DIAGNOSTIC_PREFIX = "\ntempo hook diagnostics v1: "
HOOK_DIAGNOSTIC_V2_PREFIX = "\ntempo hook diagnostics v2: "
HOOK_DIAGNOSTIC_PHASES = frozenset(("none", "other", "admission", "open", "begin", "prepare", "bind", "step", "commit", "verify", "rollback", "finalize", "close", "checkpoint"))
HOOK_DIAGNOSTIC_CATEGORIES = frozenset(("none", "other", "invalid", "busy", "canceled", "unsafe", "corrupt", "full", "constraint", "io", "closed", "misuse"))
HOOK_DIAGNOSTIC_KEYS = frozenset(("ordinal", "start_us", "end_us", "deadline_us", "caller_deadline_us", "phase_us", "caller", "retry", "native_phase", "native_category", "native_code", "native_cleanup"))


def hook_diagnostic_text(text):
    # A complete fixed envelope is required before extracting the unchanged
    # three-field public first line. Never search arbitrary model/user text.
    if not isinstance(text, str) or len(text) > 4096:
        return text, None
    prefixes = (HOOK_DIAGNOSTIC_PREFIX, HOOK_DIAGNOSTIC_V2_PREFIX)
    if sum(text.count(prefix) for prefix in prefixes) != 1: return text, None
    prefix = next(prefix for prefix in prefixes if prefix in text)
    version = 2 if prefix == HOOK_DIAGNOSTIC_V2_PREFIX else 1
    public, encoded = text.split(prefix)
    match = CAPTURE_CONTEXT_PATTERN.fullmatch(public)
    if not match or match[2] != "state_busy" or match[3] != "not_committed" or len(encoded) > 3072:
        return text, None
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value: raise ValueError("duplicate")
            value[key] = item
        return value
    try:
        rows = json.loads(encoded, object_pairs_hook=unique)
    except (ValueError, TypeError, RecursionError):
        return text, None
    if not isinstance(rows, list) or not 1 <= len(rows) <= 3: return text, None
    previous_end = 0
    for ordinal, row in enumerate(rows, 1):
        keys = HOOK_DIAGNOSTIC_KEYS | {"eligibility"} if version == 2 else HOOK_DIAGNOSTIC_KEYS
        if not isinstance(row, dict) or set(row) != keys: return text, None
        if any(type(row[key]) is not int for key in ("ordinal", "start_us", "end_us", "deadline_us", "caller_deadline_us", "native_code")): return text, None
        if row["ordinal"] != ordinal or not previous_end <= row["start_us"] <= row["end_us"] <= 120000000: return text, None
        if not -1 <= row["deadline_us"] <= 120000000 or not -1 <= row["caller_deadline_us"] <= 120000000 or not -(2**31) <= row["native_code"] < 2**31: return text, None
        if row["caller"] not in ("live", "canceled", "deadline") or type(row["retry"]) is not bool or type(row["native_cleanup"]) is not bool: return text, None
        if not isinstance(row["native_phase"], str) or not isinstance(row["native_category"], str) or row["native_phase"] not in HOOK_DIAGNOSTIC_PHASES or row["native_category"] not in HOOK_DIAGNOSTIC_CATEGORIES: return text, None
        stamps = row["phase_us"]
        if not isinstance(stamps, list) or len(stamps) != 8 or any(type(stamp) is not int or stamp < -1 or stamp > row["end_us"] for stamp in stamps): return text, None
        if version == 2 and not valid_eligibility_diagnostic(row["eligibility"]): return text, None
        previous_end = row["end_us"]
    record = {"kind": match[1], "attempts": rows}
    if version == 2: record["version"] = 2
    return public, record


def valid_eligibility_diagnostic(value):
    if not isinstance(value, dict) or set(value) != {"p", "r", "c", "d"} or value["d"] is not False: return False
    phases, roles, cpu = value["p"], value["r"], value["c"]
    if not isinstance(phases, list) or len(phases) != 7 or any(type(n) is not int or not -1 <= n <= 120000000 for n in phases): return False
    if not isinstance(roles, list) or len(roles) != 6: return False
    for role in roles:
        if not isinstance(role, list) or len(role) != 9: return False
        if any(type(n) is not int or not 0 <= n <= (1 << 40 if i < 4 else 120000000) for i, n in enumerate(role)): return False
        if role[0] > 128 or role[1] > role[0]: return False
    if sum(role[0] for role in roles) > 128: return False
    if not isinstance(cpu, list) or len(cpu) != 2 or any(type(n) is not int or not -1 <= n <= 120000000 for n in cpu): return False
    if (cpu[0] == -1) != (cpu[1] == -1): return False
    if phases[6] == -1:
        return all(n == -1 for n in phases) and all(n == 0 for role in roles for n in role) and cpu == [-1, -1]
    return all(n <= phases[6] for n in phases)


CPU_DIAGNOSTIC_FLAGS = ("sha_ni", "avx", "avx2", "bmi2", "sse4_1", "ssse3")


def observe_cpu_flags(joined):
    unavailable = {"status": "unavailable", "flags": {}}
    if joined is not True or platform.system() != "Linux": return unavailable
    try:
        # Fixed, bounded OS capability read only after all owned host/model joins.
        # Never export CPU descriptions or infer the crypto implementation used.
        with Path("/proc/cpuinfo").open("rb") as stream:
            raw = stream.read(1048577)
        if len(raw) > 1048576: return unavailable
        values = {name: True for name in CPU_DIAGNOSTIC_FLAGS}
        count = 0
        for line in raw.decode("ascii").splitlines():
            name, separator, content = line.partition(":")
            if name.strip() != "flags" or not separator: continue
            count += 1
            if count > 4096: return unavailable
            words = content.split()
            for flag in values: values[flag] = values[flag] and flag in words
        if not count: return unavailable
        return {"status": "observed", "flags": values}
    except FixtureFailure:
        raise
    except Exception:
        return unavailable


def capture_admission_diagnostics(body):
    records = []
    items = body.get("input")
    if not isinstance(items, list): return records
    for item in items:
        if not isinstance(item, dict) or item.get("role") != "developer": continue
        content = item.get("content")
        if not isinstance(content, list): continue
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "input_text": continue
            _, record = hook_diagnostic_text(part.get("text"))
            if record is not None and len(records) < 17: records.append(record)
    return records


def capture_context_tuples(body):
    observed = set()
    items = body.get("input")
    if not isinstance(items, list): return observed
    for item in items:
        if not isinstance(item, dict) or item.get("role") != "developer": continue
        content = item.get("content")
        if not isinstance(content, list): continue
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "input_text": continue
            text, _ = hook_diagnostic_text(part.get("text"))
            if not isinstance(text, str) or len(text) > 160: continue
            match = CAPTURE_CONTEXT_PATTERN.fullmatch(text)
            if match and match[2] in CAPTURE_CONTEXT_CODES:
                observed.add(match.groups())
    # The 2 MiB provider-body bound and finite 2 * 21 * 3 tuple space bound work/storage.
    return observed


class Model:
    def __init__(self, read, repo, deadline):
        self.read, self.repo, self.deadline = read, repo, deadline
        self.session = self.turn = self.child = self.interrupt_turn = None
        self.baseline = None
        self.initial_receipt_probe = {"status": "not_observed"}
        self.capture_contexts = set()
        self.child_turn = None
        self.lock = threading.Lock()
        self.error = None
        self.counts = {}
        self.entry_count = 0
        self.title_count = 0
        self.phase = "initial"
        self.child_seen = threading.Event()
        self.child_release = threading.Event()
        self.interrupt_seen = threading.Event()
        self.shutdown = threading.Event()
        self.requests = []
        model = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                # Bound request-line/header parsing too, including a connection
                # that sends no request. close() joins this non-daemon handler.
                self.connection.settimeout(5)
            def log_message(self, *_): pass
            def do_GET(self): self.reject()
            def reject(self):
                model.error = "unexpected_provider_endpoint"
                self.send_response(404); self.end_headers()
            def do_POST(self):
                try:
                    require(self.path == "/codex/v1/responses", "unexpected_provider_endpoint")
                    require(self.headers.get("Authorization") == "Bearer " + TOKEN, "synthetic_provider_auth")
                    require(self.headers.get("Content-Encoding", "identity") == "identity", "unexpected_request_encoding")
                    length = int(self.headers.get("Content-Length", "0"))
                    require(0 < length <= 2 * 1024 * 1024, "provider_request_bound")
                    self.connection.settimeout(5)
                    body = json.loads(self.rfile.read(length))
                    item, category, hold = model.respond(body)
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    if hold == "interrupt":
                        self.wfile.write(b'event: response.created\ndata: {"type":"response.created","response":{"id":"interrupt-held"}}\n\n')
                        self.wfile.flush()
                        model.interrupt_seen.set()
                        model.shutdown.wait(max(0, min(30, model.deadline-time.monotonic())))
                        return
                    if hold == "child":
                        model.child_seen.set()
                        require(model.child_release.wait(max(0, min(30, model.deadline-time.monotonic()))), "child_release_deadline")
                    for event in sse_events("response-" + category, item):
                        data = "event: " + event["type"] + "\ndata: " + json.dumps(event) + "\n\n"
                        self.wfile.write(data.encode())
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    if not model.interrupt_seen.is_set(): model.error = "provider_disconnected"
                except Exception as exc:
                    model.error = str(exc) if isinstance(exc, FixtureFailure) else "provider_protocol_failed"
                    try: self.send_error(400, "synthetic fixture rejected request")
                    except OSError: pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        # server_close joins every owned handler, not just the accept loop. Each
        # handler has bounded input/helper/response waits and observes shutdown.
        self.server.daemon_threads = False
        self.server.block_on_close = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.shutdown.set(); self.child_release.set()
        self.server.shutdown(); self.server.server_close(); self.thread.join(timeout=2)
        require(not self.thread.is_alive(), "provider_join_failed")

    def respond(self, body):
        with self.lock:
            self.entry_count = min(16, self.entry_count + 1)
        require(body.get("stream") is True and body.get("model") == MODEL, "provider_request_contract")
        with self.lock:
            self.capture_contexts.update(capture_context_tuples(body))
            records = getattr(self, "admission_diagnostics", [])
            for record in capture_admission_diagnostics(body):
                if record not in records:
                    if len(records) == 16:
                        self.admission_diagnostics_saturated = True
                    else:
                        records.append(record)
            self.admission_diagnostics = records
        # The newest actual user message distinguishes parent/child/interrupt;
        # nested tool arguments containing the child marker don't count.
        users = [item for item in body.get("input", []) if item.get("role") == "user"]
        require(bool(users), "provider_user_missing")
        output_format = (body.get("text") or {}).get("format")
        if output_format is not None or body.get("tools") == [] or users[-1].get("content") == [{"type": "input_text", "text": TITLE_PROMPT}]:
            # The TUI's separate ephemeral title thread has no tools or hooks.
            # It must never be mistaken for a primary lifecycle continuation.
            require(body.get("tools") == [] and output_format == TITLE_FORMAT
                    and output_format.get("strict") is True
                    and users[-1].get("content") == [{"type": "input_text", "text": TITLE_PROMPT}], "auxiliary_title_contract")
            with self.lock:
                require(not self.shutdown.is_set() and self.baseline is not None, "auxiliary_title_unarmed")
                require(self.title_count == 0, "duplicate_auxiliary_title")
                self.title_count += 1
                return message_item('{"title":"Verify native hooks"}'), "title1", None
        text = json.dumps(users[-1].get("content"))
        if INTERRUPT_PROMPT in text: category = "interrupt"
        elif CHILD_PROMPT in text: category = "child"
        elif PARENT_PROMPT in text: category = "parent"
        else: raise FixtureFailure("unexpected_provider_turn")
        with self.lock:
            require(not self.shutdown.is_set(), "provider_request_after_shutdown")
            try:
                snap = self.read()
                receipts = snap["receipts"]
            except Exception:
                if self.session is None:
                    self.initial_receipt_probe = {"status": "read_failed"}
                raise
            if self.session is None:
                self.initial_receipt_probe = receipt_gate_probe(receipts, self.baseline)
                require(self.baseline is not None and category == "parent" and self.phase == "initial", "request_before_measured_session")
                starts = [r for r in receipts if r["kind"] == "SessionStart" and r["id"] not in self.baseline and accepted(r)]
                require(len(starts) == 1, "measured_session_ambiguous")
                session = starts[0]["session_id"]
                prompts = [r for r in receipts if r["kind"] == "UserPromptSubmit" and r["id"] not in self.baseline
                           and r["session_id"] == session and not r["agent_id"] and accepted(r)]
                require(len(prompts) == 1, "parent_prompt_identity")
                require(prompts[0]["ordering"] == "supported", "prompt_barrier_missing")
                require_prompt_barrier(receipts, session, prompts[0]["turn_id"])
                self.session = session
            if category == "child":
                require(self.counts.get("child", 0) == 0, "duplicate_child_request")
                starts = [r for r in receipts if r["kind"] == "SubagentStart" and r["session_id"] == self.session and accepted(r)]
                require(len(starts) == 1 and starts[0]["agent_id"] and starts[0]["turn_id"], "child_start_barrier_missing")
                if self.child is not None:
                    require(self.child == starts[0]["agent_id"], "child_identity_mismatch")
                self.child = starts[0]["agent_id"]
                self.child_turn = starts[0]["turn_id"]
                item, hold = message_item("tempo-child-complete"), "child"
            else:
                prompts = [r for r in receipts if r["kind"] == "UserPromptSubmit" and r["session_id"] == self.session and not r["agent_id"] and accepted(r)]
                if category == "parent":
                    require(len(prompts) == 1, "parent_prompt_identity")
                    self.turn = prompts[0]["turn_id"]
                    require_prompt_barrier(receipts, self.session, self.turn)
                    if self.phase == "initial":
                        item = function_item(body, ("update_plan",), "tempo-plan", {
                            "update_plan": {"plan": [{"step": "Verify native hook lifecycle", "status": "completed"}]}})
                        self.phase = "plan"
                    elif self.phase == "plan":
                        require_plan_result(body)
                        require(prompts[0].get("actor") is not None, "plan_tool_actor_missing")
                        pre = [r for r in receipts if r["kind"] == "PreToolUse" and r["session_id"] == self.session and r["turn_id"] == self.turn and r["tool_id"] == "tempo-plan" and r.get("actor") == prompts[0].get("actor")
                               and r["ordering"] == "supported" and accepted(r)]
                        post = [r for r in receipts if r["kind"] == "PostToolUse" and r["session_id"] == self.session and r["turn_id"] == self.turn and r["tool_id"] == "tempo-plan" and r.get("actor") == prompts[0].get("actor")
                               and r["ordering"] == "supported" and accepted(r)]
                        require(pre and post, "plan_tool_receipts_missing")
                        item = function_item(body, ("spawn_agent",), "tempo-spawn", {"spawn_agent": {"message": CHILD_PROMPT}})
                        self.phase = "spawn"
                    elif self.phase == "spawn":
                        results = [v for v in body.get("input", []) if v.get("type") == "function_call_output" and v.get("call_id") == "tempo-spawn"]
                        require(len(results) == 1, "spawn_result_missing")
                        output = results[0].get("output")
                        if isinstance(output, str):
                            try: output = json.loads(output)
                            except ValueError: raise FixtureFailure("spawn_result_contract")
                        require(isinstance(output, dict) and isinstance(output.get("agent_id"), str), "spawn_result_identity")
                        if self.child is not None: require(self.child == output["agent_id"], "child_identity_mismatch")
                        self.child = output["agent_id"]
                        item = message_item("tempo-parent-complete")
                        self.phase = "parent-complete"
                    else: raise FixtureFailure("extra_parent_request")
                    hold = None
                else:
                    require(self.phase == "parent-complete" and len(prompts) == 2, "interrupt_prompt_identity")
                    turns = {r["turn_id"] for r in prompts} - {self.turn}
                    require(len(turns) == 1, "interrupt_turn_ambiguous")
                    self.interrupt_turn = turns.pop()
                    require_prompt_barrier(receipts, self.session, self.interrupt_turn)
                    require(self.counts.get("interrupt", 0) == 0, "duplicate_interrupt_request")
                    item, hold = None, "interrupt"
            self.counts[category] = self.counts.get(category, 0) + 1
            self.requests.append({"case": category, "index": len(self.requests) + 1, "receipt_count": len(receipts)})
            return item, category + str(self.counts[category]), hold


def download_runtime(root, pin):
    archive = root / "codex.tar.gz"
    # The public download is done by the harness, never by Codex. No credential
    # headers, netrc, proxy settings, or environment credentials are supplied.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    total = 0
    with opener.open(pin["url"], timeout=30) as source, archive.open("xb") as target:
        while True:
            data = source.read(1024 * 1024)
            if not data: break
            total += len(data)
            require(total <= pin["size"], "runtime_download_bound")
            target.write(data)
    require(total == pin["size"] and digest(archive) == pin["sha256"], "runtime_integrity")
    dest = root / "runtime"
    dest.mkdir(mode=0o700)
    with tarfile.open(archive, "r:gz") as bundle:
        for item in bundle.getmembers():
            p = Path(item.name)
            require(not p.is_absolute() and ".." not in p.parts and (item.isfile() or item.isdir()), "runtime_archive_entry")
            target = dest / p
            if item.isdir(): target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(item) as source, target.open("xb") as output:
                    shutil.copyfileobj(source, output)
                target.chmod(0o755 if item.mode & 0o111 else 0o644)
    metadata = json.loads((dest / "codex-package.json").read_text())
    require(metadata.get("layoutVersion") == 1 and metadata.get("version") == pin["version"]
            and metadata.get("target") == pin["platform"] and metadata.get("entrypoint") == "bin/codex"
            and metadata.get("resourcesDir") == "codex-resources" and metadata.get("pathDir") == "codex-path", "runtime_package_layout")
    runtime = dest / "bin/codex"
    require(runtime.is_file() and (dest / "codex-resources/bwrap").is_file(), "runtime_package_layout")
    return runtime


def config_text(port):
    return f'''model = "{MODEL}"
model_provider = "tempo_ci"
check_for_update_on_startup = false
cli_auth_credentials_store = "ephemeral"
web_search = "disabled"
sandbox_mode = "read-only"
approval_policy = "on-request"
[tools.update_plan]
enabled = true
[analytics]
enabled = false
[otel]
exporter = "none"
trace_exporter = "none"
metrics_exporter = "none"
log_user_prompt = false
[agents]
enabled = true
max_concurrent_threads_per_session = 1
max_depth = 1
[features]
multi_agent = true
multi_agent_v2 = false
code_mode = false
code_mode_only = false
apps = false
plugins = false
remote_models = false
api_key_model_discovery = false
shell_snapshot = false
enable_request_compression = false
[model_providers.tempo_ci]
name = "Tempo CI synthetic provider"
base_url = "http://127.0.0.1:{port}/codex/v1"
wire_api = "responses"
env_key = "TEMPO_CI_PROVIDER_TOKEN"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0
'''


def workspace_trust_probe(screen, repo):
    lines = [line.strip() for line in screen.splitlines()]
    return {"title_present": "Trust this folder?" in screen,
            "exact_path_line_present": str(repo) in lines,
            "path_substring_present": str(repo) in screen,
            "trust_choice_present": "Trust and continue" in screen,
            "quit_choice_present": "Quit" in screen,
            "alternate_root_warning_present": "Trusting will apply to the repository root:" in screen,
            "nonempty_rows": min(80, sum(bool(line) for line in lines))}


def startup_trust_probe(screen):
    lines = [re.sub(r"^›\s*", "", line.strip()) for line in screen.splitlines()]
    return {"workspace_title_present": "Trust this folder?" in screen,
            "model_label_present": MODEL in screen,
            "hooks_review_title_present": "Hooks need review" in screen,
            "exact_count_present": str(len(INSTALLED_EVENTS)) + " hooks are new or changed." in lines,
            "review_choice_present": "1. Review hooks" in lines,
            "trust_all_choice_present": "2. Trust all and continue" in lines,
            "continue_choice_present": "3. Continue without trusting (hooks won't run)" in lines}


def review_transition_probe(screen):
    startup = startup_trust_probe(screen)
    selected = [line.strip() for line in screen.splitlines() if line.strip().startswith("›")]
    def browser_row(event):
        return len(selected) == 1 and "Issues" not in screen and bool(re.search(
            r"›\s+" + event + r"\s+1\s+0\s+1\s", screen))
    return {"complete_startup_modal": not startup["workspace_title_present"] and all(startup[key] for key in
                ("hooks_review_title_present", "exact_count_present", "review_choice_present", "trust_all_choice_present", "continue_choice_present")),
            "selected_review_row": len(selected) == 1 and bool(re.fullmatch(r"›\s+1\. Review hooks", selected[0])),
            "selected_continue_row": len(selected) == 1 and bool(re.fullmatch(r"›\s+3\. Continue without trusting \(hooks won't run\)", selected[0])),
            "inventory_caption_present": "Lifecycle hooks from config and enabled plugins." in screen,
            "inventory_first_row": browser_row("PreToolUse"),
            "inventory_last_row": browser_row("Interrupt"),
            "model_label_present": startup["model_label_present"],
            "workspace_title_present": startup["workspace_title_present"]}


def navigation_ack(terminal, screen, probe, origin, acknowledged, category, key=b"\x1b[F", project=review_transition_probe):
    require(key in (b"\x1b[F", b"\x1b[B", b"\x1b[A"), "invalid_navigation_key")
    probe.clear()
    probe.update(project(screen))
    def observe(value):
        probe.clear()
        probe.update(project(value))
        return acknowledged(probe)
    # Only idempotent navigation is retried. Each resend requires the same
    # complete original screen; confirmation/shortcut actions are never retried.
    for _ in range(2):
        require(origin(probe), category)
        terminal.send(key)
        try:
            return terminal.until(observe, category, seconds=2)
        except FixtureFailure as exc:
            if str(exc) != category:
                raise
    raise FixtureFailure(category)


def review_input_ack(terminal, screen, probe):
    navigation_ack(terminal, screen, probe,
                       lambda p: p["complete_startup_modal"] and p["selected_review_row"],
                       lambda p: p["complete_startup_modal"] and p["selected_continue_row"],
                       "startup_review_input_unavailable")
    terminal.send(b"1")


def browser_input_ack(terminal, screen, probe):
    navigation_ack(terminal, screen, probe,
                       lambda p: p["inventory_caption_present"] and p["inventory_first_row"],
                       lambda p: p["inventory_caption_present"] and p["inventory_last_row"],
                       "browser_input_unavailable")


def workspace_input_probe(screen, repo):
    probe = workspace_trust_probe(screen, repo)
    selected = [line.strip() for line in screen.splitlines() if line.strip().startswith("›")]
    probe["selected_trust_row"] = len(selected) == 1 and bool(re.fullmatch(r"›\s+1\. Trust and continue", selected[0]))
    probe["selected_quit_row"] = len(selected) == 1 and bool(re.fullmatch(r"›\s+2\. Quit", selected[0]))
    return probe


def workspace_input_ack(terminal, screen, repo, probe):
    def complete(p):
        return all(p[key] for key in ("title_present", "exact_path_line_present", "trust_choice_present", "quit_choice_present")) and not p["alternate_root_warning_present"]
    project = lambda value: workspace_input_probe(value, repo)
    quit_screen = navigation_ack(terminal, screen, probe,
        lambda p: complete(p) and p["selected_trust_row"], lambda p: complete(p) and p["selected_quit_row"],
        "workspace_input_down_unavailable", b"\x1b[B", project)
    navigation_ack(terminal, quit_screen, probe,
        lambda p: complete(p) and p["selected_quit_row"], lambda p: complete(p) and p["selected_trust_row"],
        "workspace_input_up_unavailable", b"\x1b[A", project)
    terminal.send(b"\r")


def normal_trust(terminal, repo, command, trusted_events, probe=None, startup_probe=None, input_probes=None):
    if probe is None:
        probe = {}
    probe.clear()
    probe.update(workspace_trust_probe("", repo))
    if startup_probe is None:
        startup_probe = {}
    startup_probe.clear()
    startup_probe.update(startup_trust_probe(""))
    if input_probes is None:
        input_probes = {}
    input_probes.clear()
    input_probes.update({"workspace": workspace_input_probe("", repo), "startup": review_transition_probe(""), "browser": review_transition_probe("")})

    def startup_ready(screen):
        startup_probe.clear()
        startup_probe.update(startup_trust_probe(screen))
        input_probes["startup"].clear()
        input_probes["startup"].update(review_transition_probe(screen))
        if startup_probe["workspace_title_present"]:
            return False
        if startup_probe["hooks_review_title_present"]:
            return all(startup_probe[key] for key in ("exact_count_present", "review_choice_present",
                       "trust_all_choice_present", "continue_choice_present")) and input_probes["startup"]["selected_review_row"]
        return startup_probe["model_label_present"]

    def observe_until(predicate, category):
        def observed(screen):
            # Replace each observation; never union stale evidence or retain text.
            probe.clear()
            probe.update(workspace_trust_probe(screen, repo))
            input_probes["workspace"].clear()
            input_probes["workspace"].update(workspace_input_probe(screen, repo))
            return predicate(screen)
        return terminal.until(observed, category)

    initial = observe_until(lambda s: "Trust this folder?" in s or MODEL in s or "Hooks need review" in s, "startup_ui_unavailable")
    startup_screen = initial
    if "Trust this folder?" in initial:
        # A PTY read can end halfway through a redraw. Wait for the complete
        # exact folder/choice display before taking the normal trust action.
        workspace_screen = observe_until(lambda s: "Trust this folder?" in s
                       and str(repo) in [line.strip() for line in s.splitlines()]
                       and "Trust and continue" in s and "Quit" in s
                       and "Trusting will apply to the repository root:" not in s
                       and workspace_input_probe(s, repo)["selected_trust_row"],
                       "workspace_trust_mismatch")
        workspace_input_ack(terminal, workspace_screen, repo, input_probes["workspace"])
        startup_screen = terminal.until(startup_ready, "workspace_trust_failed")
    elif not startup_ready(initial):
        startup_screen = terminal.until(startup_ready, "startup_hooks_review_unavailable")
    if startup_probe["hooks_review_title_present"]:
        # Pinned normal UI shortcut 1 opens Review hooks; it grants no trust.
        review_input_ack(terminal, startup_screen, input_probes["startup"])
    else:
        terminal.command("/hooks")
    def browser_ready(screen):
        input_probes["browser"].clear()
        input_probes["browser"].update(review_transition_probe(screen))
        return input_probes["browser"]["inventory_caption_present"] and input_probes["browser"]["inventory_first_row"]
    browser_screen = terminal.until(browser_ready, "hooks_ui_unavailable")
    browser_input_ack(terminal, browser_screen, input_probes["browser"])
    # Home selects first pinned event. Inspect inventory counts including all
    # zero-handler events before any trust action.
    terminal.send(b"\x1b[H")
    for event in EVENT_ORDER:
        expected = 1 if event in INSTALLED_EVENTS else 0
        screen = terminal.until(lambda s: "Issues" not in s and re.search(
            r"›\s+" + event + r"\s+" + str(expected) + r"\s+0\s+" + str(expected) + r"\s", s),
            "hook_inventory_navigation")
        require("Issues" not in screen, "hook_inventory_issues")
        require(re.search(r"›\s+" + event + r"\s+" + str(expected) + r"\s+0\s+" + str(expected) + r"\s", screen), "hook_inventory_mismatch")
        terminal.send(b"\x1b[B")
    terminal.send(b"\x1b[H")
    for event in EVENT_ORDER:
        terminal.until(lambda s: re.search(r"›\s+" + event + r"\s", s), "hook_inventory_navigation")
        if event in INSTALLED_EVENTS:
            terminal.send(b"\r")
            def details_ready(screen):
                try:
                    review_hook_screen(screen, event, command, "~/.codex/hooks.json")
                except FixtureFailure:
                    return False
                return True
            screen = terminal.until(details_ready, "hook_details_unavailable")
            review_hook_screen(screen, event, command, "~/.codex/hooks.json")
            terminal.send(b"t")
            terminal.until(lambda s: event + " hooks" in s and re.search(r"Trust\s+Trusted", s) and "[x] Hook 1" in s, "hook_trust_failed")
            trusted_events.append(event)
            terminal.send(b"\x1b")
            terminal.until(lambda s: "Lifecycle hooks from config and enabled plugins." in s, "hook_inventory_unavailable")
        terminal.send(b"\x1b[B")
    terminal.send(b"\x1b")
    terminal.until(lambda s: "Lifecycle hooks from config and enabled plugins." not in s, "hook_browser_not_closed")
    terminal.command("/quit")
    terminal.until(lambda _: terminal.proc.poll() is not None, "trust_host_exit", seconds=10)
    require(terminal.proc.returncode == 0, "trust_host_exit_failed")


def start_measured_turn(terminal, model, baseline, wait_snapshot):
    with model.lock:
        require(model.baseline is None and model.session is None and model.phase == "initial", "measured_session_already_armed")
        model.baseline = frozenset(baseline)
    # The pinned host drains pending SessionStart only on the first actual turn.
    # The provider independently gates every response on genuine hook receipts.
    terminal.command(PARENT_PROMPT)
    snapshot = wait_snapshot(lambda s: any(r["kind"] == "SessionStart" and r["id"] not in baseline and accepted(r)
                             for r in s["receipts"]), "posttrust_session_start_missing")
    starts = [r for r in snapshot["receipts"] if r["kind"] == "SessionStart" and r["id"] not in baseline and accepted(r)]
    require(len(starts) == 1, "measured_session_ambiguous")
    with model.lock:
        require(model.session is None or model.session == starts[0]["session_id"], "measured_session_identity_mismatch")


def actor_state(snapshot, receipt):
    matches = [a for a in snapshot["actors"] if a["ref"] == receipt.get("actor")]
    require(len(matches) == 1, "actor_projection_missing")
    return matches[0]["state"]


def codex_argv(runtime):
    # Public embedded mode keeps both launches in the owned process group;
    # normal workspace and hook trust remain required by the host.
    return [str(runtime), "--no-alt-screen", "--no-daemon"]


def run(args, report):
    started = time.monotonic()
    deadline = started + 240
    report["stage"] = "hosted_preconditions"
    hosted_precondition(os.environ, platform.system(), platform.machine(), pwd.getpwuid(os.getuid()).pw_dir)
    home = Path(os.environ["HOME"])
    require_absent([home / ".codex", home / ".agents", home / ".config/codex", Path("/etc/codex")])
    pin = json.loads(Path(__file__).with_name("codex-0.159.3.json").read_text())
    root = Path(tempfile.mkdtemp(prefix="tempo-native-", dir=os.environ["RUNNER_TEMP"]))
    (root / "tmp").mkdir(mode=0o700)
    repo = root / "project"
    repo.mkdir(mode=0o700)
    env = child_environment(os.environ, root)
    report["stage"] = "pinned_runtime_download"
    runtime = download_runtime(root, pin)
    tempo = Path(args.tempo).resolve(strict=True)
    helper = Path(args.helper).resolve(strict=True)
    require(tempo.is_file() and helper.is_file(), "built_binaries_required")
    # Copy to a short controlled path so the exact command is never TUI-truncated.
    shutil.copy2(tempo, root / "tempo")
    tempo = root / "tempo"
    bounded_run(["/usr/bin/git", "-c", "credential.helper=", "init", "-q", str(repo)], env, root)
    (repo / "AGENTS.md").write_text("Synthetic native lifecycle fixture. Update the native plan and run the requested child lifecycle. No shell, network or file writes.\n")

    def helper_call(action, *extra, timeout_cap=None):
        timeout = 20 if action in ("install", "status", "confirm") else 8
        if timeout_cap is not None:
            timeout = min(timeout, timeout_cap)
        output = bounded_run([str(helper), "fixture", action, env["TEMPO_STATE"], env["TEMPO_HOOK_STATE"], *map(str, extra)],
                             env, repo, timeout=timeout)
        return json.loads(output) if output else None

    report["stage"] = "production_link"
    helper_call("link", repo)
    model = Model(lambda: helper_call("read"), repo, deadline)
    terminal = None
    baseline = None
    confirmed_artifacts = None
    profile = None
    report["hook_diagnostics"] = {"status": "unavailable", "counts": []}
    report["diagnostic_source"] = "host_managed_stderr"
    try:
        hostdir = home / ".codex"
        hostdir.mkdir(mode=0o700)
        config = hostdir / "config.toml"
        definitions = hostdir / "hooks.json"
        config.write_text(config_text(model.server.server_port))
        config.chmod(0o600)
        report["stage"] = "production_install"
        installed = helper_call("install", repo, runtime, tempo, "user")
        require_installed_profile(installed, "codex", "user", repo, pin["version"], False)
        command = require_installed_definitions(definitions, tempo, "codex", INSTALLED_EVENTS)
        report.update({"runtime_version": pin["version"], "archive_sha256": pin["sha256"], "runtime_sha256": digest(runtime),
                       "tempo_sha256": digest(tempo), "definitions_sha256": digest(definitions),
                       "configuration_origin": "production_installer", "provider_configuration_origin": "fixture_authored",
                       "installed_events": list(INSTALLED_EVENTS), "delivery_origin": "actual_codex_process",
                       "profile_basis": "operator_declared", "product_receipt_origin": "unverified"})
        argv = codex_argv(runtime)
        report["stage"] = "normal_trust_ui"
        report["trusted_events"] = []
        terminal = Terminal(argv, env, repo, deadline, report.setdefault("command_input_probe", {}))
        normal_trust(terminal, repo, command, report["trusted_events"], report.setdefault("workspace_trust_probe", {}),
                     report.setdefault("startup_trust_probe", {}), report.setdefault("input_probes", {}))
        terminal.close(); terminal = None
        require(not model.requests and model.error is None, "unexpected_pretrust_inference")
        report["stage"] = "production_policy_confirmation"
        require_installed_profile(helper_call("status", repo, runtime, tempo, "user"), "codex", "user", repo, pin["version"], False)
        profile = require_installed_profile(helper_call("confirm", repo, runtime, tempo, "user"), "codex", "user", repo, pin["version"], True)
        report["profile_fingerprint"] = profile["fingerprint"]
        report["configuration_sha256"] = digest(config)
        baseline = {r["id"] for r in helper_call("read")["receipts"]}
        confirmed_artifacts = {role: (path, digest(path)) for role, path in
                               (("runtime", runtime), ("executable", tempo), ("definitions", definitions), ("configuration", config))}
        report["stage"] = "measured_restart"
        terminal = Terminal(argv, env, repo, deadline, report.setdefault("command_input_probe", {}))

        def wait_snapshot(predicate, category, seconds=20):
            result = None
            def check(_):
                nonlocal result
                require(model.error is None, model.error or "provider_failed")
                result = helper_call("read")
                return predicate(result)
            terminal.until(check, category, seconds)
            return result

        start_measured_turn(terminal, model, baseline, wait_snapshot)
        report["stage"] = "parent_plan_and_child"
        snapshot = wait_snapshot(lambda s: model.child_seen.is_set() and any(r["kind"] == "Stop" and r["session_id"] == model.session and accepted(r) for r in s["receipts"]), "parent_stop_missing", 40)
        require(model.child_seen.is_set() and model.child, "child_request_missing")
        parent_stop = next(r for r in snapshot["receipts"] if r["kind"] == "Stop" and r["session_id"] == model.session and accepted(r))
        child_start = next(r for r in snapshot["receipts"] if r["kind"] == "SubagentStart" and r["agent_id"] == model.child and accepted(r))
        require(actor_state(snapshot, parent_stop) == "wait_user" and actor_state(snapshot, child_start) == "working", "parent_child_independence")
        require(parent_stop["actor"] != child_start["actor"], "parent_child_actor_collision")
        model.child_release.set()
        report["stage"] = "child_completion"
        completed = wait_snapshot(lambda s: any(r["kind"] == "SubagentStop" and r["session_id"] == model.session and r["agent_id"] == model.child
                                   and r["turn_id"] == model.child_turn and accepted(r) for r in s["receipts"]), "child_stop_missing")
        completed_queued = require_completed_capture(completed)
        report["completed_queued_count"] = completed_queued
        report["completed_capture_review_count"] = completed["capture_reviews"]
        terminal.command(INTERRUPT_PROMPT)
        report["stage"] = "ordinary_interrupt"
        terminal.until(lambda _: model.interrupt_seen.is_set(), "interrupt_request_missing")
        terminal.send(b"\x1b")
        interrupted = wait_snapshot(lambda s: any(r["kind"] == "Interrupt" and r["session_id"] == model.session and r["turn_id"] == model.interrupt_turn and accepted(r) for r in s["receipts"]), "interrupt_hook_missing")
        interrupt_receipt = next(r for r in interrupted["receipts"] if r["kind"] == "Interrupt" and r["session_id"] == model.session
                                 and r["turn_id"] == model.interrupt_turn and accepted(r))
        terminal.command("/quit")
        report["stage"] = "normal_session_end"
        terminal.until(lambda _: terminal.proc.poll() is not None, "normal_exit_missing", seconds=10)
        require(terminal.proc.returncode == 0, "normal_exit_failed")
        snapshot = helper_call("read")
        measured = [r for r in snapshot["receipts"] if r["id"] not in baseline]
        # After an applied Interrupt the adapter owner specifies a committed
        # stale SessionEnd: same root actor, no second terminal state effect.
        ends = [r for r in measured if r["kind"] == "SessionEnd" and r["session_id"] == model.session]
        require(len(ends) == 1 and ends[0]["durability"] == "committed" and ends[0]["disposition"] == "stale"
                and ends[0]["actor"] == interrupt_receipt["actor"], "session_end_missing")
        require(actor_state(snapshot, interrupt_receipt) == "interrupted", "interrupt_actor_effect_missing")
        require(model.counts == {"parent": 3, "child": 1, "interrupt": 1}, "provider_request_counts")
        require_capture_effects(snapshot, interrupt_receipt, completed_queued)
        require(all(any(r["kind"] == event and accepted(r) for r in measured) for event in EVENTS if event != "SessionEnd"), "required_native_event_missing")
        require(model.error is None, model.error or "provider_failed")
        current_profile = require_installed_profile(helper_call("status", repo, runtime, tempo, "user"), "codex", "user", repo, pin["version"], True)
        report["installed_artifact_matches"] = require_unchanged_profile(profile, current_profile)
        report.update({"status": "passed", "receipts": [project_receipt(r) for r in measured], "requests": model.requests,
                       "request_counts": model.counts, "queued_count": snapshot["queued"], "uncertainty_count": snapshot["uncertainties"],
                       "capture_review_count": snapshot["capture_reviews"],
                       "assertions": ["production_install", "full_installed_inventory", "all_profile_artifacts_unchanged", "normal_exact_definition_trust", "posttrust_session_restart", "prompt_before_provider",
                                      "actual_plan_result", "actual_child_request", "parent_stop_child_still_working",
                                      "child_stop", "completed_work_queued_before_interrupt", "interrupt", "normal_session_end",
                                      "exact_bounded_interrupt_uncertainty", "production_capture_effects"]})
        report["stage"] = "complete"
    finally:
        primary_failure = sys.exc_info()[0] is not None
        if primary_failure:
            report["terminal_failure_probe"] = terminal_failure_probe(terminal)
        joined = close_native(terminal, model, primary_failure=primary_failure)
        report["cpu_capability_probe"] = observe_cpu_flags(joined)
        report["initial_receipt_probe"] = model.initial_receipt_probe
        report["capture_context_probe"] = {
            "status": "observed" if joined and model.capture_contexts else "not_observed" if joined else "unavailable",
            "source": "model_developer_input", "completeness": "not_established",
            "invocation_count": "unavailable",
            "tuples": [{"kind": kind, "code": code, "durability": durability}
                       for kind, code, durability in sorted(model.capture_contexts)] if joined else [],
        }
        report["capture_admission_probe"] = {
            "source": "opt_in_model_developer_input", "completeness": "not_established",
            "invocation_count": "unavailable", "saturated": getattr(model, "admission_diagnostics_saturated", False),
            "records": getattr(model, "admission_diagnostics", []) if joined else [],
        }
        if primary_failure:
            report["failure_profile_probe"] = observe_failed_profile(
                lambda timeout: helper_call("status", repo, runtime, tempo, "user", timeout_cap=timeout),
                profile, repo, pin["version"], deadline, joined)
        report["request_counts"] = model.counts
        report["requests"] = model.requests
        report["provider_entry_count"] = model.entry_count
        report["auxiliary_title_count"] = model.title_count
        if confirmed_artifacts is not None:
            report["artifact_matches"] = artifact_match_probe(confirmed_artifacts)
        if baseline is not None and "receipts" not in report:
            try:
                report["receipts"] = [project_receipt(r) for r in helper_call("read")["receipts"] if r["id"] not in baseline]
            except Exception:
                report["receipt_observation_status"] = "unavailable"
        # Never archive the generated host home, transcripts, requests or stores.
        # Runner teardown disposes of them; unexpected preexisting state is never removed.
        report["elapsed_ms"] = int((time.monotonic() - started) * 1000)


def close_native(terminal, model, primary_failure):
    joined = False
    try:
        try:
            if terminal is not None: terminal.close()
        finally:
            model.close()
        joined = True
        # Joined handlers cannot change the verdict after this check.
        require(model.error is None, model.error or "provider_failed")
    except Exception:
        if not primary_failure:
            raise
    return joined


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tempo", required=True)
    parser.add_argument("--helper", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    report = {"schema_version": 1, "status": "failed", "host": "codex", "coverage": "finite_hosted_linux_cases"}
    # Hard wall-clock ceiling also bounds download/setup, not just host waits.
    def alarm(*_): raise FixtureFailure("overall_deadline")
    def cancelled(*_): raise FixtureFailure("fixture_cancelled")
    old_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}
    signal.signal(signal.SIGALRM, alarm)
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    signal.alarm(250)
    try:
        run(args, report)
    except Exception as exc:
        report["status"] = "failed"
        report["failure_category"] = str(exc) if isinstance(exc, FixtureFailure) else "harness_failed"
    finally:
        signal.alarm(0)
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        Path(args.evidence).write_text(json.dumps(report, indent=2) + "\n")
    print("native_codex_smoke: " + report["status"])
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
