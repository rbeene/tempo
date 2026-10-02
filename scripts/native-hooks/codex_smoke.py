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
READ_RESULT = "tempo-fixture-read-ok"
TOKEN = "tempo-ci-invalid-synthetic-token"


class FixtureFailure(Exception):
    """Contains only a fixed safe category, never an underlying exception."""


def require(value, code):
    if not value:
        raise FixtureFailure(code)


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
            "TEMPO_CI_PROVIDER_TOKEN": TOKEN}


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


def require_prompt_barrier(receipts, session, turn):
    require(any(r["kind"] == "SessionStart" and r["session_id"] == session and accepted(r) for r in receipts), "session_start_barrier_missing")
    require(any(r["kind"] == "UserPromptSubmit" and r["session_id"] == session and r["turn_id"] == turn and accepted(r)
                and r["ordering"] == "supported" for r in receipts), "prompt_barrier_missing")


def require_read_result(body):
    matches = [v for v in body.get("input", []) if v.get("type") == "function_call_output" and v.get("call_id") == "tempo-read"]
    require(len(matches) == 1 and READ_RESULT in json.dumps(matches[0].get("output")), "actual_read_result_missing")


def read_result_probe(body):
    matches = [v for v in body.get("input", []) if v.get("type") == "function_call_output" and v.get("call_id") == "tempo-read"]
    result = {"matching_results": min(2, len(matches)), "output_type": "missing", "sentinel_present": False,
              "error_category": "unavailable", "process_state": "unknown",
              "namespace_failure_marker": False, "companion_failure_marker": False}
    if len(matches) != 1: return result
    value = matches[0].get("output")
    result["output_type"] = "string" if isinstance(value, str) else "list" if isinstance(value, list) else "object" if isinstance(value, dict) else "other"
    if not isinstance(value, str): return result
    if len(value.encode()) > 16384:
        result["output_type"] = "oversize"
        return result
    result["sentinel_present"] = READ_RESULT in value
    variants = {"CreateProcess": "create_process", "ProcessFailed": "process_failed", "UnknownProcessId": "unknown_process_id",
                "StdinApproval": "stdin_approval", "WriteToStdin": "write_to_stdin", "StdinClosed": "stdin_closed",
                "MissingCommandLine": "missing_command_line", "SandboxDenied": "sandbox_denied", "ForeignPath": "foreign_path"}
    match = re.match(r"^exec_command failed: (" + "|".join(variants) + r")(?= \{|\(|$)", value)
    if match: result["error_category"] = variants[match[1]]
    elif value == "unified exec is unavailable in this session": result["error_category"] = "exec_unavailable"
    lines = value.splitlines()
    if "Output:" in lines:
        header = lines[:lines.index("Output:")]
        states = ["exited_zero" if line == "Process exited with code 0" else "exited_nonzero"
                  for line in header if re.fullmatch(r"Process exited with code -?[0-9]{1,10}", line)]
        states += ["running" for line in header if re.fullmatch(r"Process running with session ID [0-9]{1,10}", line)]
        if len(states) == 1: result["process_state"] = states[0]
    # Presence only: these selected pinned literals do not prove the cause.
    result["namespace_failure_marker"] = any(marker in value for marker in ("loopback: Failed RTM_NEWADDR",
        "loopback: Failed RTM_NEWLINK", "setting up uid map: Permission denied", "No permissions to create a new namespace"))
    result["companion_failure_marker"] = any(marker in value for marker in ("failed to open bundled bubblewrap ",
        "failed to exec bundled bubblewrap ", "failed to normalize bundled bubblewrap ", "failed to read bundled bubblewrap "))
    return result


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


class Terminal:
    def __init__(self, argv, env, cwd, deadline, input_probe=None):
        self.deadline = deadline
        self.input_probe = {} if input_probe is None else input_probe
        self.screen = Screen()
        self.master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", self.screen.rows, self.screen.cols, 0, 0))
        try:
            self.proc = subprocess.Popen(argv, env=env, cwd=cwd, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        finally:
            os.close(slave)
        self.query_tail = b""

    def send(self, data):
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


class Model:
    def __init__(self, read, repo, deadline):
        self.read, self.repo, self.deadline = read, repo, deadline
        self.session = self.turn = self.child = self.interrupt_turn = None
        self.baseline = None
        self.child_turn = None
        self.lock = threading.Lock()
        self.error = None
        self.counts = {}
        self.entry_count = 0
        self.title_count = 0
        self.read_probe = read_result_probe({})
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

    def respond(self, body):
        with self.lock:
            self.entry_count = min(16, self.entry_count + 1)
        require(body.get("stream") is True and body.get("model") == MODEL, "provider_request_contract")
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
            snap = self.read()
            receipts = snap["receipts"]
            if self.session is None:
                require(self.baseline is not None and category == "parent" and self.phase == "initial", "request_before_measured_session")
                starts = [r for r in receipts if r["kind"] == "SessionStart" and r["id"] not in self.baseline and accepted(r)]
                require(len(starts) == 1, "measured_session_ambiguous")
                session = starts[0]["session_id"]
                prompts = [r for r in receipts if r["kind"] == "UserPromptSubmit" and r["id"] not in self.baseline
                           and r["session_id"] == session and accepted(r)]
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
                prompts = [r for r in receipts if r["kind"] == "UserPromptSubmit" and r["session_id"] == self.session and accepted(r)]
                if category == "parent":
                    require(len(prompts) == 1, "parent_prompt_identity")
                    self.turn = prompts[0]["turn_id"]
                    require_prompt_barrier(receipts, self.session, self.turn)
                    if self.phase == "initial":
                        item = function_item(body, ("exec_command", "shell_command"), "tempo-read", {
                            "exec_command": {"cmd": "cat fixture.txt", "workdir": str(self.repo), "max_output_tokens": 1000},
                            "shell_command": {"command": "cat fixture.txt", "workdir": str(self.repo), "timeout_ms": 1000}})
                        self.phase = "read"
                    elif self.phase == "read":
                        self.read_probe = read_result_probe(body)
                        require_read_result(body)
                        pre = [r for r in receipts if r["kind"] == "PreToolUse" and r["session_id"] == self.session and r["turn_id"] == self.turn and r["tool_id"] == "tempo-read" and accepted(r)]
                        post = [r for r in receipts if r["kind"] == "PostToolUse" and r["session_id"] == self.session and r["turn_id"] == self.turn and r["tool_id"] == "tempo-read" and accepted(r)]
                        require(pre and post, "read_tool_receipts_missing")
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
[tui]
show_tooltips = false
[analytics]
enabled = false
[otel]
exporter = "none"
trace_exporter = "none"
metrics_exporter = "none"
log_user_prompt = false
[agents]
enabled = true
default_subagent_model = "{MODEL}"
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
            "exact_count_present": str(len(EVENTS)) + " hooks are new or changed." in lines,
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
        expected = 1 if event in EVENTS else 0
        screen = terminal.until(lambda s: "Issues" not in s and re.search(
            r"›\s+" + event + r"\s+" + str(expected) + r"\s+0\s+" + str(expected) + r"\s", s),
            "hook_inventory_navigation")
        require("Issues" not in screen, "hook_inventory_issues")
        require(re.search(r"›\s+" + event + r"\s+" + str(expected) + r"\s+0\s+" + str(expected) + r"\s", screen), "hook_inventory_mismatch")
        terminal.send(b"\x1b[B")
    terminal.send(b"\x1b[H")
    for event in EVENT_ORDER:
        terminal.until(lambda s: re.search(r"›\s+" + event + r"\s", s), "hook_inventory_navigation")
        if event in EVENTS:
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
    diagnostics = root / "hook-errors"
    command = prepare_diagnostic_command(tempo, diagnostics)
    diagnostic_identity = diagnostics.stat()
    bounded_run(["/usr/bin/git", "-c", "credential.helper=", "init", "-q", str(repo)], env, root)
    (repo / "fixture.txt").write_text(READ_RESULT + "\n")
    (repo / "AGENTS.md").write_text("Synthetic native lifecycle fixture. Read only fixture.txt. No network or writes.\n")

    def helper_call(action, *extra):
        output = bounded_run([str(helper), "fixture", action, env["TEMPO_STATE"], env["TEMPO_HOOK_STATE"], *map(str, extra)], env, repo)
        return json.loads(output) if output else None

    report["stage"] = "production_link"
    helper_call("link", repo)
    model = Model(lambda: helper_call("read"), repo, deadline)
    terminal = None
    baseline = None
    confirmed_artifacts = None
    measured_diagnostics = False
    try:
        hostdir = home / ".codex"
        hostdir.mkdir(mode=0o700)
        config = hostdir / "config.toml"
        definitions = hostdir / "hooks.json"
        config.write_text(config_text(model.server.server_port))
        definitions.write_text(json.dumps({"hooks": {event: [{"hooks": [{"type": "command", "command": command, "timeout": 2}]}] for event in EVENTS}}, indent=2) + "\n")
        config.chmod(0o600); definitions.chmod(0o600)
        report.update({"runtime_version": pin["version"], "archive_sha256": pin["sha256"], "runtime_sha256": digest(runtime),
                       "tempo_sha256": digest(tempo), "definitions_sha256": digest(definitions),
                       "configuration_origin": "fixture_authored", "delivery_origin": "actual_codex_process",
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
        profile = helper_call("confirm", repo, runtime, tempo, definitions, config)
        report["profile_fingerprint"] = profile["fingerprint"]
        report["configuration_sha256"] = digest(config)
        baseline = {r["id"] for r in helper_call("read")["receipts"]}
        confirmed_artifacts = {role: (path, digest(path)) for role, path in
                               (("runtime", runtime), ("executable", tempo), ("definitions", definitions), ("configuration", config))}
        reset_hook_diagnostics(diagnostics, diagnostic_identity)
        measured_diagnostics = True
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
        report["stage"] = "parent_read_and_child"
        snapshot = wait_snapshot(lambda s: model.child_seen.is_set() and any(r["kind"] == "Stop" and r["session_id"] == model.session and accepted(r) for r in s["receipts"]), "parent_stop_missing", 40)
        require(model.child_seen.is_set() and model.child, "child_request_missing")
        parent_stop = next(r for r in snapshot["receipts"] if r["kind"] == "Stop" and r["session_id"] == model.session and accepted(r))
        child_start = next(r for r in snapshot["receipts"] if r["kind"] == "SubagentStart" and r["agent_id"] == model.child and accepted(r))
        require(actor_state(snapshot, parent_stop) == "wait_user" and actor_state(snapshot, child_start) == "working", "parent_child_independence")
        require(parent_stop["actor"] != child_start["actor"], "parent_child_actor_collision")
        model.child_release.set()
        report["stage"] = "child_completion"
        wait_snapshot(lambda s: any(r["kind"] == "SubagentStop" and r["session_id"] == model.session and r["agent_id"] == model.child
                                   and r["turn_id"] == model.child_turn and accepted(r) for r in s["receipts"]), "child_stop_missing")
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
        require(snapshot["queued"] > 0 and snapshot["uncertainties"] == 0, "native_capture_effects_missing")
        require(all(any(r["kind"] == event and accepted(r) for r in measured) for event in EVENTS if event != "SessionEnd"), "required_native_event_missing")
        require(model.error is None, model.error or "provider_failed")
        report.update({"status": "passed", "receipts": [project_receipt(r) for r in measured], "requests": model.requests,
                       "request_counts": model.counts, "queued_count": snapshot["queued"], "uncertainty_count": snapshot["uncertainties"],
                       "assertions": ["normal_exact_definition_trust", "posttrust_session_restart", "prompt_before_provider",
                                      "actual_read_result", "actual_child_request", "parent_stop_child_still_working",
                                      "child_stop", "interrupt", "normal_session_end", "production_capture_effects"]})
        report["stage"] = "complete"
    finally:
        try:
            if terminal is not None: terminal.close()
        finally:
            model.close()
        report["request_counts"] = model.counts
        report["requests"] = model.requests
        report["provider_entry_count"] = model.entry_count
        report["auxiliary_title_count"] = model.title_count
        report["read_result_probe"] = model.read_probe
        report["hook_diagnostics"] = hook_diagnostic_probe(diagnostics, diagnostic_identity) if measured_diagnostics else {"status": "unavailable", "counts": []}
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
        # All request handlers are joined by close, so no late endpoint or
        # protocol failure can arrive after the final verdict.
        require(model.error is None, model.error or "provider_failed")


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
