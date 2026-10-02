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
PARENT_PROMPT = "tempo-native-parent-case"
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
        self.saved = (0, 0)
        self.pending = ""
        self.decoder = codecs.getincrementaldecoder("utf-8")("replace")

    def text(self):
        return "\n".join("".join(row).rstrip() for row in self.grid)

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
                    self.csi(match[1], match[3])
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
                elif code not in ("=", ">", "\\"):
                    raise FixtureFailure("unsupported_terminal_control")
                i += 2
                continue
            if ch == "\r":
                self.col = 0
            elif ch == "\n":
                self.row += 1
                if self.row >= self.rows:
                    self.grid.pop(0)
                    self.grid.append([" "] * self.cols)
                    self.row = self.rows - 1
            elif ch == "\b":
                self.col = max(0, self.col - 1)
            elif ch == "\t":
                self.col = min(self.cols - 1, (self.col // 8 + 1) * 8)
            elif ord(ch) >= 32 and ch != "\x7f":
                if self.col >= self.cols:
                    self.col = 0
                    self.row = min(self.row + 1, self.rows - 1)
                self.grid[self.row][self.col] = ch
                self.col += 1
            i += 1

    def csi(self, params, op):
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
            for _ in range(min(n, self.rows)):
                self.grid.pop(0); self.grid.append([" "] * self.cols)
        elif op == "T":
            for _ in range(min(n, self.rows)):
                self.grid.pop(); self.grid.insert(0, [" "] * self.cols)
        elif op == "s": self.saved = self.row, self.col
        elif op == "u": self.row, self.col = self.saved
        else:
            raise FixtureFailure("unsupported_terminal_control")


class Terminal:
    def __init__(self, argv, env, cwd, deadline):
        self.deadline = deadline
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
        self.send(value.encode() + b"\r")

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
        self.child_turn = None
        self.lock = threading.Lock()
        self.error = None
        self.counts = {}
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
        require(body.get("stream") is True and body.get("model") == "gpt-6.1-sol", "provider_request_contract")
        # The newest actual user message distinguishes parent/child/interrupt;
        # nested tool arguments containing the child marker don't count.
        users = [item for item in body.get("input", []) if item.get("role") == "user"]
        require(bool(users), "provider_user_missing")
        text = json.dumps(users[-1].get("content"))
        if INTERRUPT_PROMPT in text: category = "interrupt"
        elif CHILD_PROMPT in text: category = "child"
        elif PARENT_PROMPT in text: category = "parent"
        else: raise FixtureFailure("unexpected_provider_turn")
        with self.lock:
            require(not self.shutdown.is_set(), "provider_request_after_shutdown")
            snap = self.read()
            receipts = snap["receipts"]
            require(self.session is not None, "request_before_measured_session")
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
    return f'''model = "gpt-6.1-sol"
model_provider = "tempo_ci"
check_for_update_on_startup = false
cli_auth_credentials_store = "ephemeral"
web_search = "disabled"
sandbox_mode = "read-only"
approval_policy = "on-request"
[analytics]
enabled = false
[otel]
exporter = "none"
trace_exporter = "none"
metrics_exporter = "none"
log_user_prompt = false
[agents]
enabled = true
default_subagent_model = "gpt-6.1-sol"
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
            "model_label_present": "gpt-6.1-sol" in screen,
            "hooks_review_title_present": "Hooks need review" in screen,
            "exact_count_present": str(len(EVENTS)) + " hooks are new or changed." in lines,
            "review_choice_present": "1. Review hooks" in lines,
            "trust_all_choice_present": "2. Trust all and continue" in lines,
            "continue_choice_present": "3. Continue without trusting (hooks won't run)" in lines}


def normal_trust(terminal, repo, command, trusted_events, probe=None, startup_probe=None):
    if probe is None:
        probe = {}
    probe.clear()
    probe.update(workspace_trust_probe("", repo))
    if startup_probe is None:
        startup_probe = {}
    startup_probe.clear()
    startup_probe.update(startup_trust_probe(""))

    def startup_ready(screen):
        startup_probe.clear()
        startup_probe.update(startup_trust_probe(screen))
        if startup_probe["workspace_title_present"]:
            return False
        if startup_probe["hooks_review_title_present"]:
            return all(startup_probe[key] for key in ("exact_count_present", "review_choice_present",
                       "trust_all_choice_present", "continue_choice_present"))
        return startup_probe["model_label_present"]

    def observe_until(predicate, category):
        def observed(screen):
            # Replace each observation; never union stale evidence or retain text.
            probe.clear()
            probe.update(workspace_trust_probe(screen, repo))
            return predicate(screen)
        return terminal.until(observed, category)

    initial = observe_until(lambda s: "Trust this folder?" in s or "gpt-6.1-sol" in s or "Hooks need review" in s, "startup_ui_unavailable")
    if "Trust this folder?" in initial:
        # A PTY read can end halfway through a redraw. Wait for the complete
        # exact folder/choice display before taking the normal trust action.
        observe_until(lambda s: "Trust this folder?" in s
                       and str(repo) in [line.strip() for line in s.splitlines()]
                       and "Trust and continue" in s and "Quit" in s
                       and "Trusting will apply to the repository root:" not in s,
                       "workspace_trust_mismatch")
        terminal.send(b"\r")
        terminal.until(startup_ready, "workspace_trust_failed")
    elif not startup_ready(initial):
        terminal.until(startup_ready, "startup_hooks_review_unavailable")
    if startup_probe["hooks_review_title_present"]:
        # Pinned normal UI shortcut 1 opens Review hooks; it grants no trust.
        terminal.send(b"1")
    else:
        terminal.command("/hooks")
    terminal.until(lambda s: "Lifecycle hooks from config and enabled plugins." in s, "hooks_ui_unavailable")
    # Home selects first pinned event. Inspect inventory counts including all
    # zero-handler events before any trust action.
    terminal.send(b"\x1b[H")
    for event in EVENT_ORDER:
        expected = 1 if event in EVENTS else 0
        screen = terminal.until(lambda s: re.search(r"›\s+" + event + r"\s", s), "hook_inventory_navigation")
        require("Issues" not in screen, "hook_inventory_issues")
        require(re.search(r"›\s+" + event + r"\s+" + str(expected) + r"\s+0\s+" + str(expected) + r"\s", screen), "hook_inventory_mismatch")
        terminal.send(b"\x1b[B")
    terminal.send(b"\x1b[H")
    for event in EVENT_ORDER:
        terminal.until(lambda s: re.search(r"›\s+" + event + r"\s", s), "hook_inventory_navigation")
        if event in EVENTS:
            terminal.send(b"\r")
            screen = terminal.until(lambda s: event + " hooks" in s and "Trust" in s, "hook_details_unavailable")
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
    command = str(tempo) + " hook codex --input-stdin"
    require(re.fullmatch(r"[A-Za-z0-9/_-]+ hook codex --input-stdin", command), "unsafe_fixture_path")
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
        terminal = Terminal(argv, env, repo, deadline)
        normal_trust(terminal, repo, command, report["trusted_events"], report.setdefault("workspace_trust_probe", {}),
                     report.setdefault("startup_trust_probe", {}))
        terminal.close(); terminal = None
        require(not model.requests and model.error is None, "unexpected_pretrust_inference")
        report["stage"] = "production_policy_confirmation"
        profile = helper_call("confirm", repo, runtime, tempo, definitions, config)
        report["profile_fingerprint"] = profile["fingerprint"]
        report["configuration_sha256"] = digest(config)
        baseline = {r["id"] for r in helper_call("read")["receipts"]}
        report["stage"] = "measured_restart"
        terminal = Terminal(argv, env, repo, deadline)

        def wait_snapshot(predicate, category, seconds=20):
            result = None
            def check(_):
                nonlocal result
                require(model.error is None, model.error or "provider_failed")
                result = helper_call("read")
                return predicate(result)
            terminal.until(check, category, seconds)
            return result

        snapshot = wait_snapshot(lambda s: any(r["kind"] == "SessionStart" and r["id"] not in baseline and accepted(r) for r in s["receipts"]), "posttrust_session_start_missing")
        starts = [r for r in snapshot["receipts"] if r["kind"] == "SessionStart" and r["id"] not in baseline and accepted(r)]
        require(len(starts) == 1, "measured_session_ambiguous")
        model.session = starts[0]["session_id"]
        report["stage"] = "parent_read_and_child"
        terminal.command(PARENT_PROMPT)
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
