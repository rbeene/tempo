"""Optional hosted Linux diagnosis. Traced observations NEVER count as acceptance.

No host starts on import. Raw fd2 trace bytes are bounded, transient and never
written to a file or exported. The ordinary acceptance script stays unchanged.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys

spec = importlib.util.spec_from_file_location("codex_smoke", Path(__file__).with_name("codex_smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)

MAX_TRACE_BYTES = 1 << 20
MAX_TRACE_LINE = 2048
MAX_PENDING = 32
MAX_SAFE_LINES = 128
CODES = ("validation", "unsupported_contract", "state_corrupt", "state_busy", "local_write_unknown",
         "clock_unavailable", "clock_conflict", "binding_unavailable", "event_conflict", "event_gap",
         "ordering_unavailable", "profile_required", "profile_invalidated", "profile_revoked", "untracked",
         "review_required", "source_lost", "restart_unknown", "incomplete_wait", "source_loss_while_waiting", "internal")
DURABILITIES = ("committed", "not_committed", "unknown")
SAFE_BYTES = {f"tempo hook: {code}; durability={durability}\n".encode(): (code, durability)
              for code in CODES for durability in DURABILITIES}
PREFIX = rb"(?:(?:([1-9][0-9]{0,9})|\[pid ([1-9][0-9]{0,9})\])[ \t]+)?"
PREFIX_ONLY = re.compile(PREFIX)
WRITE = re.compile(PREFIX + rb'write\(2, "((?:\\x[0-9a-f]{2})+)", ([0-9]{1,10})(?:\) += +([0-9]{1,10})| <unfinished \.\.\.>)')
RESUME = re.compile(PREFIX + rb'<\.\.\. write resumed>\) += +([0-9]{1,10})')


class Collector:
    def __init__(self):
        self.total = 0
        self.pending = {}
        self.buffer = b""
        self.values = {}
        self.accepted = 0
        self.failed = False

    def bound(self):
        self.failed = True
        self.buffer = b""
        self.pending.clear()
        raise smoke.FixtureFailure("diagnostic_trace_bound")

    def record(self, value):
        if self.accepted >= MAX_SAFE_LINES: self.bound()
        self.accepted += 1
        self.values[value] = self.values.get(value, 0) + 1

    def feed(self, data):
        if self.failed: return
        self.total += len(data)
        if self.total > MAX_TRACE_BYTES: self.bound()
        self.buffer += data
        while b"\n" in self.buffer:
            raw, self.buffer = self.buffer.split(b"\n", 1)
            if len(raw) > MAX_TRACE_LINE: self.bound()
            prefix = PREFIX_ONLY.match(raw)
            pid = prefix[1] or prefix[2] or b"leader"
            body = raw[prefix.end():]
            prior = None
            if body.startswith((b"write(", b"<... write resumed>")):
                prior = self.pending.pop(pid, None)
            match = WRITE.fullmatch(raw)
            if match:
                # Decode only canonical hexadecimal pairs, never eval/string escapes.
                value = bytes(int(pair, 16) for pair in match[3].split(b"\\x")[1:])
                selected = SAFE_BYTES.get(value)
                if selected is None or len(value) != int(match[4]): continue
                if match[5] is not None:
                    if int(match[5]) == len(value): self.record(selected)
                else:
                    if len(self.pending) >= MAX_PENDING: self.bound()
                    self.pending[pid] = (selected, len(value))
                continue
            match = RESUME.fullmatch(raw)
            if match:
                if prior is not None and int(match[3]) == prior[1]: self.record(prior[0])
        if len(self.buffer) > MAX_TRACE_LINE: self.bound()

    def counts(self):
        return [{"code": code, "durability": durability, "count": count}
                for (code, durability), count in sorted(self.values.items())]


def trace_argv(tracer, output_fd, argv):
    smoke.require(type(output_fd) is int and output_fd > 2, "diagnostic_trace_fd")
    return [tracer, "-f", "--kill-on-exit", "-qq", "-xx", "-s", "128",
            "-e", "trace=write", "-e", "trace-fds=2", "-e", "signal=none",
            "-o", f"/proc/self/fd/{output_fd}", "--", *argv]


def close_descriptors(*fds):
    ok = True
    for fd in fds:
        if fd is None: continue
        try: os.close(fd)
        except OSError: ok = False
    return ok


def precondition():
    smoke.hosted_precondition(os.environ, smoke.platform.system(), smoke.platform.machine(),
                             smoke.pwd.getpwuid(os.getuid()).pw_dir)


def find_strace():
    path = shutil.which("strace", path="/usr/bin:/bin")
    if path is None: return None
    env = {"HOME": os.environ.get("HOME", ""), "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    try:
        output = smoke.bounded_run([path, "--version"], env, Path.cwd(), timeout=2)
    except Exception:
        return None
    # Only the reviewed manual/version is supported; no install or ptrace changes.
    return path if output.splitlines()[:1] == [b"strace -- version 6.8"] else None


class Session:
    def __init__(self, tracer):
        self.tracer = tracer
        self.collector = Collector()
        self.owners = []  # Exactly the existing trust and measured launches.
        self.unjoined = False
        self.phase = "trust"

    def close(self):
        for owner in reversed(self.owners):
            try: owner.close()
            except Exception as exc:
                if not (owner.closed and self.collector.failed and isinstance(exc, smoke.FixtureFailure)
                        and str(exc) == "diagnostic_trace_bound"):
                    self.unjoined = True


class TraceTerminal(smoke.Terminal):
    def __init__(self, argv, env, cwd, deadline, input_probe=None, *, session):
        smoke.require(not session.collector.failed, "diagnostic_trace_bound")
        smoke.require(len(session.owners) < 2, "diagnostic_launch_bound")
        smoke.require(not session.unjoined and all(owner.closed for owner in session.owners),
                      "diagnostic_previous_owner_unreleased")
        self.session, self.deadline = session, deadline
        self.input_probe = {} if input_probe is None else input_probe
        self.screen, self.query_tail = smoke.Screen(), b""
        self.proc, self.master, self.trace_read, self.closed = None, None, None, False
        if session.owners:
            session.phase = "measured"
            # Do not attribute trust-launch messages or unfinished writes to the
            # measured process. The lifetime byte budget does not reset.
            session.collector.pending.clear()
            session.collector.buffer = b""
            session.collector.values.clear()
            session.collector.accepted = 0
        session.owners.append(self)
        slave = writer = None
        try:
            self.master, slave = smoke.pty.openpty()
            smoke.fcntl.ioctl(slave, smoke.termios.TIOCSWINSZ,
                              smoke.struct.pack("HHHH", self.screen.rows, self.screen.cols, 0, 0))
            self.trace_read, writer = os.pipe()
            os.set_blocking(self.trace_read, False)
            self.proc = subprocess.Popen(trace_argv(session.tracer, writer, argv), env=env, cwd=cwd,
                                         stdin=slave, stdout=slave, stderr=slave, start_new_session=True,
                                         pass_fds=(writer,))
        except Exception:
            self.close()
            raise smoke.FixtureFailure("diagnostic_trace_spawn")
        finally:
            if not close_descriptors(slave, writer):
                session.unjoined = True
                self.close()
                raise smoke.FixtureFailure("diagnostic_descriptor_cleanup")

    def read_trace(self):
        smoke.require(not self.session.collector.failed, "diagnostic_trace_bound")
        if self.trace_read is None: return False
        try:
            chunk = os.read(self.trace_read, 16384)
        except BlockingIOError:
            return False
        if not chunk:
            os.close(self.trace_read)
            self.trace_read = None
            return False
        self.session.collector.feed(chunk)
        return True

    def pump(self, wait=.05):
        smoke.require(smoke.time.monotonic() < self.deadline, "overall_deadline")
        fds = [self.master] + ([] if self.trace_read is None else [self.trace_read])
        ready = smoke.select.select(fds, [], [], wait)[0]
        if self.trace_read is not None and self.trace_read in ready: self.read_trace()
        # All keyboard/trust/screen behavior stays in the ordinary implementation.
        super().pump(wait=0)

    def close(self):
        if self.closed: return
        joined = self.proc is None
        failure = None
        try:
            if self.proc is not None:
                try:
                    if self.proc.poll() is None:
                        try: self.proc.terminate()
                        except ProcessLookupError: pass
                        self.proc.wait(timeout=1)
                    joined = True
                except subprocess.TimeoutExpired:
                    pass
                except BaseException as exc:
                    failure = exc
                finally:
                    # Cancellation during TERM or its wait must still reach the
                    # owned leader's forced kill AND final bounded join.
                    if not joined:
                        try:
                            try: self.proc.kill()
                            except ProcessLookupError: pass
                        except BaseException as exc:
                            if failure is None: failure = exc
                        finally:
                            try:
                                self.proc.wait(timeout=2)
                                joined = True
                            except BaseException as exc:
                                if failure is None: failure = exc
            if joined and self.proc is not None and self.trace_read is not None:
                # Nonblocking final drain; no thread or indefinite EOF wait.
                try:
                    while self.read_trace(): pass
                except BaseException as exc:
                    if failure is None: failure = exc
        finally:
            self.session.collector.pending.clear()
            self.session.collector.buffer = b""
            ok = close_descriptors(self.trace_read, self.master)
            self.trace_read, self.master = None, None
            self.closed = joined and ok
            if not self.closed:
                self.session.unjoined = True
            if not ok and failure is None:
                failure = smoke.FixtureFailure("diagnostic_descriptor_cleanup")
        if failure is not None: raise failure


def execute(args):
    report = {"schema_version": 1, "diagnostic_only": True, "acceptance_eligible": False,
              "status": "unavailable", "reason": "precondition", "phase": "not_started", "safe_error_counts": []}
    head = os.environ.get("TEMPO_DIAGNOSTIC_HEAD_SHA", "")
    report["source_head_sha"] = head if re.fullmatch(r"[a-f0-9]{40}", head) else None
    session = None
    original = smoke.Terminal
    try:
        precondition()
        tracer = find_strace()
        if tracer is None:
            report["reason"] = "tracer_unavailable"
            return report
        session = Session(tracer)
        def terminal(*values, **keywords):
            return TraceTerminal(*values, **keywords, session=session)
        smoke.Terminal = terminal
        report.update(status="observed", reason="no_safe_error_observed")
        smoke.run(args, {})  # Never export its acceptance-shaped report.
    except Exception as exc:
        report["status"] = "incomplete"
        category = str(exc) if isinstance(exc, smoke.FixtureFailure) else ""
        report["reason"] = category if category in {
            "child_start_barrier_missing", "measured_session_ambiguous", "posttrust_session_start_missing",
            "overall_deadline", "diagnostic_trace_bound", "diagnostic_trace_spawn"} else "fixture_failed"
    finally:
        try:
            if session is not None:
                session.close()
                report["phase"] = session.phase
                report["safe_error_counts"] = session.collector.counts()
                if session.collector.failed: report.update(status="incomplete", reason="diagnostic_trace_bound")
                if session.unjoined: report.update(status="incomplete", reason="owner_cleanup_failed")
                elif report["status"] == "observed" and report["safe_error_counts"]:
                    report["reason"] = "safe_error_observed"
        finally:
            smoke.Terminal = original
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tempo", required=True)
    parser.add_argument("--helper", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    def alarm(*_): raise smoke.FixtureFailure("overall_deadline")
    def cancelled(*_): raise smoke.FixtureFailure("fixture_cancelled")
    old = {sig: signal.getsignal(sig) for sig in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}
    signal.signal(signal.SIGALRM, alarm)
    signal.signal(signal.SIGTERM, cancelled)
    signal.signal(signal.SIGINT, cancelled)
    signal.alarm(250)
    report = {"schema_version": 1, "diagnostic_only": True, "acceptance_eligible": False,
              "status": "incomplete", "reason": "fixture_failed", "phase": "not_started", "safe_error_counts": [],
              "source_head_sha": None}
    try:
        report = execute(args)
    except Exception:
        # Never print an exception or turn an outer cancellation into acceptance.
        report["status"], report["reason"] = "incomplete", "fixture_failed"
    finally:
        signal.alarm(0)
        for sig, handler in old.items(): signal.signal(sig, handler)
    Path(args.evidence).write_text(json.dumps(report, indent=2) + "\n")
    print("native_codex_diagnostic: " + report["status"] + "; acceptance_eligible=false")
    return 1 if report["reason"] == "owner_cleanup_failed" else 0


if __name__ == "__main__":
    sys.exit(main())
