"""Finite pure mocks: no host, tracer, helper, process, thread or socket starts."""
import importlib.util
import json
from pathlib import Path
import signal
import socket
import subprocess
import types
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("codex_trace_diagnostic", Path(__file__).with_name("codex_trace_diagnostic.py"))
diag = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diag)


def line(code="state_busy", durability="not_committed", pid=123, result=None):
    data = f"tempo hook: {code}; durability={durability}\n".encode()
    encoded = "".join(f"\\x{value:02x}" for value in data)
    size = len(data)
    return f'{pid} write(2, "{encoded}", {size}) = {size if result is None else result}\n'.encode()


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        def forbidden(*args, **kwargs):
            raise AssertionError("unmocked runtime effect")
        for target, name in ((diag.subprocess, "Popen"), (diag.os, "kill"), (diag.os, "killpg"),
                             (diag.os, "read"), (diag.os, "write"), (diag.os, "pipe"),
                             (diag.smoke.pty, "openpty"), (diag.smoke.select, "select"),
                             (diag.smoke.threading.Thread, "start"), (diag.signal, "alarm"),
                             (socket, "socket"), (diag.smoke.time, "sleep"),
                             (diag.smoke, "download_runtime"), (diag.smoke, "bounded_run")):
            patcher = mock.patch.object(target, name, side_effect=forbidden)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_exact_safe_lines_and_chunk_boundaries_count_without_exporting_trace(self):
        collector = diag.Collector()
        raw = line() + line("local_write_unknown", "unknown") + line()
        for offset in range(0, len(raw), 7): collector.feed(raw[offset:offset + 7])
        self.assertEqual(collector.counts(), [
            {"code": "local_write_unknown", "durability": "unknown", "count": 1},
            {"code": "state_busy", "durability": "not_committed", "count": 2}])
        self.assertNotIn("write(", json.dumps(collector.counts()))

    def test_unfinished_write_requires_matching_successful_resume_once(self):
        collector = diag.Collector()
        complete = line()
        start, result = complete.split(b") = ")
        collector.feed(start + b" <unfinished ...>\n")
        self.assertEqual(collector.counts(), [])
        collector.feed(b"456 <... write resumed>) = " + result)
        self.assertEqual(collector.counts(), [])
        collector.feed(b"123 <... write resumed>) = " + result)
        collector.feed(b"123 <... write resumed>) = " + result)
        self.assertEqual(collector.counts()[0]["count"], 1)
        collector.feed(start + b" <unfinished ...>\n")
        collector.feed(b"123 <... write resumed>) = -1 EPIPE (ignored)\n")
        collector.feed(b"123 <... write resumed>) = " + result)
        self.assertEqual(collector.counts()[0]["count"], 1)

    def test_unknown_raw_partial_failed_truncated_and_wrong_fd_never_become_codes(self):
        collector = diag.Collector()
        cases = [line("CANARY"), line(durability="CANARY"), line(result=-1), line(result=1),
                 line().replace(b"write(2", b"write(1"), b'123 write(2, "tempo hook: state_busy; durability=not_committed\\n", 49) = 49\n',
                 b'123 write(2, "\\x74"..., 900) = 900\n', b"CANARY /private/path token=CANARY\n",
                 b"123 <... write resumed>) = 49\n", line()[:-1]]
        for raw in cases: collector.feed(raw)
        self.assertEqual(collector.counts(), [])
        self.assertNotIn("CANARY", json.dumps(collector.counts()))

    def test_byte_line_pending_and_count_bounds_are_finite(self):
        for setup, data in [(lambda c: setattr(c, "total", diag.MAX_TRACE_BYTES), b"x"),
                            (lambda c: None, b"x" * (diag.MAX_TRACE_LINE + 1))]:
            collector = diag.Collector(); setup(collector)
            with self.assertRaises(diag.smoke.FixtureFailure): collector.feed(data)
            self.assertTrue(collector.failed)
        collector = diag.Collector()
        for pid in range(1, diag.MAX_PENDING + 2):
            raw = line(pid=pid).split(b") = ")[0] + b" <unfinished ...>\n"
            if pid <= diag.MAX_PENDING: collector.feed(raw)
            else:
                with self.assertRaises(diag.smoke.FixtureFailure): collector.feed(raw)
        collector = diag.Collector()
        collector.accepted = diag.MAX_SAFE_LINES
        with self.assertRaises(diag.smoke.FixtureFailure): collector.feed(line())

    def test_argv_is_launch_only_fd2_hex_no_attach_shell_dump_or_sidecar(self):
        runtime = ["/synthetic/codex", "--no-alt-screen", "--no-daemon"]
        self.assertEqual(diag.trace_argv("/usr/bin/strace", 19, runtime), [
            "/usr/bin/strace", "-f", "--kill-on-exit", "-qq", "-xx", "-s", "128",
            "-e", "trace=write", "-e", "trace-fds=2", "-e", "signal=none",
            "-o", "/proc/self/fd/19", "--", *runtime])
        with self.assertRaises(diag.smoke.FixtureFailure): diag.trace_argv("/usr/bin/strace", 2, runtime)

    def test_missing_or_unverified_strace_is_unavailable_without_install(self):
        with mock.patch.object(diag.shutil, "which", return_value=None), mock.patch.object(diag.smoke, "bounded_run") as run:
            self.assertIsNone(diag.find_strace())
            run.assert_not_called()
        with mock.patch.object(diag.shutil, "which", return_value="/usr/bin/strace"), \
             mock.patch.object(diag.smoke, "bounded_run", return_value=b"strace -- version 6.8\nCopyright\n") as run:
            self.assertEqual(diag.find_strace(), "/usr/bin/strace")
            self.assertEqual(run.call_args.args[0], ["/usr/bin/strace", "--version"])
            self.assertEqual(run.call_args.kwargs["timeout"], 2)
        for output in (b"strace -- version 6.9\n", b"CANARY\n"):
            with mock.patch.object(diag.shutil, "which", return_value="/usr/bin/strace"), mock.patch.object(diag.smoke, "bounded_run", return_value=output):
                self.assertIsNone(diag.find_strace())

    def test_only_terminal_launch_inherits_trace_fd_and_spawn_failure_closes_every_fd(self):
        session = diag.Session("/usr/bin/strace")
        proc = mock.Mock()
        with mock.patch.object(diag.smoke.pty, "openpty", return_value=(10, 11)), \
             mock.patch.object(diag.smoke.fcntl, "ioctl"), mock.patch.object(diag.os, "pipe", return_value=(12, 13)), \
             mock.patch.object(diag.os, "set_blocking"), mock.patch.object(diag.os, "close") as close, \
             mock.patch.object(diag.subprocess, "Popen", return_value=proc) as popen:
            terminal = diag.TraceTerminal(["/synthetic/codex", "--no-alt-screen", "--no-daemon"], {}, Path("/synthetic"), 99, session=session)
            self.assertEqual(popen.call_args.kwargs["pass_fds"], (13,))
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertEqual((popen.call_args.kwargs["stdin"], popen.call_args.kwargs["stdout"], popen.call_args.kwargs["stderr"]), (11, 11, 11))
            self.assertEqual(close.call_args_list, [mock.call(11), mock.call(13)])
            self.assertIs(session.owners[0], terminal)
        session = diag.Session("/usr/bin/strace")
        with mock.patch.object(diag.smoke.pty, "openpty", return_value=(20, 21)), \
             mock.patch.object(diag.smoke.fcntl, "ioctl"), mock.patch.object(diag.os, "pipe", return_value=(22, 23)), \
             mock.patch.object(diag.os, "set_blocking"), mock.patch.object(diag.os, "close") as close, \
             mock.patch.object(diag.subprocess, "Popen", side_effect=OSError("CANARY")):
            with self.assertRaises(diag.smoke.FixtureFailure):
                diag.TraceTerminal(["/synthetic/codex"], {}, Path("/synthetic"), 99, session=session)
            self.assertEqual(sorted(c.args[0] for c in close.call_args_list), [20, 21, 22, 23])

    def owner(self, proc):
        session = diag.Session("/usr/bin/strace")
        terminal = diag.TraceTerminal.__new__(diag.TraceTerminal)
        terminal.session, terminal.proc = session, proc
        terminal.master, terminal.trace_read, terminal.closed = None, None, False
        session.owners.append(terminal)
        return session, terminal

    def test_cleanup_never_signals_reaped_leader_or_pgid_and_kills_then_joins_live_tracer(self):
        proc = mock.Mock(); proc.poll.return_value = 0
        session, terminal = self.owner(proc)
        terminal.close(); terminal.close()
        proc.terminate.assert_not_called(); proc.kill.assert_not_called()
        self.assertFalse(session.unjoined)
        proc = mock.Mock(); proc.poll.return_value = None
        proc.wait.side_effect = [subprocess.TimeoutExpired("strace", 1), 0]
        session, terminal = self.owner(proc)
        terminal.close()
        self.assertEqual(proc.method_calls, [mock.call.poll(), mock.call.terminate(), mock.call.wait(timeout=1), mock.call.kill(), mock.call.wait(timeout=2)])
        self.assertFalse(session.unjoined)

    def test_cleanup_failure_remains_incomplete_and_other_owners_are_attempted(self):
        session = diag.Session("/usr/bin/strace")
        bad, good = mock.Mock(), mock.Mock()
        bad.close.side_effect = RuntimeError("CANARY")
        session.owners = [good, bad]
        session.close()
        bad.close.assert_called_once(); good.close.assert_called_once()
        self.assertTrue(session.unjoined)

    def test_final_trace_bound_still_releases_descriptors_after_tracer_join(self):
        proc = mock.Mock(); proc.poll.return_value = None
        session, terminal = self.owner(proc)
        terminal.trace_read, terminal.master = 12, 10
        session.collector.total = diag.MAX_TRACE_BYTES
        with mock.patch.object(diag.os, "read", return_value=b"x"), mock.patch.object(diag.os, "close") as close:
            with self.assertRaisesRegex(diag.smoke.FixtureFailure, "diagnostic_trace_bound"):
                terminal.close()
        self.assertEqual(close.call_args_list, [mock.call(12), mock.call(10)])
        self.assertTrue(session.collector.failed)
        self.assertFalse(session.unjoined)
        proc.wait.assert_called_once_with(timeout=1)
        # A bound first discovered by outer cleanup keeps its diagnostic reason;
        # successfully released resources must not become an ownership failure.
        proc = mock.Mock(); proc.poll.return_value = None
        session, terminal = self.owner(proc)
        terminal.trace_read, terminal.master = 12, 10
        session.collector.total = diag.MAX_TRACE_BYTES
        args = types.SimpleNamespace(tempo="unused", helper="unused", evidence="unused")
        with mock.patch.object(diag, "precondition"), mock.patch.object(diag, "find_strace", return_value="/usr/bin/strace"), \
             mock.patch.object(diag, "Session", return_value=session), mock.patch.object(diag.smoke, "run"), \
             mock.patch.object(diag.os, "read", return_value=b"x"), mock.patch.object(diag.os, "close"):
            result = diag.execute(args)
        self.assertTrue(terminal.closed)
        self.assertFalse(session.unjoined)
        self.assertEqual(result["reason"], "diagnostic_trace_bound")
        self.assertEqual(result["status"], "incomplete")

    def test_interrupted_graceful_terminate_or_wait_forces_join_before_preserving_error(self):
        for stage in ("terminate", "wait"):
            with self.subTest(stage=stage):
                proc = mock.Mock(); proc.poll.return_value = None
                session, terminal = self.owner(proc)
                terminal.master = 10
                interruption = diag.smoke.FixtureFailure("fixture_cancelled")
                if stage == "terminate": proc.terminate.side_effect = interruption
                else: proc.wait.side_effect = [interruption, 0]
                proc.kill.side_effect = lambda: self.assertFalse(terminal.closed)
                with mock.patch.object(diag.os, "close") as close:
                    with self.assertRaises(diag.smoke.FixtureFailure) as caught:
                        terminal.close()
                self.assertIs(caught.exception, interruption)
                proc.kill.assert_called_once_with()
                self.assertEqual(proc.wait.call_args_list,
                                 ([mock.call(timeout=1)] if stage == "wait" else []) + [mock.call(timeout=2)])
                self.assertTrue(terminal.closed)
                self.assertFalse(session.unjoined)
                self.assertEqual(close.call_args_list, [mock.call(10)])
                terminal.close()
                proc.kill.assert_called_once_with()

    def test_interrupted_forced_join_keeps_same_owner_retryable(self):
        proc = mock.Mock(); proc.poll.return_value = None
        session, terminal = self.owner(proc)
        first = diag.smoke.FixtureFailure("fixture_cancelled")
        proc.wait.side_effect = [first, diag.smoke.FixtureFailure("overall_deadline")]
        with self.assertRaises(diag.smoke.FixtureFailure) as caught:
            terminal.close()
        self.assertIs(caught.exception, first)
        proc.kill.assert_called_once_with()
        self.assertFalse(terminal.closed)
        self.assertTrue(session.unjoined)
        self.assertIs(session.owners[0], terminal)
        # The next bounded cleanup can observe the exact owned leader reaped.
        proc.poll.return_value = 0
        proc.wait.side_effect = None
        session.close()
        self.assertTrue(terminal.closed)
        proc.kill.assert_called_once_with()
        proc.terminate.assert_called_once_with()

    def test_final_trust_drain_bound_is_terminal_and_cannot_launch_measured_host(self):
        for kind in ("bytes", "line", "count"):
            with self.subTest(kind=kind):
                proc = mock.Mock(); proc.poll.return_value = None
                session, terminal = self.owner(proc)
                terminal.trace_read, terminal.master = 12, 10
                if kind == "bytes":
                    session.collector.total = diag.MAX_TRACE_BYTES
                    chunk = b"x"
                elif kind == "line": chunk = b"x" * (diag.MAX_TRACE_LINE + 1)
                else:
                    session.collector.accepted = diag.MAX_SAFE_LINES
                    chunk = line()
                with mock.patch.object(diag.os, "read", return_value=chunk), mock.patch.object(diag.os, "close") as close:
                    with self.assertRaisesRegex(diag.smoke.FixtureFailure, "diagnostic_trace_bound"):
                        terminal.close()
                self.assertTrue(terminal.closed)
                self.assertTrue(session.collector.failed)
                self.assertFalse(session.unjoined)
                self.assertEqual(close.call_args_list, [mock.call(12), mock.call(10)])
                total = session.collector.total
                with mock.patch.object(diag.subprocess, "Popen") as popen, mock.patch.object(diag.smoke.pty, "openpty") as pty:
                    with self.assertRaisesRegex(diag.smoke.FixtureFailure, "diagnostic_trace_bound"):
                        diag.TraceTerminal(["/synthetic/codex"], {}, Path("/synthetic"), 99, session=session)
                    popen.assert_not_called(); pty.assert_not_called()
                self.assertEqual(session.phase, "trust")
                self.assertEqual(session.collector.total, total)
                self.assertEqual(session.owners, [terminal])

    def test_descriptor_failure_does_not_skip_remaining_owned_descriptors(self):
        with mock.patch.object(diag.os, "close", side_effect=[OSError("CANARY"), None]) as close:
            self.assertFalse(diag.close_descriptors(12, None, 10))
            self.assertEqual(close.call_args_list, [mock.call(12), mock.call(10)])

    def test_measured_restart_drops_trust_counts_and_fragments_without_resetting_byte_budget(self):
        session = diag.Session("/usr/bin/strace")
        with mock.patch.object(diag.smoke.pty, "openpty", return_value=(10, 11)), \
             mock.patch.object(diag.smoke.fcntl, "ioctl"), mock.patch.object(diag.os, "pipe", return_value=(12, 13)), \
             mock.patch.object(diag.os, "set_blocking"), mock.patch.object(diag.os, "close"), \
             mock.patch.object(diag.subprocess, "Popen") as popen:
            first = diag.TraceTerminal(["/synthetic/codex"], {}, Path("/synthetic"), 99, session=session)
            session.collector.feed(line())
            total = session.collector.total
            session.collector.pending[b"123"] = (("state_busy", "not_committed"), 49)
            session.collector.buffer = b"CANARY"
            # Closure behavior is exercised separately with owned fake processes.
            first.closed = True
            diag.TraceTerminal(["/synthetic/codex"], {}, Path("/synthetic"), 99, session=session)
            self.assertEqual(session.phase, "measured")
            self.assertEqual(session.collector.counts(), [])
            self.assertEqual(session.collector.pending, {})
            self.assertEqual(session.collector.buffer, b"")
            self.assertEqual(session.collector.total, total)
            with self.assertRaises(diag.smoke.FixtureFailure):
                diag.TraceTerminal(["/synthetic/codex"], {}, Path("/synthetic"), 99, session=session)
            self.assertEqual(popen.call_count, 2)

    def test_pump_services_trace_and_preserves_original_terminal_and_deadline(self):
        session, terminal = self.owner(mock.Mock())
        terminal.trace_read, terminal.master, terminal.deadline = 12, 10, 99
        with mock.patch.object(diag.smoke.time, "monotonic", return_value=98), \
             mock.patch.object(diag.smoke.select, "select", return_value=([12, 10], [], [])), \
             mock.patch.object(diag.os, "read", return_value=line()), \
             mock.patch.object(diag.TraceTerminal.__mro__[1], "pump") as original:
            terminal.pump()
            original.assert_called_once_with(wait=0)
            self.assertEqual(session.collector.counts()[0]["code"], "state_busy")
        with mock.patch.object(diag.smoke.time, "monotonic", return_value=100):
            with self.assertRaisesRegex(diag.smoke.FixtureFailure, "overall_deadline"): terminal.pump()
        # A prior pump overflow is terminal: cleanup may join and close,
        # but must not consume another unaccounted byte from the trace pipe.
        proc = mock.Mock(); proc.poll.return_value = 0
        session, terminal = self.owner(proc)
        terminal.trace_read, terminal.master = 12, 10
        session.collector.total = diag.MAX_TRACE_BYTES
        with self.assertRaisesRegex(diag.smoke.FixtureFailure, "diagnostic_trace_bound"):
            session.collector.feed(b"x")
        with mock.patch.object(diag.os, "read", return_value=b"CANARY") as read, \
             mock.patch.object(diag.os, "close") as close:
            with self.assertRaisesRegex(diag.smoke.FixtureFailure, "diagnostic_trace_bound"):
                terminal.read_trace()
            read.assert_not_called()
            with self.assertRaisesRegex(diag.smoke.FixtureFailure, "diagnostic_trace_bound"):
                terminal.close()
            read.assert_not_called()
        self.assertTrue(terminal.closed)
        self.assertFalse(session.unjoined)
        self.assertEqual(close.call_args_list, [mock.call(12), mock.call(10)])

    def test_success_failure_and_unavailable_can_never_be_acceptance(self):
        original = diag.smoke.Terminal
        args = types.SimpleNamespace(tempo="unused", helper="unused", evidence="unused")
        def completed(_args, scratch):
            scratch.update(status="passed", private_path="CANARY", receipts=[{"raw": "CANARY"}])
        for failure in (completed, diag.smoke.FixtureFailure("child_start_barrier_missing"),
                        diag.smoke.FixtureFailure("fixture_cancelled"), RuntimeError("CANARY")):
            with mock.patch.object(diag, "precondition"), mock.patch.object(diag, "find_strace", return_value="/usr/bin/strace"), \
                 mock.patch.object(diag.smoke, "run", side_effect=failure), \
                 mock.patch.object(diag.Session, "close", autospec=True) as close:
                result = diag.execute(args)
                close.assert_called_once()
            self.assertTrue(result["diagnostic_only"])
            self.assertIs(result["acceptance_eligible"], False)
            self.assertNotIn("passed", json.dumps(result))
            self.assertNotIn("CANARY", json.dumps(result))
            self.assertIs(diag.smoke.Terminal, original)
        with mock.patch.object(diag, "precondition"), mock.patch.object(diag, "find_strace", return_value=None), mock.patch.object(diag.smoke, "run") as run:
            result = diag.execute(args)
            self.assertEqual(result["status"], "unavailable")
            run.assert_not_called()
        try:
            with mock.patch.object(diag, "precondition"), mock.patch.object(diag, "find_strace", return_value="/usr/bin/strace"), \
                 mock.patch.object(diag.smoke, "run"), \
                 mock.patch.object(diag.Session, "close", side_effect=diag.smoke.FixtureFailure("fixture_cancelled")):
                with self.assertRaises(diag.smoke.FixtureFailure): diag.execute(args)
                self.assertIs(diag.smoke.Terminal, original)
        finally:
            # A failed old-producer assertion must not contaminate later cases.
            diag.smoke.Terminal = original


if __name__ == "__main__":
    unittest.main()
