"""Bounded unit/mocked-effect QA. Never launch a host, helper or network call."""
import contextlib
import http.server
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.request
from unittest import mock

spec = importlib.util.spec_from_file_location("codex_stderr_diagnostic", Path(__file__).with_name("codex_stderr_diagnostic.py"))
diagnostic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostic)


def row(status="safe_error", code="state_busy", durability="not_committed"):
    return {"schema_version": 1, "diagnostic_only": True, "acceptance_eligible": False,
            "status": status, "code": code, "durability": durability, "exit_code": 0}


class DiagnosticTests(unittest.TestCase):
    def setUp(self):
        # Install every effect guard before loading/invoking the original runner.
        self.guards = contextlib.ExitStack()
        self.addCleanup(self.guards.close)
        for owner, name in ((subprocess, "Popen"), (subprocess, "run"), (os, "system"),
                            (os, "fork"), (os, "kill"), (os, "killpg"), (socket, "socket"),
                            (threading.Thread, "start"), (time, "sleep"),
                            (urllib.request, "urlopen"),
                            (diagnostic.signal, "alarm"), (diagnostic.signal, "signal")):
            self.guards.enter_context(mock.patch.object(owner, name, side_effect=AssertionError("forbidden external effect")))
        self.smoke = diagnostic.load_smoke()
        self.guards.enter_context(mock.patch.object(self.smoke.pty, "openpty", side_effect=AssertionError("forbidden PTY")))
        self.guards.enter_context(mock.patch.object(self.smoke, "run", side_effect=AssertionError("unmocked native runner")))

    def sink(self):
        parent = tempfile.TemporaryDirectory()
        self.addCleanup(parent.cleanup)
        sink = diagnostic.Sink(parent.name)
        self.addCleanup(sink.close)
        return sink

    def put(self, sink, name, value, mode=0o600):
        data = value if isinstance(value, bytes) else json.dumps(value).encode()
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode, dir_fd=sink.fd)
        try:
            os.fchmod(fd, mode)
            self.assertEqual(os.write(fd, data), len(data))
        finally:
            os.close(fd)

    def test_typed_parser_rejects_unknown_raw_duplicate_and_wrong_types(self):
        self.assertEqual(diagnostic.typed_row(json.dumps(row()).encode()), row())
        bad = [b'PRIVATE', b'[]', json.dumps(dict(row(), payload="PRIVATE")).encode(),
               json.dumps(dict(row(), code="PRIVATE")).encode(),
               json.dumps(dict(row(), durability="PRIVATE")).encode(),
               json.dumps(dict(row(), acceptance_eligible=True)).encode(),
               json.dumps(dict(row(), schema_version=True)).encode(),
               json.dumps(dict(row(), exit_code=True)).encode(),
               json.dumps(dict(row(), exit_code=256)).encode(),
               json.dumps(dict(row(), status="empty")).encode(),
               json.dumps(row()).replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1').encode(),
               b'x' * 257]
        for data in bad:
            with self.subTest(size=len(data)):
                with self.assertRaises(diagnostic.DiagnosticUnavailable) as caught:
                    diagnostic.typed_row(data)
                self.assertNotIn("PRIVATE", str(caught.exception))

    def test_collector_aggregates_only_allowlisted_summaries_and_counts(self):
        sink = self.sink()
        self.assertEqual(sink.collect()["status"], "no_records")
        for n, value in enumerate((row(), row(), row(code="local_write_unknown", durability="unknown"),
                                   row("empty", "", ""), row("unavailable", "", ""))):
            self.put(sink, f"r{n:03d}.json", value)
        result = sink.collect()
        self.assertEqual(result, {"status": "observed", "record_count": 5, "empty_count": 1,
            "unclassified_count": 1, "saturated": False, "counts": [
                {"code": "local_write_unknown", "durability": "unknown", "count": 1},
                {"code": "state_busy", "durability": "not_committed", "count": 2}]})

    def test_collector_refuses_bad_namespace_mode_partial_oversize_and_symlink(self):
        for kind in ("namespace", "mode", "partial", "oversize", "symlink", "hardlink"):
            with self.subTest(kind=kind):
                sink = self.sink()
                if kind == "namespace": self.put(sink, "PRIVATE", row())
                elif kind == "mode": self.put(sink, "r000.json", row(), 0o644)
                elif kind == "partial": self.put(sink, "r000.json", b'{')
                elif kind == "oversize": self.put(sink, "r000.json", b'x' * 257)
                elif kind == "symlink": os.symlink("PRIVATE", sink.path / "r000.json")
                else:
                    self.put(sink, "r000.json", row())
                    os.link(sink.path / "r000.json", sink.path / "r001.json")
                with self.assertRaises((diagnostic.DiagnosticUnavailable, OSError)):
                    sink.collect()

    def test_collector_bound_and_directory_identity(self):
        sink = self.sink()
        for n in range(128): self.put(sink, f"r{n:03d}.json", row("empty", "", ""))
        self.assertTrue(sink.collect()["saturated"])
        self.put(sink, "r128.json", row())
        with self.assertRaises(diagnostic.DiagnosticUnavailable): sink.collect()
        os.chmod(sink.path, 0o755)
        with self.assertRaises(diagnostic.DiagnosticUnavailable): sink.verify()
        os.chmod(sink.path, 0o700)
        original = sink.path
        sink.path = original.parent
        try:
            with self.assertRaises(diagnostic.DiagnosticUnavailable): sink.verify()
        finally:
            sink.path = original

    def test_collector_changed_record_or_close_error_refuses_partial_result(self):
        sink = self.sink()
        self.put(sink, "r000.json", row())
        original_read = os.read
        def changed(fd, size):
            data = original_read(fd, size)
            with open(sink.path / "r000.json", "ab") as output: output.write(b' ')
            return data
        with mock.patch.object(diagnostic.os, "read", side_effect=changed):
            with self.assertRaises(diagnostic.DiagnosticUnavailable): sink.collect()
        original_close = os.close
        def failed_close(fd):
            original_close(fd)
            raise OSError("PRIVATE")
        with mock.patch.object(diagnostic.os, "close", side_effect=failed_close):
            with self.assertRaises(OSError): sink.collect()

    def test_observe_relays_only_four_keys_and_preserves_join_arguments_and_failure(self):
        sink = self.sink()
        self.put(sink, "r000.json", row())
        original_environment = self.smoke.child_environment
        with mock.patch.object(self.smoke, "close_native", return_value=True) as close:
            original_close = self.smoke.close_native
            def run(args, report):
                expected = original_environment({"HOME": "/owned"}, Path("/owned/root"))
                got = self.smoke.child_environment({"HOME": "/owned", "PRIVATE": "canary"}, Path("/owned/root"))
                self.assertEqual({k: v for k, v in got.items() if k not in diagnostic.ENV_KEYS}, expected)
                self.assertEqual({k: got[k] for k in diagnostic.ENV_KEYS}, sink.environment())
                report.update(stage="parent_plan_and_child", tempo_sha256="a" * 64, runtime_sha256="b" * 64,
                              requests=[{"PRIVATE": "secret"}], receipts=["PRIVATE"])
                self.assertTrue(self.smoke.close_native("terminal", "model", primary_failure=True))
                raise self.smoke.FixtureFailure("child_start_barrier_missing")
            with mock.patch.object(self.smoke, "run", side_effect=run):
                result = diagnostic.observe(self.smoke, object(), sink)
            close.assert_called_once_with("terminal", "model", primary_failure=True)
            self.assertIs(self.smoke.close_native, original_close)
        self.assertIs(self.smoke.child_environment, original_environment)
        self.assertEqual(result["failure_category"], "child_start_barrier_missing")
        self.assertEqual(result["stderr_observation"]["record_count"], 1)
        self.assertFalse(result["acceptance_eligible"])
        self.assertEqual(result["completeness"], "not_established")
        self.assertEqual(result["event_attribution"], "unavailable")
        self.assertNotIn("PRIVATE", json.dumps(result))
        self.assertNotIn(str(sink.path), json.dumps(result))

    def test_observe_unjoined_interrupted_cleanup_never_collects_and_restores_functions(self):
        sink = self.sink()
        for outcome in (False, self.smoke.FixtureFailure("fixture_cancelled")):
            with self.subTest(interrupted=isinstance(outcome, Exception)):
                original_environment = self.smoke.child_environment
                with mock.patch.object(self.smoke, "close_native", side_effect=outcome if isinstance(outcome, Exception) else None,
                                       return_value=outcome) as close:
                    original_close = self.smoke.close_native
                    def run(args, report): self.smoke.close_native(None, "model", primary_failure=True)
                    with mock.patch.object(self.smoke, "run", side_effect=run), mock.patch.object(sink, "collect") as collect:
                        result = diagnostic.observe(self.smoke, object(), sink)
                    collect.assert_not_called()
                    self.assertIs(self.smoke.close_native, original_close)
                self.assertIs(self.smoke.child_environment, original_environment)
                self.assertFalse(result["owners_joined"])
                self.assertEqual(result["stderr_observation"]["reason"], "owner_cleanup_unconfirmed")

    def test_observe_collection_failure_cannot_replace_original_failure_or_claim_acceptance(self):
        sink = self.sink()
        for passed in (False, True):
            with self.subTest(gates_completed=passed):
                def run(args, report):
                    self.smoke.close_native(None, "model", primary_failure=not passed)
                    if not passed: raise self.smoke.FixtureFailure("measured_session_ambiguous")
                with mock.patch.object(self.smoke, "close_native", return_value=True), mock.patch.object(self.smoke, "run", side_effect=run), \
                     mock.patch.object(sink, "collect", side_effect=OSError("PRIVATE")):
                    result = diagnostic.observe(self.smoke, object(), sink)
                self.assertEqual(result["failure_category"], "none" if passed else "measured_session_ambiguous")
                self.assertEqual(result["stderr_observation"]["reason"], "diagnostic_records_unavailable")
                self.assertFalse(result["acceptance_eligible"])
                self.assertNotIn("PRIVATE", json.dumps(result))

    def test_main_preserves_failure_and_restores_handlers_after_checked_sink_close_error(self):
        sink = mock.Mock()
        sink.close.side_effect = OSError("PRIVATE")
        result = {"diagnostic_only": True, "acceptance_eligible": False, "status": "failed",
                  "failure_category": "child_start_barrier_missing", "completeness": "not_established",
                  "stderr_observation": {"status": "observed", "record_count": 1}}
        handlers = {sig: object() for sig in (diagnostic.signal.SIGALRM, diagnostic.signal.SIGTERM, diagnostic.signal.SIGINT)}
        with mock.patch.object(diagnostic, "load_smoke", return_value=self.smoke), \
             mock.patch.object(diagnostic, "Sink", return_value=sink), \
             mock.patch.object(diagnostic, "observe", return_value=result), \
             mock.patch.object(diagnostic.signal, "getsignal", side_effect=lambda sig: handlers[sig]), \
             mock.patch.object(diagnostic.signal, "signal") as signals, \
             mock.patch.object(diagnostic.signal, "alarm") as alarm, \
             mock.patch.object(diagnostic.sys, "argv", ["diagnostic", "--tempo", "test-binary", "--helper", "helper", "--evidence", "report"]), \
             mock.patch.dict(os.environ, {"RUNNER_TEMP": "/owned"}), \
             mock.patch.object(Path, "write_text") as write, mock.patch("builtins.print"):
            self.assertEqual(diagnostic.main(), 1)
        sink.close.assert_called_once_with()
        self.assertEqual(alarm.call_args_list, [mock.call(250), mock.call(0)])
        self.assertEqual(signals.call_args_list[-3:], [mock.call(sig, old) for sig, old in handlers.items()])
        emitted = json.loads(write.call_args.args[0])
        self.assertEqual(emitted["failure_category"], "child_start_barrier_missing")
        self.assertEqual(emitted["sink_cleanup"], "unavailable")
        self.assertFalse(emitted["acceptance_eligible"])
        self.assertNotIn("PRIVATE", json.dumps(emitted))


if __name__ == "__main__":
    unittest.main()
