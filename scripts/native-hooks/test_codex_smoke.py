"""Pure harness tests. Never downloads or starts Codex, including on macOS."""
import copy
import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
import threading
from unittest import mock
import json
import time

spec = importlib.util.spec_from_file_location("codex_smoke", Path(__file__).with_name("codex_smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class HarnessTests(unittest.TestCase):
    def test_hosted_precondition_cannot_be_enabled_by_one_ci_flag(self):
        env = {"HOME": "/home/runner", "GITHUB_ACTIONS": "true", "RUNNER_ENVIRONMENT": "github-hosted",
               "RUNNER_OS": "Linux", "RUNNER_ARCH": "X64", "RUNNER_TEMP": "/tmp/runner"}
        smoke.hosted_precondition(env, "Linux", "x86_64", "/home/runner")
        for key in env:
            bad = dict(env)
            del bad[key]
            with self.subTest(key=key), self.assertRaises(smoke.FixtureFailure):
                smoke.hosted_precondition(bad, "Linux", "x86_64", "/home/runner")
        for bad in [dict(env, CODEX_HOME=""), dict(env, RUNNER_ENVIRONMENT="self-hosted")]:
            with self.assertRaises(smoke.FixtureFailure):
                smoke.hosted_precondition(bad, "Linux", "x86_64", "/home/runner")
        with self.assertRaises(smoke.FixtureFailure):
            smoke.hosted_precondition(env, "Darwin", "arm64", "/home/runner")
        with self.assertRaises(smoke.FixtureFailure):
            smoke.hosted_precondition(env, "Linux", "x86_64", "/different/home")

    def test_environment_is_allowlisted_and_home_is_unchanged(self):
        parent = {"HOME": "/home/runner", "PATH": "secret/path", "HARVEST_TOKEN": "secret",
                  "OPENAI_API_KEY": "secret", "GITHUB_TOKEN": "secret", "AWS_SECRET_ACCESS_KEY": "secret",
                  "HTTPS_PROXY": "secret", "SSH_AUTH_SOCK": "secret", "CODEX_THREAD_ID": "secret"}
        actual = smoke.child_environment(parent, Path("/tmp/native"))
        self.assertEqual(actual["HOME"], parent["HOME"])
        self.assertNotIn("CODEX_HOME", actual)
        self.assertFalse(any("secret" in v for v in actual.values()))
        self.assertEqual(set(actual), {"HOME", "PATH", "LANG", "LC_ALL", "TERM", "TMPDIR",
                                      "TEMPO_STATE", "TEMPO_HOOK_STATE", "TEMPO_CI_PROVIDER_TOKEN"})

    def test_unexpected_state_fails_without_reading_or_deleting_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state"
            smoke.require_absent([path])
            path.write_text("SECRET")
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_absent([path])
            self.assertEqual(path.read_text(), "SECRET")
            path.unlink()
            path.symlink_to(Path(tmp) / "missing")
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_absent([path])

    def test_trust_review_requires_exact_command_source_sync_and_one_handler(self):
        screen = "SessionStart hooks\n› [!] Hook 1 · new\nEvent: SessionStart\nSource: User - ~/.codex/hooks.json\nCommand: /tmp/native/tempo hook codex --input-stdin\nMode: Sync\nTimeout: 2s\nTrust: New hook - review required\nt trust · esc back"
        smoke.review_hook_screen(screen, "SessionStart", "/tmp/native/tempo hook codex --input-stdin", "~/.codex/hooks.json")
        for old, new in [("Sync", "Async"), ("User -", "Managed -"), ("/tmp/native/tempo", "/tmp/evil"),
                         ("hooks.json", "other.json"), ("SessionStart hooks", "Stop hooks"),
                         ("Timeout: 2s", "Timeout: 20s"), ("[!] Hook 1 · new", "[!] Hook 1 · new\n[!] Hook 2 · new")]:
            with self.subTest(change=new), self.assertRaises(smoke.FixtureFailure):
                smoke.review_hook_screen(screen.replace(old, new), "SessionStart", "/tmp/native/tempo hook codex --input-stdin", "~/.codex/hooks.json")

    def test_screen_redraw_does_not_leave_old_trust_evidence(self):
        screen = smoke.Screen(6, 80)
        screen.feed(b"\x1b[2J\x1b[HCommand: expected\r\nTrust: New hook - review required")
        self.assertIn("Command: expected", screen.text())
        screen.feed(b"\x1b[H\x1b[2JCommand: unexpected")
        self.assertNotIn("Trust:", screen.text())
        self.assertNotIn("Command: expected", screen.text())

    def test_receipt_projection_cannot_export_raw_payload_or_unknown_status(self):
        receipt = self.receipt("SessionStart")
        receipt.update(prompt="SECRET", headers={"Authorization": "SECRET"}, diagnostic_code="SECRET")
        projected = smoke.project_receipt(receipt)
        self.assertNotIn("SECRET", str(projected))
        self.assertNotIn("prompt", projected)
        receipt["origin"] = "host_observed"
        with self.assertRaises(smoke.FixtureFailure):
            smoke.project_receipt(receipt)

    @staticmethod
    def receipt(kind, **extra):
        result = {"id": "receipt-" + kind, "source": "codex", "session_id": "session-1", "turn_id": "turn-1",
                  "agent_id": "", "kind": kind, "tool_id": "", "disposition": "applied", "ordering": "supported",
                  "durability": "committed", "origin": "unverified", "profile_basis": "operator_declared",
                  "snapshot_revision": "1", "profile_revision": "1", "fingerprint": "a" * 64,
                  "observed_at": "2026-10-02T10:00:00Z", "actor": None}
        result.update(extra)
        return result

    def test_request_barrier_requires_actual_matching_prompt_and_session(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        smoke.require_prompt_barrier(receipts, "session-1", "turn-1")
        for bad in [receipts[:1], receipts[1:], [receipts[0], self.receipt("UserPromptSubmit", turn_id="old")],
                    [receipts[0], self.receipt("UserPromptSubmit", disposition="review_required")]]:
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_prompt_barrier(bad, "session-1", "turn-1")

    def test_tool_result_must_prove_real_read_before_spawn(self):
        good = {"input": [{"type": "function_call_output", "call_id": "tempo-read", "output": "tempo-fixture-read-ok"}]}
        smoke.require_read_result(good)
        for bad in [{"input": []}, {"input": [{"type": "function_call", "call_id": "tempo-read", "output": "tempo-fixture-read-ok"}]},
                    {"input": [{"type": "function_call_output", "call_id": "wrong", "output": "tempo-fixture-read-ok"}]}]:
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_read_result(bad)

    def test_timeout_cleanup_terminates_owned_process_group(self):
        # A plain Python sleeper is safe locally; this never starts an application host.
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        smoke.terminate_group(proc)
        self.assertIsNotNone(proc.poll())

    def model(self, receipts):
        # Invoke the pure protocol transition without opening a socket or host.
        model = object.__new__(smoke.Model)
        model.read = lambda: {"receipts": receipts}
        model.repo = Path("/tmp/project")
        model.lock = threading.Lock()
        model.session, model.turn, model.child = "session-1", None, None
        model.child_turn = None
        model.shutdown = threading.Event()
        model.phase, model.counts, model.requests = "initial", {}, []
        return model

    def request(self, marker=smoke.PARENT_PROMPT):
        return {"model": "gpt-6.1-sol", "stream": True, "input": [{"role": "user", "content": [{"text": marker}]}],
                "tools": [{"type": "function", "name": "exec_command", "parameters": {"properties": {"cmd": {}, "workdir": {}, "max_output_tokens": {}}, "required": ["cmd"]}},
                          {"type": "function", "name": "spawn_agent", "parameters": {"properties": {"message": {}}, "required": ["message"]}}]}

    def test_model_transition_requires_real_read_and_tool_receipts(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        model = self.model(receipts)
        body = self.request()
        item, _, hold = model.respond(body)
        self.assertEqual(item["name"], "exec_command")
        self.assertEqual(item["call_id"], "tempo-read")
        self.assertIsNone(hold)
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)
        body["input"].append({"type": "function_call_output", "call_id": "tempo-read", "output": smoke.READ_RESULT})
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)
        receipts.extend([self.receipt("PreToolUse", tool_id="tempo-read"), self.receipt("PostToolUse", tool_id="tempo-read")])
        item, _, _ = model.respond(body)
        self.assertEqual(item["name"], "spawn_agent")
        self.assertEqual(len(model.requests), 2)

    def test_child_request_requires_real_child_barrier_and_is_bounded(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        model = self.model(receipts)
        with self.assertRaises(smoke.FixtureFailure): model.respond(self.request(smoke.CHILD_PROMPT))
        receipts.append(self.receipt("SubagentStart", agent_id="child-1"))
        _, _, hold = model.respond(self.request(smoke.CHILD_PROMPT))
        self.assertEqual(hold, "child")
        self.assertEqual(model.child, "child-1")
        with self.assertRaises(smoke.FixtureFailure): model.respond(self.request(smoke.CHILD_PROMPT))

    def test_provider_cannot_use_unadvertised_or_wrong_tools(self):
        model = self.model([self.receipt("SessionStart"), self.receipt("UserPromptSubmit")])
        body = self.request()
        body["tools"] = []
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)
        body = self.request()
        body["tools"][0]["parameters"]["required"].append("unavailable_parameter")
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)
        self.assertEqual(model.requests, [])

    def test_sse_completes_after_real_function_item(self):
        events = smoke.sse_events("response-1", {"type": "function_call", "call_id": "call-1"})
        self.assertEqual([v["type"] for v in events], ["response.created", "response.output_item.done", "response.completed"])
        self.assertEqual(events[0]["response"]["id"], events[-1]["response"]["id"])

    def test_exact_pinned_ui_labels_are_accepted(self):
        screen = "SessionStart hooks\n› [!] Hook 1 · new\nEvent     SessionStart\nSource    User config - ~/.codex/hooks.json\nCommand   /tmp/tempo hook codex --input-stdin\nMode      Sync\nTimeout   2s\nTrust     New hook - review required\nt trust · esc back"
        smoke.review_hook_screen(screen, "SessionStart", "/tmp/tempo hook codex --input-stdin", "~/.codex/hooks.json")

    def test_cleanup_failure_cannot_preserve_a_passed_result(self):
        def cleanup_failure(_args, report):
            report["status"] = "passed"
            raise smoke.FixtureFailure("cleanup_failed")
        with tempfile.TemporaryDirectory() as tmp:
            evidence = Path(tmp) / "evidence.json"
            with mock.patch.object(smoke, "run", cleanup_failure), mock.patch.object(sys, "argv", ["fixture", "--tempo", "unused", "--helper", "unused", "--evidence", str(evidence)]), mock.patch("builtins.print"):
                self.assertEqual(smoke.main(), 1)
            self.assertEqual(json.loads(evidence.read_text())["status"], "failed")

    def test_helper_output_is_bounded_while_the_process_is_running(self):
        started = time.monotonic()
        with self.assertRaises(smoke.FixtureFailure):
            smoke.bounded_run([sys.executable, "-c", "import os,time; os.write(1,b'x'*2097152); time.sleep(30)"],
                              dict(os.environ), Path.cwd(), timeout=3)
        self.assertLess(time.monotonic() - started, 2)

    def test_child_identity_is_crosschecked_in_both_request_arrival_orders(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit"),
                    self.receipt("SubagentStart", agent_id="child-actual", turn_id="child-turn")]
        model = self.model(receipts)
        model.child = "child-from-spawn-result"
        with self.assertRaises(smoke.FixtureFailure): model.respond(self.request(smoke.CHILD_PROMPT))
        self.assertEqual(model.child, "child-from-spawn-result")
        model = self.model(receipts)
        model.phase, model.turn = "spawn", "turn-1"
        model.respond(self.request(smoke.CHILD_PROMPT))
        body = self.request()
        body["input"].append({"type": "function_call_output", "call_id": "tempo-spawn", "output": json.dumps({"agent_id": "different"})})
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)

    def test_catchable_cancellation_uses_failed_evidence_path(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            def cancel_run(_args, report):
                report["status"] = "passed"
                handler = signal.getsignal(signum)
                self.assertTrue(callable(handler))
                handler(signum, None)
            with tempfile.TemporaryDirectory() as tmp:
                evidence = Path(tmp) / "evidence.json"
                with mock.patch.object(smoke, "run", cancel_run), mock.patch.object(sys, "argv", ["fixture", "--tempo", "unused", "--helper", "unused", "--evidence", str(evidence)]), mock.patch("builtins.print"):
                    self.assertEqual(smoke.main(), 1)
                self.assertEqual(json.loads(evidence.read_text())["failure_category"], "fixture_cancelled")


if __name__ == "__main__":
    unittest.main()
