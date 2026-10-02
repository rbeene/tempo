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
    def test_shared_host_arguments_require_embedded_execution_without_bypasses(self):
        self.assertEqual(smoke.codex_argv(Path("/synthetic/inert-runtime")),
                         ["/synthetic/inert-runtime", "--no-alt-screen", "--no-daemon"])

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

    def test_inventory_waits_for_complete_validated_row_before_navigation(self):
        class ReachedNextRow(Exception): pass
        class Terminal:
            def __init__(self, rows):
                self.screens = iter(["gpt-6.1-sol", "Lifecycle hooks from config and enabled plugins.", *rows])
                self.sent = []
            def until(self, predicate, category, **_):
                for screen in self.screens:
                    if predicate(screen): return screen
                raise smoke.FixtureFailure(category)
            def command(self, value): self.asserted_command = value
            def send(self, value):
                self.sent.append(value)
                if value == b"\x1b[B": raise ReachedNextRow()
        with mock.patch.object(smoke, "EVENT_ORDER", ("PreToolUse",)):
            good = Terminal(["› PreToolUse ", "› PreToolUse 1 0 1 \n"])
            with self.assertRaises(ReachedNextRow):
                smoke.normal_trust(good, Path("/tmp/project"), "unused", [])
            self.assertEqual(good.sent, [b"\x1b[H", b"\x1b[B"])
            for row in ("› PreToolUse ", "› PreToolUse 2 0 2 \n", "› PreToolUse 1 0 1 \nIssues"):
                bad = Terminal([row])
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(bad, Path("/tmp/project"), "unused", [])
                self.assertEqual(bad.sent, [b"\x1b[H"])

    def test_details_wait_for_entire_validated_definition_before_trust(self):
        class ReachedTrust(Exception): pass
        command = "/tmp/native/tempo hook codex --input-stdin"
        full = "PreToolUse hooks\n› [!] Hook 1 · new\nEvent: PreToolUse\nSource: User - ~/.codex/hooks.json\nCommand: " + command + "\nMode: Sync\nTimeout: 2s\nTrust: New hook - review required\nt trust · esc back"
        class Terminal:
            def __init__(self, details):
                self.screens = iter(["gpt-6.1-sol", "Lifecycle hooks from config and enabled plugins.",
                                     "› PreToolUse 1 0 1 \n", "› PreToolUse 1 0 1 \n", *details])
                self.sent = []
            def until(self, predicate, category, **_):
                for screen in self.screens:
                    if predicate(screen): return screen
                raise smoke.FixtureFailure(category)
            def command(self, _): pass
            def send(self, value):
                self.sent.append(value)
                if value == b"t": raise ReachedTrust()
        with mock.patch.object(smoke, "EVENT_ORDER", ("PreToolUse",)):
            good = Terminal(["PreToolUse hooks\nTrust", full])
            with self.assertRaises(ReachedTrust):
                smoke.normal_trust(good, Path("/tmp/project"), command, [])
            self.assertEqual(good.sent[-1], b"t")
            for screen in ("PreToolUse hooks\nTrust", full.replace("Mode: Sync", "Mode: Async"), full.replace(command, "/tmp/other")):
                bad = Terminal([screen])
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(bad, Path("/tmp/project"), command, [])
                self.assertNotIn(b"t", bad.sent)
            unexpected = Terminal([full])
            with mock.patch.object(smoke, "review_hook_screen", side_effect=RuntimeError("synthetic")):
                with self.assertRaises(RuntimeError):
                    smoke.normal_trust(unexpected, Path("/tmp/project"), command, [])
            self.assertNotIn(b"t", unexpected.sent)

    def test_screen_redraw_does_not_leave_old_trust_evidence(self):
        screen = smoke.Screen(6, 80)
        screen.feed(b"\x1b[2J\x1b[HCommand: expected\r\nTrust: New hook - review required")
        self.assertIn("Command: expected", screen.text())
        screen.feed(b"\x1b[H\x1b[2JCommand: unexpected")
        self.assertNotIn("Trust:", screen.text())
        self.assertNotIn("Command: expected", screen.text())

    def test_pinned_keyboard_negotiation_is_nonprinting_and_fragment_safe(self):
        # Exact forms emitted by rust-v0.159.3 terminal_probe.rs and
        # tui/keyboard_modes.rs: query, push flags, pop/reset flags.
        screen = smoke.Screen(6, 80)
        screen.feed(b"visible trust evidence")
        before = screen.text()
        for control in (b"\x1b[?u", b"\x1b[>5u", b"\x1b[>7u", b"\x1b[<1u", b"\x1b[<u"):
            for byte in control:
                screen.feed(bytes([byte]))
            self.assertEqual(screen.text(), before)
        screen.feed(b"\x1b[s!\x1b[u?")
        self.assertIn("visible trust evidence?", screen.text())
        self.assertNotIn("!", screen.text())

    def test_unrecognized_private_cursor_control_fails_with_safe_category(self):
        with self.assertRaisesRegex(smoke.FixtureFailure, "unsupported_terminal_parameters"):
            smoke.Screen().feed(b"\x1b[?1H")

    def test_workspace_trust_waits_for_complete_exact_prompt_before_enter(self):
        class ReachedHooks(Exception):
            pass
        class Terminal:
            def __init__(self, path):
                self.screens = iter(["Trust this folder?", "Trust this folder?\n" + path + "\nTrust and continue\nQuit", "gpt-6.1-sol"])
                self.sent = []
            def until(self, predicate, category, **_):
                for screen in self.screens:
                    if predicate(screen): return screen
                raise smoke.FixtureFailure(category)
            def send(self, data): self.sent.append(data)
            def command(self, value):
                self.asserted_command = value
                raise ReachedHooks()
        terminal = Terminal("/tmp/exact-project")
        with self.assertRaises(ReachedHooks):
            smoke.normal_trust(terminal, Path("/tmp/exact-project"), "unused", [])
        self.assertEqual(terminal.sent, [b"\r"])
        self.assertEqual(terminal.asserted_command, "/hooks")
        for path in ("/tmp/different-project", "/tmp/exact-project-other"):
            wrong = Terminal(path)
            with self.assertRaisesRegex(smoke.FixtureFailure, "workspace_trust_mismatch"):
                smoke.normal_trust(wrong, Path("/tmp/exact-project"), "unused", [])
            self.assertEqual(wrong.sent, [])

    def test_workspace_trust_rejects_near_prefix_folder_without_enter(self):
        class Terminal:
            def __init__(self, path_display):
                self.sent = []
                self.screen = "Trust this folder?\n" + path_display + "\nTrust and continue\nQuit"
            def until(self, predicate, category, **_):
                if predicate(self.screen): return self.screen
                raise smoke.FixtureFailure(category)
            def send(self, data): self.sent.append(data)
        for path_display in ("/tmp/exact-project-other", "/tmp/exact-project\nTrusting will apply to the repository root:\n/tmp/different-root"):
            with self.subTest(path_display=path_display):
                terminal = Terminal(path_display)
                with self.assertRaisesRegex(smoke.FixtureFailure, "workspace_trust_mismatch"):
                    smoke.normal_trust(terminal, Path("/tmp/exact-project"), "unused", [])
                self.assertEqual(terminal.sent, [])

    def test_workspace_probe_exports_only_fixed_booleans_and_bounded_count(self):
        screen = "Trust this folder?\n/tmp/exact-project-other\nTrust and continue\nQuit\nPRIVATE-SCREEN-TEXT\n" + "secret\n" * 100
        probe = smoke.workspace_trust_probe(screen, Path("/tmp/exact-project"))
        self.assertEqual(probe, {"title_present": True, "exact_path_line_present": False,
                                "path_substring_present": True, "trust_choice_present": True,
                                "quit_choice_present": True, "alternate_root_warning_present": False,
                                "nonempty_rows": 80})
        self.assertNotIn("PRIVATE", json.dumps(probe))
        self.assertNotIn("/tmp", json.dumps(probe))
        complete = smoke.workspace_trust_probe("Trust this folder?\n /tmp/exact-project \nTrust and continue\nQuit\nTrusting will apply to the repository root:", Path("/tmp/exact-project"))
        self.assertTrue(complete["exact_path_line_present"])
        self.assertTrue(complete["alternate_root_warning_present"])

    def test_workspace_probe_is_initialized_and_retains_last_timeout_observation(self):
        class Terminal:
            def __init__(self, screens): self.screens, self.sent = iter(screens), []
            def until(self, predicate, category, **_):
                for screen in self.screens:
                    if predicate(screen): return screen
                raise smoke.FixtureFailure(category)
            def send(self, data): self.sent.append(data)
        for screens, title, rows in (([], False, 0), (["Trust this folder?", "Trust this folder?\nPRIVATE"], True, 2),
                                    (["Trust this folder?", "Trust this folder?\n/tmp/exact-project-other\nTrust and continue\nQuit", ""], False, 0)):
            with self.subTest(screens=screens):
                evidence = {}
                terminal = Terminal(screens)
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(terminal, Path("/tmp/exact-project"), "unused", [], evidence)
                self.assertEqual(terminal.sent, [])
                self.assertEqual(evidence["title_present"], title)
                self.assertEqual(evidence["nonempty_rows"], rows)
                self.assertFalse(evidence["exact_path_line_present"])
                if rows == 0:
                    self.assertFalse(any(evidence.values()))
                self.assertNotIn("PRIVATE", json.dumps(evidence))

    def test_normal_startup_review_requires_complete_nine_hook_prompt(self):
        class ReachedInventory(Exception): pass
        title = "Hooks need review"
        choices = "\n› 1. Review hooks\n2. Trust all and continue\n3. Continue without trusting (hooks won't run)"
        full = title + "\n9 hooks are new or changed." + choices
        class Terminal:
            def __init__(self, review):
                self.screens = iter(["Trust this folder?", "Trust this folder?\n/tmp/exact-project\nTrust and continue\nQuit", *review])
                self.sent = []
            def until(self, predicate, category, **_):
                if category == "hook_inventory_navigation": raise ReachedInventory()
                for screen in self.screens:
                    if predicate(screen): return screen
                raise smoke.FixtureFailure(category)
            def send(self, value): self.sent.append(value)
            def command(self, _): raise AssertionError("must not type /hooks into startup modal")
        good = Terminal([title, full, "Lifecycle hooks from config and enabled plugins."])
        with self.assertRaises(ReachedInventory):
            smoke.normal_trust(good, Path("/tmp/exact-project"), "unused", [])
        self.assertEqual(good.sent, [b"\r", b"1", b"\x1b[H"])
        for invalid in (title, full.replace("9 hooks", "19 hooks"), full.replace("9 hooks", "8 hooks"), full.replace("2. Trust all and continue", "")):
            with self.subTest(invalid=invalid):
                bad = Terminal([invalid + "\ngpt-6.1-sol"])
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(bad, Path("/tmp/exact-project"), "unused", [])
                self.assertEqual(bad.sent, [b"\r"])
        cleared = Terminal([title + "\n19 hooks are new or changed.", ""])
        evidence = {"untrusted_old_text": "SECRET"}
        with self.assertRaises(smoke.FixtureFailure):
            smoke.normal_trust(cleared, Path("/tmp/exact-project"), "unused", [], {}, evidence)
        self.assertEqual(cleared.sent, [b"\r"])
        self.assertEqual(evidence, smoke.startup_trust_probe(""))

    def test_startup_probe_has_only_fixed_booleans_and_exact_count(self):
        probe = smoke.startup_trust_probe("Hooks need review\n19 hooks are new or changed.\n1. Review hooks\nSECRET /private/path")
        self.assertEqual(probe, {"workspace_title_present": False, "model_label_present": False,
                                "hooks_review_title_present": True, "exact_count_present": False,
                                "review_choice_present": True, "trust_all_choice_present": False,
                                "continue_choice_present": False})
        self.assertFalse(any(smoke.startup_trust_probe("").values()))

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
