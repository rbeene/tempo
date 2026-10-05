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
import tomllib

spec = importlib.util.spec_from_file_location("codex_smoke", Path(__file__).with_name("codex_smoke.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


def workspace_frames(path="/tmp/exact-project"):
    first = "Trust this folder?\n" + path + "\n› 1. Trust and continue\n2. Quit"
    last = first.replace("› 1.", "1.").replace("2. Quit", "› 2. Quit")
    return [first, last, first]


def browser_frames():
    caption = "Lifecycle hooks from config and enabled plugins.\n"
    return [caption + "› PreToolUse 1 0 1 \n", caption + "› Interrupt 1 0 1 \n"]


class HarnessTests(unittest.TestCase):
    def test_fixture_provider_config_leaves_tooltip_to_installer_without_trust_seeding(self):
        config = tomllib.loads(smoke.config_text(43210))
        self.assertNotIn("tui", config)
        self.assertEqual(config.get("tools"), {"update_plan": {"enabled": True}})
        self.assertNotIn("projects", config)
        self.assertNotIn("hooks", config)
        self.assertEqual(config["model"], "tempo-ci-fixture")
        self.assertNotIn("default_subagent_model", config["agents"])
        self.assertFalse(config["features"]["code_mode"])
        self.assertFalse(config["features"]["code_mode_only"])
        self.assertEqual(config["model_providers"]["tempo_ci"]["base_url"], "http://127.0.0.1:43210/codex/v1")

    def test_command_pastes_then_waits_for_cursor_local_echo_before_single_enter(self):
        for value in ("/quit", "/hooks", smoke.PARENT_PROMPT, smoke.INTERRUPT_PROMPT):
            for glyph in ("›", "»"):
                with self.subTest(value=value, glyph=glyph):
                    terminal = smoke.Terminal.__new__(smoke.Terminal)
                    terminal.input_probe = {}
                    terminal.send = mock.Mock()
                    row = "  " + glyph + " " + value
                    frames = [("tempo-ci-fixture\n" + row + "\n› empty", 2, 7),
                              ("tempo-ci-fixture\n" + row[:-1], 1, len(row)-1),
                              ("tempo-ci-fixture\n" + row, 1, 0),
                              ("tempo-ci-fixture\n" + row, 1, len(row))]
                    def until(predicate, category, seconds):
                        self.assertEqual(seconds, 5)
                        self.assertEqual(terminal.send.call_args_list,
                                         [mock.call(b"\x1b[200~" + value.encode() + b"\x1b[201~")])
                        for index, (text, cursor_row, cursor_col) in enumerate(frames):
                            terminal.screen = smoke.Screen()
                            terminal.screen.feed(text.replace("\n", "\r\n").encode())
                            terminal.screen.row, terminal.screen.col = cursor_row, cursor_col
                            ready = predicate(terminal.screen.text())
                            self.assertEqual(ready, index == len(frames)-1)
                            if ready: return text
                        raise smoke.FixtureFailure(category)
                    terminal.until = until
                    terminal.command(value)
                    self.assertEqual(terminal.send.call_args_list,
                                     [mock.call(b"\x1b[200~" + value.encode() + b"\x1b[201~"), mock.call(b"\r")])
                    self.assertTrue(terminal.input_probe["enter_sent"])
                    self.assertTrue(terminal.input_probe["cursor_at_echo_end"])

    def test_command_accepts_actual_composer_echo_without_raw_model_slug(self):
        for text, row in (("› /quit", 0), ("OpenAI Codex\nmodel: GPT-6.1\n› /quit", 2)):
            terminal = smoke.Terminal.__new__(smoke.Terminal)
            terminal.input_probe = {}
            terminal.send = mock.Mock()
            def until(predicate, category, seconds):
                terminal.screen = smoke.Screen()
                terminal.screen.feed(text.replace("\n", "\r\n").encode())
                terminal.screen.row, terminal.screen.col = row, 7
                self.assertTrue(predicate(terminal.screen.text()))
                return text
            terminal.until = until
            terminal.command("/quit")
            self.assertEqual(terminal.send.call_args_list, [mock.call(b"\x1b[200~/quit\x1b[201~"), mock.call(b"\r")])
            self.assertFalse(terminal.input_probe["model_label_present"])
            self.assertTrue(terminal.input_probe["cursor_row_exact_echo"])
            self.assertTrue(terminal.input_probe["cursor_at_echo_end"])
            self.assertTrue(terminal.input_probe["enter_sent"])

    def test_command_never_submits_partial_historical_popup_or_modal_echo(self):
        value = "/quit"
        cases = [("tempo-ci-fixture\n› /qui", 1, 6),
                 ("tempo-ci-fixture\n› /quit\n› empty", 2, 7),
                 ("tempo-ci-fixture\n› /quit  exit Codex", 1, 19),
                 ("tempo-ci-fixture\n› /quit", 1, 0)]
        for title in ("Trust this folder?", "Hooks need review", "Lifecycle hooks from config and enabled plugins.", "Stop hooks"):
            cases.append(("tempo-ci-fixture\n" + title + "\n› /quit", 2, 7))
        for text, row, col in cases:
            with self.subTest(text=text):
                terminal = smoke.Terminal.__new__(smoke.Terminal)
                terminal.input_probe = {"secret": "CANARY"}
                terminal.send = mock.Mock()
                def until(predicate, category, seconds):
                    terminal.screen = smoke.Screen()
                    terminal.screen.feed((text + "\r\nCANARY").replace("\n", "\r\n").encode())
                    terminal.screen.row, terminal.screen.col = row, col
                    self.assertFalse(predicate(terminal.screen.text()))
                    raise smoke.FixtureFailure(category)
                terminal.until = until
                with self.assertRaisesRegex(smoke.FixtureFailure, "command_echo_unavailable"):
                    terminal.command(value)
                self.assertEqual(terminal.send.call_args_list, [mock.call(b"\x1b[200~" + value.encode() + b"\x1b[201~")])
                self.assertEqual(set(terminal.input_probe), {"is_quit", "model_label_present", "modal_present",
                    "cursor_row_exact_echo", "cursor_at_echo_end", "enter_sent"})
                self.assertTrue(all(type(v) is bool for v in terminal.input_probe.values()))
                self.assertNotIn("CANARY", json.dumps(terminal.input_probe))
                self.assertFalse(terminal.input_probe["enter_sent"])

    def test_diagnostic_command_preserves_stdin_stdout_exit_and_exact_append_only_stderr(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log, tempo = root / "hook-errors", root / "tempo"
            command = smoke.prepare_diagnostic_command(tempo, log)
            identity = log.stat()
            self.assertEqual(command, str(tempo) + " hook codex --input-stdin 2>> " + str(log))
            self.assertEqual(log.stat().st_mode & 0o777, 0o600)
            tempo.write_text('#!/bin/sh\n[ "$*" = "hook codex --input-stdin" ] || exit 2\n[ "$(cat)" = "synthetic-input" ] || exit 3\nprintf \'{}\\n\'\nprintf \'tempo hook: state_busy; durability=not_committed\\n\' >&2\n')
            tempo.chmod(0o700)
            for _ in range(2):
                result = subprocess.run(["/bin/sh", "-c", command], input=b"synthetic-input", capture_output=True, timeout=2)
                self.assertEqual((result.returncode, result.stdout, result.stderr), (0, b"{}\n", b""))
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), {"status": "available", "counts": [
                {"code": "state_busy", "durability": "not_committed", "count": 2}]})
            smoke.reset_hook_diagnostics(log, identity)
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), {"status": "available", "counts": []})
            result = subprocess.run(["/bin/sh", "-c", command], input=b"synthetic-input", capture_output=True, timeout=2)
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity)["counts"][0]["count"], 1)
            with self.assertRaises(smoke.FixtureFailure): smoke.prepare_diagnostic_command(tempo, log)
            self.assertEqual(log.read_bytes().count(b"tempo hook:"), 1)
            tempo.write_text(tempo.read_text() + "exit 7\n")
            failed = subprocess.run(["/bin/sh", "-c", command], input=b"synthetic-input", capture_output=True, timeout=2)
            self.assertEqual((failed.returncode, failed.stdout, failed.stderr), (7, b"{}\n", b""))
            for bad in (root / "bad;name", root / "bad name"):
                with self.assertRaises(smoke.FixtureFailure): smoke.prepare_diagnostic_command(bad, root / "unused")
                self.assertFalse((root / "unused").exists())

    def test_hook_diagnostics_reject_unknown_partial_oversized_or_unsafe_files_without_text(self):
        unavailable = {"status": "unavailable", "counts": []}
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "hook-errors"
            smoke.prepare_diagnostic_command(Path(tmp) / "tempo", log)
            identity = log.stat()
            held = log.open("rb")  # Keep inode allocated while testing replacements.
            self.addCleanup(held.close)
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), {"status": "available", "counts": []})
            good = b"tempo hook: validation; durability=not_committed\n"
            for data in (good + b"CANARY\n", good[:-1], b"prefix " + good, good.replace(b"validation", b"CANARY"),
                         good.replace(b"not_committed", b"CANARY"), b"\xff\n", good * 129, b"x" * 16385):
                log.write_bytes(data)
                result = smoke.hook_diagnostic_probe(log, identity)
                self.assertEqual(result, unavailable)
                self.assertNotIn("CANARY", json.dumps(result))
            log.write_bytes(good)
            log.chmod(0o644)
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), unavailable)
            log.unlink()
            target = Path(tmp) / "private"
            target.write_text("CANARY")
            log.symlink_to(target)
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), unavailable)
            self.assertEqual(target.read_text(), "CANARY")
            log.unlink()
            log.write_text("replacement-CANARY")
            log.chmod(0o600)
            with self.assertRaises(smoke.FixtureFailure): smoke.reset_hook_diagnostics(log, identity)
            self.assertEqual(log.read_text(), "replacement-CANARY")
            log.unlink(); log.mkdir()
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), unavailable)
            log.rmdir()
            self.assertEqual(smoke.hook_diagnostic_probe(log, identity), unavailable)

    def test_artifact_drift_probe_exports_only_fixed_match_booleans(self):
        with tempfile.TemporaryDirectory() as tmp:
            baseline = {}
            for role in ("runtime", "executable", "definitions", "configuration"):
                path = Path(tmp) / role
                path.write_text("synthetic " + role)
                baseline[role] = (path, smoke.digest(path))
            result = smoke.artifact_match_probe(baseline)
            self.assertEqual(result, {"available": True, "runtime_matches": True, "executable_matches": True,
                                     "definitions_matches": True, "configuration_matches": True})
            baseline["configuration"][0].write_text("CANARY")
            result = smoke.artifact_match_probe(baseline)
            self.assertTrue(result["available"])
            self.assertFalse(result["configuration_matches"])
            self.assertNotIn("CANARY", json.dumps(result))
            runtime = baseline["runtime"][0]
            original_fstat = os.fstat
            calls = 0
            def replace_after_read(fd):
                nonlocal calls
                info = original_fstat(fd)
                calls += 1
                if calls == 2:
                    runtime.rename(Path(tmp) / "old-runtime")
                    runtime.write_text("replacement-CANARY")
                return info
            with mock.patch.object(smoke.os, "fstat", side_effect=replace_after_read):
                replaced = smoke.artifact_match_probe(baseline)
            self.assertFalse(replaced["available"])
            self.assertFalse(replaced["runtime_matches"])
            self.assertNotIn("CANARY", json.dumps(replaced))
            with runtime.open("wb") as oversized: oversized.truncate((320 << 20) + 1)
            self.assertFalse(smoke.artifact_match_probe(baseline)["available"])
            runtime.unlink()
            runtime.symlink_to(baseline["configuration"][0])
            self.assertFalse(smoke.artifact_match_probe(baseline)["available"])
            runtime.unlink()
            self.assertFalse(smoke.artifact_match_probe(baseline)["available"])
            self.assertTrue(all(type(v) is bool for v in smoke.artifact_match_probe(baseline).values()))

    def test_provider_entry_count_includes_rejection_and_is_bounded(self):
        model = self.model([])
        model.session = None
        for _ in range(20):
            with self.assertRaises(smoke.FixtureFailure): model.respond(self.request())
        self.assertEqual(model.entry_count, 16)
        self.assertEqual(model.requests, [])
        self.assertEqual(model.counts, {})

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
                                      "TEMPO_STATE", "TEMPO_HOOK_STATE", "TEMPO_CI_PROVIDER_TOKEN", "TEMPO_HOOK_DIAGNOSTICS"})

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
                self.screens = iter(["tempo-ci-fixture", *browser_frames(), *rows])
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
            self.assertEqual(good.sent, [b"\x1b[F", b"\x1b[H", b"\x1b[B"])
            for row in ("› PreToolUse ", "› PreToolUse 2 0 2 \n", "› PreToolUse 1 0 1 \nIssues"):
                bad = Terminal([row])
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(bad, Path("/tmp/project"), "unused", [])
                self.assertEqual(bad.sent, [b"\x1b[F", b"\x1b[H"])

    def test_details_wait_for_entire_validated_definition_before_trust(self):
        class ReachedTrust(Exception): pass
        command = "/tmp/native/tempo hook codex --input-stdin"
        full = "PreToolUse hooks\n› [!] Hook 1 · new\nEvent: PreToolUse\nSource: User - ~/.codex/hooks.json\nCommand: " + command + "\nMode: Sync\nTimeout: 2s\nTrust: New hook - review required\nt trust · esc back"
        class Terminal:
            def __init__(self, details):
                self.screens = iter(["tempo-ci-fixture", *browser_frames(),
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

    def test_pinned_scroll_region_and_reverse_index_preserve_surrounding_rows(self):
        screen = smoke.Screen(6, 12)
        for row, label in enumerate("ABCDEF", 1): screen.feed(("\x1b[" + str(row) + ";1H" + label).encode())
        for byte in b"\x1b[2;5r": screen.feed(bytes([byte]))
        self.assertEqual((screen.row, screen.col), (0, 0))
        screen.feed(b"\x1b[2;1H\x1bM")
        self.assertEqual(screen.text().splitlines(), ["A", "", "B", "C", "D", "F"])
        self.assertEqual((screen.row, screen.col), (1, 0))
        screen.feed(b"\x1b[r\x1b[6;1H\n")
        self.assertEqual(screen.text().split("\n"), ["", "B", "C", "D", "F", ""])

    def test_linefeed_and_scroll_controls_obey_active_margins(self):
        for control, expected in ((b"\n", ["A", "C", "D", "E", "", "F"]),
                                  (b"\x1b[S", ["A", "C", "D", "E", "", "F"]),
                                  (b"\x1b[T", ["A", "", "B", "C", "D", "F"])):
            with self.subTest(control=control):
                screen = smoke.Screen(6, 12)
                for row, label in enumerate("ABCDEF", 1): screen.feed(("\x1b[" + str(row) + ";1H" + label).encode())
                screen.feed(b"\x1b[2;5r\x1b[5;1H" + control)
                self.assertEqual(screen.text().splitlines(), expected)

    def test_scroll_region_bounds_defaults_and_reverse_index_outside_margin(self):
        for control in (b"\x1b[5;2r", b"\x1b[2;7r", b"\x1b[3;3r", b"\x1b[1;2;3r"):
            with self.subTest(control=control), self.assertRaisesRegex(smoke.FixtureFailure, "^unsupported_terminal_scroll_region$"):
                smoke.Screen(6, 12).feed(control)
        screen = smoke.Screen(6, 12)
        screen.feed(b"\x1b[1;1HA\x1b[6;1HF\x1b[2;5r\x1b[6;1H\x1bM")
        self.assertEqual((screen.row, screen.col), (4, 0))
        self.assertEqual(screen.text().splitlines(), ["A", "", "", "", "", "F"])
        screen.feed(b"\x1b[999S")
        self.assertEqual(screen.text().splitlines(), ["A", "", "", "", "", "F"])
        screen.feed(b"\x1b[0;0r")
        self.assertEqual((screen.scroll_top, screen.scroll_bottom, screen.row, screen.col), (0, 5, 0, 0))

    def test_different_csi_intermediate_cannot_change_scroll_margins(self):
        screen = smoke.Screen(6, 12)
        screen.feed(b"\x1b[3;4Hknown")
        before = (screen.scroll_top, screen.scroll_bottom, screen.row, screen.col, screen.text())
        with self.assertRaisesRegex(smoke.FixtureFailure, "^unsupported_terminal_scroll_region$"):
            screen.feed(b"\x1b[2;5$r")
        self.assertEqual((screen.scroll_top, screen.scroll_bottom, screen.row, screen.col, screen.text()), before)

    def test_unsupported_control_diagnostic_is_fixed_family_and_operation_only(self):
        for control, category in ((b"\x1b(BPRIVATE", "unsupported_terminal_escape_character_set"),
                                  (b"\x1b[123LPRIVATE", "unsupported_terminal_csi_insert_lines"),
                                  (b"\x1b[123zPRIVATE", "unsupported_terminal_csi_other")):
            with self.subTest(control=control), self.assertRaisesRegex(smoke.FixtureFailure, "^" + category + "$"):
                smoke.Screen().feed(control)

    def test_workspace_trust_waits_for_complete_exact_prompt_before_enter(self):
        class ReachedHooks(Exception):
            pass
        class Terminal:
            def __init__(self, path):
                self.screens = iter(["Trust this folder?", workspace_frames(path)[0].replace("› ", ""), *workspace_frames(path), "tempo-ci-fixture"])
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
        self.assertEqual(terminal.sent, [b"\x1b[B", b"\x1b[A", b"\r"])
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
        full = title + "\n12 hooks are new or changed." + choices
        class Terminal:
            def __init__(self, review):
                self.screens = iter(["Trust this folder?", *workspace_frames(), *review])
                self.sent = []
            def until(self, predicate, category, **_):
                if category == "hook_inventory_navigation": raise ReachedInventory()
                for screen in self.screens:
                    if predicate(screen): return screen
                raise smoke.FixtureFailure(category)
            def send(self, value): self.sent.append(value)
            def command(self, _): raise AssertionError("must not type /hooks into startup modal")
        last = full.replace("› 1.", "1.").replace("3. Continue", "› 3. Continue")
        good = Terminal([title, full.replace("› ", ""), full, last, *browser_frames()])
        with self.assertRaises(ReachedInventory):
            smoke.normal_trust(good, Path("/tmp/exact-project"), "unused", [])
        self.assertEqual(good.sent, [b"\x1b[B", b"\x1b[A", b"\r", b"\x1b[F", b"1", b"\x1b[F", b"\x1b[H"])
        for invalid in (title, full.replace("12 hooks", "19 hooks"), full.replace("12 hooks", "8 hooks"), full.replace("2. Trust all and continue", ""),
                        full.replace("› ", ""), full + "\n› 3. Continue without trusting (hooks won't run)"):
            with self.subTest(invalid=invalid):
                bad = Terminal([invalid + "\ntempo-ci-fixture"])
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(bad, Path("/tmp/exact-project"), "unused", [])
                self.assertEqual(bad.sent, [b"\x1b[B", b"\x1b[A", b"\r"])
        cleared = Terminal([title + "\n19 hooks are new or changed.", ""])
        evidence = {"untrusted_old_text": "SECRET"}
        with self.assertRaises(smoke.FixtureFailure):
            smoke.normal_trust(cleared, Path("/tmp/exact-project"), "unused", [], {}, evidence)
        self.assertEqual(cleared.sent, [b"\x1b[B", b"\x1b[A", b"\r"])
        self.assertEqual(evidence, smoke.startup_trust_probe(""))

    def test_startup_probe_has_only_fixed_booleans_and_exact_count(self):
        probe = smoke.startup_trust_probe("Hooks need review\n19 hooks are new or changed.\n1. Review hooks\nSECRET /private/path")
        self.assertEqual(probe, {"workspace_title_present": False, "model_label_present": False,
                                "hooks_review_title_present": True, "exact_count_present": False,
                                "review_choice_present": True, "trust_all_choice_present": False,
                                "continue_choice_present": False})
        self.assertFalse(any(smoke.startup_trust_probe("").values()))

    def test_review_navigation_acknowledges_input_before_single_review_shortcut(self):
        prompt = "Hooks need review\n12 hooks are new or changed.\n› 1. Review hooks\n2. Trust all and continue\n3. Continue without trusting (hooks won't run)"
        acknowledged = prompt.replace("› 1.", "1.").replace("3. Continue", "› 3. Continue")
        class Terminal:
            def __init__(self, outcomes): self.outcomes, self.sent = iter(outcomes), []
            def send(self, value): self.sent.append(value)
            def until(self, predicate, category, **_):
                if predicate(next(self.outcomes)): return "unused"
                raise smoke.FixtureFailure(category)
        # First idempotent navigation is consumed by the pinned post-draw drain.
        good = Terminal([prompt, acknowledged])
        evidence = {}
        smoke.review_input_ack(good, prompt, evidence)
        self.assertEqual(good.sent, [b"\x1b[F", b"\x1b[F", b"1"])
        self.assertTrue(evidence["selected_continue_row"])
        for outcomes, sends in (([prompt, prompt], 2), (["tempo-ci-fixture"], 1), ([acknowledged + "\n› 1. Review hooks"], 1)):
            bad = Terminal(outcomes)
            with self.assertRaises(smoke.FixtureFailure): smoke.review_input_ack(bad, prompt, {})
            self.assertEqual(bad.sent, [b"\x1b[F"] * sends)

    def test_review_transition_projection_is_finite_and_replaces_unknown_text(self):
        probe = smoke.review_transition_probe("PRIVATE /path\ntempo-ci-fixture")
        self.assertEqual(probe, {"complete_startup_modal": False, "selected_review_row": False,
                                "selected_continue_row": False, "inventory_caption_present": False,
                                "inventory_first_row": False, "inventory_last_row": False,
                                "model_label_present": True, "workspace_title_present": False})

    def test_normal_trust_requires_ack_at_each_initial_input_boundary(self):
        class ReachedInventory(Exception): pass
        review = "Hooks need review\n12 hooks are new or changed.\n› 1. Review hooks\n2. Trust all and continue\n3. Continue without trusting (hooks won't run)"
        review_last = review.replace("› 1.", "1.").replace("3. Continue", "› 3. Continue")
        class Terminal:
            def __init__(self, start, depart=False):
                self.phase = start if start != "browser" else "composer"
                self.screen = {"workspace": workspace_frames()[0], "startup": review, "browser": "tempo-ci-fixture"}[start]
                self.start, self.depart, self.sent, self.counts = start, depart, [], {}
                self.workspace_ack = False
            def until(self, predicate, category, **_):
                if predicate(self.screen): return self.screen
                raise smoke.FixtureFailure(category)
            def command(self, value):
                assert self.phase == "composer" and value == "/hooks"
                self.phase, self.screen = "browser", browser_frames()[0]
            def send(self, key):
                self.sent.append(key)
                if key in (b"\x1b[B", b"\x1b[F"):
                    self.counts[self.phase] = self.counts.get(self.phase, 0) + 1
                    if self.depart and self.phase == self.start:
                        self.screen = "Unexpected private screen"
                        return
                    if self.counts[self.phase] == 1: return  # Actual post-draw drain.
                    self.screen = {"workspace": workspace_frames()[1], "startup": review_last, "browser": browser_frames()[1]}[self.phase]
                elif key == b"\x1b[A":
                    assert self.phase == "workspace" and self.screen == workspace_frames()[1]
                    self.screen, self.workspace_ack = workspace_frames()[0], True
                elif key == b"\r":
                    assert self.workspace_ack, "workspace confirmation before input acknowledgment"
                    self.phase, self.screen = "startup", review
                elif key == b"1":
                    assert self.screen == review_last, "Review shortcut before input acknowledgment"
                    self.phase, self.screen = "browser", browser_frames()[0]
                elif key == b"\x1b[H":
                    assert self.screen == browser_frames()[1], "inventory navigation before input acknowledgment"
                    raise ReachedInventory()
                else:
                    raise AssertionError("unexpected action")
        for start in ("workspace", "startup", "browser"):
            with self.subTest(start=start):
                good = Terminal(start)
                with self.assertRaises(ReachedInventory):
                    smoke.normal_trust(good, Path("/tmp/exact-project"), "unused", [])
                self.assertTrue(all(count == 2 for count in good.counts.values()))
                self.assertLessEqual(good.sent.count(b"\r"), 1)
                self.assertLessEqual(good.sent.count(b"1"), 1)
                bad = Terminal(start, depart=True)
                probes = {"old": "PRIVATE"}
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(bad, Path("/tmp/exact-project"), "unused", [], None, None, probes)
                self.assertEqual(len(bad.sent), 1)
                self.assertNotIn("PRIVATE", json.dumps(probes))

    def test_input_origin_timeout_preserves_last_finite_observation(self):
        review = "Hooks need review\n12 hooks are new or changed.\n1. Review hooks\n2. Trust all and continue\n3. Continue without trusting (hooks won't run)"
        class Terminal:
            def __init__(self, screen): self.screen, self.sent = screen, []
            def until(self, predicate, category, **_):
                if predicate(self.screen): return self.screen
                raise smoke.FixtureFailure(category)
            def send(self, value): self.sent.append(value)
        for stage, screen, expected in (("workspace", workspace_frames()[0].replace("› ", ""), smoke.workspace_input_probe(workspace_frames()[0].replace("› ", ""), Path("/tmp/exact-project"))),
                                        ("startup", review, smoke.review_transition_probe(review))):
            with self.subTest(stage=stage):
                terminal, probes = Terminal(screen), {}
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.normal_trust(terminal, Path("/tmp/exact-project"), "unused", [], None, None, probes)
                self.assertEqual(terminal.sent, [])
                self.assertEqual(probes[stage], expected)

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
                  "observed_at": "2026-10-02T10:00:00Z", "actor": "root-actor"}
        result.update(extra)
        return result

    def test_request_barrier_requires_actual_matching_prompt_and_session(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        smoke.require_prompt_barrier(receipts, "session-1", "turn-1")
        for bad in [receipts[:1], receipts[1:], [receipts[0], self.receipt("UserPromptSubmit", turn_id="old")],
                    [receipts[0], self.receipt("UserPromptSubmit", disposition="review_required")]]:
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_prompt_barrier(bad, "session-1", "turn-1")

    def test_tool_result_must_prove_exact_native_plan_before_spawn(self):
        good = {"input": [{"type": "function_call_output", "call_id": "tempo-plan", "output": "Plan updated"}]}
        smoke.require_plan_result(good)
        for bad in [{"input": []}, {"input": [{"type": "function_call", "call_id": "tempo-plan", "output": "Plan updated"}]},
                    {"input": [{"type": "function_call_output", "call_id": "wrong", "output": "Plan updated"}]},
                    {"input": good["input"] * 2},
                    {"input": [{"type": "function_call_output", "call_id": "tempo-plan", "output": "Not Plan updated"}]},
                    {"input": [{"type": "function_call_output", "call_id": "tempo-plan", "output": {"text": "Plan updated"}}]}]:
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_plan_result(bad)

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
        model.baseline = None
        model.initial_receipt_probe = {"status": "not_observed"}
        model.entry_count = 0
        model.capture_contexts = set()
        model.title_count = 0
        model.shutdown = threading.Event()
        model.phase, model.counts, model.requests = "initial", {}, []
        return model

    def request(self, marker=smoke.PARENT_PROMPT):
        return {"model": "tempo-ci-fixture", "stream": True, "input": [{"role": "user", "content": [{"text": marker}]}],
                "tools": [{"type": "function", "name": "update_plan", "parameters": {"properties": {"plan": {}, "explanation": {}}, "required": ["plan"]}},
                          {"type": "function", "name": "spawn_agent", "parameters": {"properties": {"message": {}}, "required": ["message"]}}]}

    def title_request(self):
        prompt = ("Generate a concise, single-line task title of at most 36 characters and under five words where possible. "
                  "Start with an imperative verb. Capitalize only the first word unless the user's language, proper nouns, acronyms, or code terms require otherwise. "
                  "Preserve ticket references exactly. Write in the user's language. Do not use quotes, markdown, or trailing punctuation. Do not answer the request."
                  "\n\nUser prompt:\n" + smoke.PARENT_PROMPT)
        return {"model": "tempo-ci-fixture", "stream": True, "tools": [],
                "input": [{"role": "user", "content": [{"type": "input_text", "text": prompt}]}],
                "text": {"format": {"type": "json_schema", "strict": True, "name": "codex_output_schema", "schema": {
                    "type": "object", "properties": {"title": {"type": "string", "minLength": 1, "maxLength": 36}},
                    "required": ["title"], "additionalProperties": False}}}}

    def test_auxiliary_title_does_not_advance_primary_lifecycle_in_either_order(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        for primary_first in (False, True):
            model = self.model(receipts)
            model.session, model.baseline = None, frozenset()
            if primary_first: model.respond(self.request())
            before = (model.session, model.turn, model.phase, copy.deepcopy(model.counts), list(model.requests))
            item, category, hold = model.respond(self.title_request())
            self.assertEqual((category, hold, item["type"]), ("title1", None, "message"))
            self.assertEqual(json.loads(item["content"][0]["text"]), {"title": "Verify native hooks"})
            self.assertEqual((model.session, model.turn, model.phase, model.counts, model.requests), before)
            self.assertEqual(model.title_count, 1)
            if not primary_first: self.assertEqual(model.respond(self.request())[0]["call_id"], "tempo-plan")
            with self.assertRaises(smoke.FixtureFailure): model.respond(self.title_request())
            self.assertEqual(model.title_count, 1)

    def test_auxiliary_title_rejects_unarmed_and_contract_lookalikes(self):
        original = self.title_request()
        cases = [copy.deepcopy(original) for _ in range(6)]
        cases[0]["input"][0]["content"][0]["text"] += " PRIVATE"
        cases[1]["tools"] = self.request()["tools"]
        cases[2]["text"]["format"]["schema"]["properties"]["title"]["maxLength"] = 37
        cases[3]["text"]["format"]["strict"] = False
        cases[4]["input"][0]["content"][0]["type"] = "output_text"
        cases[5].pop("text")
        cases[5]["tools"] = self.request()["tools"]
        for body, baseline in [*( (body, frozenset()) for body in cases), (original, None)]:
            model = self.model([self.receipt("SessionStart"), self.receipt("UserPromptSubmit")])
            model.baseline = baseline
            with self.assertRaises(smoke.FixtureFailure): model.respond(body)
            self.assertEqual((model.title_count, model.counts, model.requests, model.phase), (0, {}, [], "initial"))

    def test_first_provider_response_binds_only_postbaseline_native_session_and_prompt(self):
        old = self.receipt("SessionStart", id="old-start", session_id="old-session")
        good = [old, self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        model = self.model(good)
        model.session, model.baseline = None, frozenset({"old-start"})
        item, category, _ = model.respond(self.request())
        self.assertEqual((model.session, model.turn, category, item["call_id"]),
                         ("session-1", "turn-1", "parent1", "tempo-plan"))
        self.assertEqual(len(model.requests), 1)
        cases = [(good, None), (good[2:], frozenset()), (good, frozenset({"old-start", "receipt-SessionStart"})),
                 (good + [self.receipt("SessionStart", id="second-start", session_id="session-2")], frozenset({"old-start"})),
                 ([self.receipt("SessionStart", disposition="review_required"), good[2]], frozenset()),
                 ([good[1], self.receipt("UserPromptSubmit", session_id="different")], frozenset()),
                 ([good[1], self.receipt("UserPromptSubmit", ordering="unavailable")], frozenset()),
                 (good[1:], frozenset({"receipt-UserPromptSubmit"})),
                 ([good[1]], frozenset()),
                 ([good[1], self.receipt("UserPromptSubmit", id="baseline-prompt"),
                   self.receipt("UserPromptSubmit", ordering="unavailable")], frozenset({"baseline-prompt"}))]
        for receipts, baseline in cases:
            model = self.model(receipts)
            model.session, model.baseline = None, baseline
            with self.assertRaises(smoke.FixtureFailure): model.respond(self.request())
            self.assertIsNone(model.session)
            self.assertEqual(model.requests, [])
        for marker in (smoke.CHILD_PROMPT, smoke.INTERRUPT_PROMPT):
            model = self.model(good[1:])
            model.session, model.baseline = None, frozenset()
            with self.assertRaises(smoke.FixtureFailure): model.respond(self.request(marker))
            self.assertEqual(model.requests, [])

    def test_direct_fixture_model_accepts_native_plan_schema_and_rejects_other_model(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        body = self.request()
        body["model"] = "tempo-ci-fixture"
        item, category, hold = self.model(receipts).respond(body)
        self.assertEqual((item["name"], item["call_id"], category, hold), ("update_plan", "tempo-plan", "parent1", None))
        self.assertEqual(json.loads(item["arguments"]), {"plan": [{"step": "Verify native hook lifecycle", "status": "completed"}]})
        body["model"] = "gpt-6.1-sol"
        with self.assertRaisesRegex(smoke.FixtureFailure, "^provider_request_contract$"):
            self.model(receipts).respond(body)

    def test_measured_start_submits_once_before_receipt_wait_without_session_overwrite(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        model = self.model(receipts)
        model.session = None
        terminal = mock.Mock()
        terminal.command.side_effect = lambda value: model.respond(self.request(value))
        def wait_snapshot(predicate, category):
            terminal.command.assert_called_once_with(smoke.PARENT_PROMPT)
            snapshot = {"receipts": receipts}
            self.assertTrue(predicate(snapshot))
            return snapshot
        smoke.start_measured_turn(terminal, model, {"old-start"}, wait_snapshot)
        self.assertEqual(model.session, "session-1")
        self.assertEqual(model.baseline, frozenset({"old-start"}))
        with self.assertRaises(smoke.FixtureFailure):
            smoke.start_measured_turn(terminal, model, set(), wait_snapshot)
        terminal.command.assert_called_once_with(smoke.PARENT_PROMPT)
        deferred = self.model(receipts)
        deferred.session = None
        terminal.reset_mock(side_effect=True)
        smoke.start_measured_turn(terminal, deferred, set(), wait_snapshot)
        self.assertIsNone(deferred.session)
        deferred.respond(self.request())
        self.assertEqual(deferred.session, "session-1")
        terminal.command.side_effect = lambda value: model.respond(self.request(value))
        for observed in ([self.receipt("SessionStart", session_id="different")],
                         [*receipts, self.receipt("SessionStart", id="ambiguous-start")]):
            model = self.model(receipts)
            model.session = None
            terminal.reset_mock()
            def mismatch_wait(predicate, category):
                terminal.command.assert_called_once_with(smoke.PARENT_PROMPT)
                return {"receipts": observed}
            with self.assertRaises(smoke.FixtureFailure):
                smoke.start_measured_turn(terminal, model, set(), mismatch_wait)
            self.assertEqual(model.session, "session-1")

    def test_model_transition_requires_real_plan_and_tool_receipts(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        model = self.model(receipts)
        body = self.request()
        item, _, hold = model.respond(body)
        self.assertEqual(item["name"], "update_plan")
        self.assertEqual(item["call_id"], "tempo-plan")
        self.assertIsNone(hold)
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)
        body["input"].append({"type": "function_call_output", "call_id": "tempo-plan", "output": smoke.PLAN_RESULT})
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)
        receipts.extend([self.receipt("PreToolUse", tool_id="tempo-plan"), self.receipt("PostToolUse", tool_id="tempo-plan")])
        item, _, _ = model.respond(body)
        self.assertEqual(item["name"], "spawn_agent")
        self.assertEqual(len(model.requests), 2)

    def test_native_plan_receipts_must_match_actor_turn_session_and_call(self):
        for kind in ("PreToolUse", "PostToolUse"):
            for field, value in (("actor", "other-actor"), ("session_id", "other-session"), ("turn_id", "other-turn"),
                                 ("tool_id", "other-call"), ("durability", "not_committed"), ("ordering", "unavailable")):
                receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit"),
                            self.receipt("PreToolUse", tool_id="tempo-plan"), self.receipt("PostToolUse", tool_id="tempo-plan")]
                next(r for r in receipts if r["kind"] == kind)[field] = value
                model = self.model(receipts)
                body = self.request()
                model.respond(body)
                body["input"].append({"type": "function_call_output", "call_id": "tempo-plan", "output": "Plan updated"})
                with self.assertRaises(smoke.FixtureFailure): model.respond(body)
                self.assertEqual(model.counts, {"parent": 1})
        receipts = [self.receipt(kind, actor=None, tool_id="tempo-plan") for kind in
                    ("SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse")]
        model = self.model(receipts)
        body = self.request()
        model.respond(body)
        body["input"].append({"type": "function_call_output", "call_id": "tempo-plan", "output": "Plan updated"})
        with self.assertRaises(smoke.FixtureFailure): model.respond(body)

    def eligibility_cost_context(self):
        public, rows = self.diagnostic_context()
        rows[0]["eligibility"] = {
            "p": [100, 200, 237000, 5, 20, -1, 237500],
            "r": [[1, 1, 287086056, 2192, 80, 70000, 166000, 5, 236200],
                  [1, 1, 12000, 2, 10, 8, 15, 2, 40],
                  [1, 1, 4000, 2, 10, 5, 8, 2, 30],
                  [0]*9, [0]*9, [0]*9],
            "c": [220000, 15000], "d": False}
        return public, rows

    def test_eligibility_v2_preserves_zero_receipt_gate(self):
        public, rows = self.eligibility_cost_context()
        text = public + "\ntempo hook diagnostics v2: " + json.dumps(rows, separators=(",", ":"))
        model = self.model([])
        model.session = None
        model.baseline = frozenset()
        body = self.request()
        body["input"].insert(0, {"role": "developer", "content": [{"type": "input_text", "text": text}]})
        with mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
             mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")), \
             mock.patch.object(smoke.http.server, "ThreadingHTTPServer", side_effect=AssertionError("unexpected socket")):
            with self.assertRaisesRegex(smoke.FixtureFailure, "measured_session_ambiguous"):
                model.respond(body)
        # Subtests retain every genuine refusal postcondition during old-parser RED.
        with self.subTest("fixed_public_tuple"):
            self.assertEqual(model.capture_contexts, {("SessionStart", "state_busy", "not_committed")})
        with self.subTest("fixed_numeric_policy_observation"):
            self.assertEqual(getattr(model, "admission_diagnostics", []), [{"kind":"SessionStart", "attempts":rows, "version":2}])
        self.assertEqual(model.initial_receipt_probe["accepted_session_starts"], 0)
        self.assertEqual(model.counts, {})
        self.assertEqual(model.entry_count, 1)

    def test_eligibility_v2_rejects_shapes_and_retains_v1(self):
        public, rows = self.eligibility_cost_context()
        def body(values, role="developer"):
            return {"input":[{"role":role,"content":[{"type":"input_text","text":public+"\ntempo hook diagnostics v2: "+json.dumps(values,separators=(",",":"))}]}]}
        with self.subTest("valid_v2"):
            self.assertEqual(smoke.capture_admission_diagnostics(body(rows)), [{"kind":"SessionStart","attempts":rows,"version":2}])
        legacy_public, legacy_rows = self.diagnostic_context()
        legacy = {"input":[{"role":"developer","content":[{"type":"input_text","text":legacy_public+"\ntempo hook diagnostics v1: "+json.dumps(legacy_rows)}]}]}
        self.assertEqual(smoke.capture_admission_diagnostics(legacy), [{"kind":"SessionStart","attempts":legacy_rows}])
        cases = [
            lambda v:v["p"].__setitem__(0,True),
            lambda v:v.update(path="PRIVATE-PATH"),
            lambda v:v["r"][0].__setitem__(0,129),
            lambda v:v["r"][0].__setitem__(2,-1),
            lambda v:v["r"].append([0]*9),
            lambda v:v.update(c=[-1,0]),
            lambda v:v["p"].__setitem__(6,120000001),
            lambda v:v.update(d=True),
        ]
        for mutate in cases:
            candidate = copy.deepcopy(rows); mutate(candidate[0]["eligibility"])
            with self.subTest("rejected_shape"):
                self.assertEqual(smoke.capture_admission_diagnostics(body(candidate)), [])
                self.assertEqual(smoke.capture_context_tuples(body(candidate)), set())
        self.assertEqual(smoke.capture_admission_diagnostics(body(rows, "user")), [])
        raw = public+"\ntempo hook diagnostics v2: "+"["*3073
        self.assertEqual(smoke.hook_diagnostic_text(raw), (raw,None))

    def admission_v3_context(self):
        public, rows = self.eligibility_cost_context()
        rows[0].update(effective_deadline_us=488000, eligibility_excluded_us=238000, eligibility_outcome="paused")
        return public, rows

    def admission_v3_body(self, rows, role="developer"):
        public, _ = self.diagnostic_context()
        return {"input": [{"role": role, "content": [{"type": "input_text", "text": public + "\ntempo hook diagnostics v3: " + json.dumps(rows, separators=(",", ":"))}]}]}

    def test_admission_v3_preserves_actual_zero_receipt_gate(self):
        public, rows = self.admission_v3_context()
        model = self.model([])
        model.session = None
        model.baseline = frozenset()
        body = self.request()
        body["input"].insert(0, self.admission_v3_body(rows)["input"][0])
        with mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
             mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")), \
             mock.patch.object(smoke.http.server, "ThreadingHTTPServer", side_effect=AssertionError("unexpected socket")):
            with self.assertRaisesRegex(smoke.FixtureFailure, "measured_session_ambiguous"):
                model.respond(body)
        self.assertEqual(model.capture_contexts, {("SessionStart", "state_busy", "not_committed")})
        self.assertEqual(model.admission_diagnostics, [{"kind": "SessionStart", "attempts": rows, "version": 3}])
        self.assertEqual(model.initial_receipt_probe["accepted_session_starts"], 0)
        self.assertEqual(model.counts, {})
        self.assertEqual(model.entry_count, 1)

    def test_admission_v3_outcomes_and_floor_rounding(self):
        _, base = self.admission_v3_context()
        variants = []
        for effective in (488000, 488001):
            rows = copy.deepcopy(base); rows[0]["effective_deadline_us"] = effective
            variants.append(("paused_floor_" + str(effective), rows))
        rows = copy.deepcopy(base); rows[0].update(caller_deadline_us=300000, effective_deadline_us=300000)
        variants.append(("caller_clip", rows))
        rows = copy.deepcopy(base); rows[0].update(eligibility_excluded_us=0, effective_deadline_us=250000)
        rows[0]["phase_us"][5] = rows[0]["phase_us"][4]
        rows[0]["eligibility"] = {"p": [0, 0, -1, -1, -1, -1, 0], "r": [[0]*9 for _ in range(6)], "c": [0, 0], "d": False}
        variants.append(("submicrosecond_nonhash_pause", rows))
        rows = copy.deepcopy(rows); rows[0]["phase_us"][5] += 1; rows[0]["effective_deadline_us"] += 1
        variants.append(("submicrosecond_floor_difference", rows))
        rows = copy.deepcopy(base); rows[0].update(caller="deadline", caller_deadline_us=260000, effective_deadline_us=260000)
        variants.append(("absolute_deadline_observed_before_context_signal", rows))
        for outcome in ("not_called", "error", "caller_stopped"):
            rows = copy.deepcopy(base)
            rows[0].update(eligibility_outcome=outcome, eligibility_excluded_us=0, effective_deadline_us=250000)
            if outcome == "not_called":
                rows[0]["phase_us"][4:6] = [-1, -1]
                rows[0]["eligibility"] = {"p": [-1]*7, "r": [[0]*9 for _ in range(6)], "c": [-1, -1], "d": False}
            if outcome == "caller_stopped": rows[0]["caller"] = "deadline"
            variants.append((outcome, rows))
        rows = copy.deepcopy(next(rows for name, rows in variants if name == "error")); rows[0]["caller"] = "canceled"
        variants.append(("policy_error_wins_cancellation", rows))
        rows = copy.deepcopy(next(rows for name, rows in variants if name == "not_called")); rows[0].update(deadline_us=-1, effective_deadline_us=-1)
        rows[0]["phase_us"] = [-1]*8
        variants.append(("not_called_no_deadline", rows))
        for name, rows in variants:
            with self.subTest(name=name):
                self.assertEqual(smoke.capture_admission_diagnostics(self.admission_v3_body(rows)), [{"kind": "SessionStart", "attempts": rows, "version": 3}])
                self.assertEqual(smoke.capture_context_tuples(self.admission_v3_body(rows)), {("SessionStart", "state_busy", "not_committed")})

    def test_admission_v3_three_attempts_share_original_caller(self):
        _, base = self.admission_v3_context()
        rows = []
        for index in range(3):
            row = copy.deepcopy(base[0]); offset = index * 270000
            row["ordinal"] = index + 1
            for key in ("start_us", "end_us", "deadline_us"):
                row[key] += offset
            row["phase_us"] = [stamp + offset for stamp in row["phase_us"]]
            row["effective_deadline_us"] = min(row["deadline_us"] + row["eligibility_excluded_us"], row["caller_deadline_us"])
            rows.append(row)
        body = self.admission_v3_body(rows)
        text = body["input"][0]["content"][0]["text"]
        self.assertLessEqual(len(text.split("\ntempo hook diagnostics v3: ")[1]), 3072)
        self.assertEqual(smoke.capture_admission_diagnostics(body), [{"kind": "SessionStart", "attempts": rows, "version": 3}])

    def test_admission_v3_rejects_invalid_cross_fields_and_shapes(self):
        _, base = self.admission_v3_context()
        mutations = {
            "missing": lambda r: r.pop("effective_deadline_us"),
            "extra": lambda r: r.update(path="PRIVATE-PATH"),
            "bool_effective": lambda r: r.update(effective_deadline_us=True),
            "bool_excluded": lambda r: r.update(eligibility_excluded_us=True),
            "negative_excluded": lambda r: r.update(eligibility_excluded_us=-1),
            "large_effective": lambda r: r.update(effective_deadline_us=120000001),
            "large_excluded": lambda r: r.update(eligibility_excluded_us=120000001),
            "unknown_outcome": lambda r: r.update(eligibility_outcome="trusted"),
            "object_outcome": lambda r: r.update(eligibility_outcome={}),
            "list_outcome": lambda r: r.update(eligibility_outcome=[]),
            "no_original": lambda r: r.update(deadline_us=-1),
            "reset": lambda r: r.update(effective_deadline_us=509000),
            "too_low": lambda r: r.update(effective_deadline_us=487999),
            "too_high": lambda r: r.update(effective_deadline_us=488002),
            "past_caller": lambda r: r.update(caller_deadline_us=300000),
            "unearned_exclusion": lambda r: r.update(eligibility_excluded_us=260001),
            "exclusion_above_measured_span": lambda r: r.update(eligibility_excluded_us=238001, effective_deadline_us=488001),
            "exclusion_below_measured_span": lambda r: r.update(eligibility_excluded_us=237998, effective_deadline_us=487998),
            "reversed_phase": lambda r: r["phase_us"].__setitem__(5, 0),
            "unreached_policy": lambda r: r["phase_us"].__setitem__(4, -1),
            "phase_before_attempt": lambda r: r.update(start_us=22000),
            "error_credit": lambda r: r.update(eligibility_outcome="error"),
            "not_called_stamped": lambda r: r.update(eligibility_outcome="not_called", eligibility_excluded_us=0, effective_deadline_us=250000),
            "caller_stopped_live": lambda r: r.update(eligibility_outcome="caller_stopped", eligibility_excluded_us=0, effective_deadline_us=250000),
        }
        for name, mutate in mutations.items():
            rows = copy.deepcopy(base); mutate(rows[0])
            with self.subTest(name=name):
                body = self.admission_v3_body(rows)
                self.assertEqual(smoke.capture_admission_diagnostics(body), [])
                self.assertEqual(smoke.capture_context_tuples(body), set())
        self.assertEqual(smoke.capture_admission_diagnostics(self.admission_v3_body(base, "user")), [])

    def test_admission_v3_rejects_duplicate_mixed_and_oversized_envelopes(self):
        public, rows = self.admission_v3_context()
        encoded = json.dumps(rows, separators=(",", ":"))
        duplicate = encoded.replace('"eligibility_outcome":"paused"', '"eligibility_outcome":"paused","eligibility_outcome":"error"')
        for suffix in (duplicate, "["*3073, encoded + "\ntempo hook diagnostics v2: []"):
            text = public + "\ntempo hook diagnostics v3: " + suffix
            with self.subTest(suffix=suffix[:40]):
                self.assertEqual(smoke.hook_diagnostic_text(text), (text, None))

    def test_cpu_capability_probe_is_bounded_post_join_projection(self):
        cases = [
            (b"processor : 0\nflags : sha_ni avx avx2 bmi2 sse4_1 ssse3 PRIVATE-CONTENT\n", "observed"),
            (b"flags : avx sha_ni\nflags : avx\n", "observed"),
            (b"processor : PRIVATE-CONTENT\n", "unavailable"),
            (b"flags : \xff\n", "unavailable"),
            (b"x"*1048577, "unavailable"),
        ]
        for raw, expected in cases:
            with self.subTest(status=expected):
                stream = mock.MagicMock()
                stream.__enter__.return_value = stream
                stream.read.return_value = raw
                path = mock.Mock(); path.open.return_value = stream
                with mock.patch.object(smoke.platform, "system", return_value="Linux"), \
                     mock.patch.object(smoke, "Path", return_value=path) as constructor, \
                     mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
                     mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")):
                    self.assertEqual(smoke.observe_cpu_flags(False), {"status":"unavailable","flags":{}})
                    constructor.assert_not_called()
                    result = smoke.observe_cpu_flags(True)
                constructor.assert_called_once_with("/proc/cpuinfo")
                path.open.assert_called_once_with("rb")
                stream.read.assert_called_once_with(1048577)
                stream.__exit__.assert_called_once()
                self.assertEqual(result["status"], expected)
                self.assertNotIn("PRIVATE", json.dumps(result))
                if expected == "observed":
                    self.assertEqual(set(result["flags"]), {"sha_ni","avx","avx2","bmi2","sse4_1","ssse3"})
                    self.assertTrue(all(type(value) is bool for value in result["flags"].values()))
                    if raw.count(b"flags") == 2: self.assertFalse(result["flags"]["sha_ni"])
                else: self.assertEqual(result["flags"], {})
        with mock.patch.object(smoke.platform, "system", return_value="Linux"), \
             mock.patch.object(smoke, "Path", side_effect=OSError("PRIVATE-ERROR")):
            self.assertEqual(smoke.observe_cpu_flags(True), {"status":"unavailable","flags":{}})
        for code in ("overall_deadline", "fixture_cancelled"):
            with self.subTest(fixture_failure=code):
                failure = smoke.FixtureFailure(code)
                stream = mock.MagicMock()
                stream.__enter__.return_value = stream
                stream.__exit__.return_value = False
                stream.read.side_effect = failure
                path = mock.Mock(); path.open.return_value = stream
                with mock.patch.object(smoke.platform, "system", return_value="Linux"), \
                     mock.patch.object(smoke, "Path", return_value=path) as constructor, \
                     mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
                     mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")), \
                     mock.patch.object(smoke.http.server, "ThreadingHTTPServer", side_effect=AssertionError("unexpected socket")):
                    raised = None
                    try:
                        smoke.observe_cpu_flags(True)
                    except smoke.FixtureFailure as exc:
                        raised = exc
                constructor.assert_called_once_with("/proc/cpuinfo")
                path.open.assert_called_once_with("rb")
                stream.read.assert_called_once_with(1048577)
                stream.__exit__.assert_called_once_with(smoke.FixtureFailure, failure, mock.ANY)
                self.assertIs(raised, failure)

    def diagnostic_context(self):
        rows = [{"ordinal": 1, "start_us": 0, "end_us": 260000, "deadline_us": 250000,
                 "caller_deadline_us": 900000, "phase_us": [10000, 11000, 12000, 20000, 21000, 259000, 259100, 259200],
                 "caller": "live", "retry": True, "native_phase": "none", "native_category": "none", "native_code": 0, "native_cleanup": False}]
        public = "tempo capture: kind=SessionStart; code=state_busy; durability=not_committed"
        return public, rows

    def test_admission_diagnostics_preserve_zero_receipt_gate_and_public_tuple(self):
        public, rows = self.diagnostic_context()
        text = public + "\ntempo hook diagnostics v1: " + json.dumps(rows, separators=(",", ":"))
        model = self.model([])
        model.session = None
        model.baseline = frozenset()
        body = self.request()
        body["input"].insert(0, {"role": "developer", "content": [{"type": "input_text", "text": text}]})
        with mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
             mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")), \
             mock.patch.object(smoke.http.server, "ThreadingHTTPServer", side_effect=AssertionError("unexpected socket")):
            with self.assertRaisesRegex(smoke.FixtureFailure, "measured_session_ambiguous"):
                model.respond(body)
        self.assertEqual(model.capture_contexts, {("SessionStart", "state_busy", "not_committed")})
        self.assertEqual(getattr(model, "admission_diagnostics", []), [{"kind": "SessionStart", "attempts": rows}])
        self.assertEqual(model.initial_receipt_probe["accepted_session_starts"], 0)
        self.assertEqual(model.counts, {})

    def test_admission_diagnostics_reject_untrusted_or_unbounded_metadata(self):
        public, rows = self.diagnostic_context()
        valid = json.dumps(rows, separators=(",", ":"))
        cases = [valid + " SECRET", valid.replace('"start_us":0', '"start_us":true'),
                 valid.replace('"caller":"live"', '"caller":"SECRET"'),
                 valid.replace('"native_phase":"none"', '"native_phase":[]'),
                 valid.replace('"ordinal":1', '"ordinal":1,"ordinal":1'),
                 json.dumps(rows * 4), "[" * 4000, valid.replace('"native_code":0', '"native_code":2147483648')]
        for suffix in cases:
            with self.subTest(suffix=suffix[:0]):
                body = {"input": [{"role": "developer", "content": [{"type": "input_text", "text": public + "\ntempo hook diagnostics v1: " + suffix}]}]}
                self.assertEqual(smoke.capture_context_tuples(body), set())
        body = {"input": [{"role": "user", "content": [{"type": "input_text", "text": public + "\ntempo hook diagnostics v1: " + valid}]}]}
        self.assertEqual(smoke.capture_context_tuples(body), set())
        body["input"][0]["role"] = "developer"
        body["input"][0]["content"][0]["text"] = public
        self.assertEqual(smoke.capture_context_tuples(body), {("SessionStart", "state_busy", "not_committed")})

    def test_start_capture_context_is_observed_without_satisfying_child_receipt_gate(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit")]
        model = self.model(receipts)
        body = self.request(smoke.CHILD_PROMPT)
        context = "tempo capture: kind=SubagentStart; code=state_busy; durability=not_committed"
        body["input"].insert(0, {"role": "developer", "content": [{"type": "input_text", "text": context}]})
        with mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
             mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")), \
             mock.patch.object(smoke.http.server, "ThreadingHTTPServer", side_effect=AssertionError("unexpected socket")):
            with self.assertRaisesRegex(smoke.FixtureFailure, "child_start_barrier_missing"):
                model.respond(body)
            self.assertEqual(model.capture_contexts, {("SubagentStart", "state_busy", "not_committed")})
            self.assertEqual(model.counts, {})
            receipts.append(self.receipt("SubagentStart", agent_id="child-1"))
            self.assertEqual(model.respond(body)[2], "child")
            self.assertEqual(model.capture_contexts, {("SubagentStart", "state_busy", "not_committed")})
            self.assertEqual(model.counts, {"child": 1})

    def test_start_capture_context_projection_rejects_untrusted_shapes_and_raw_text(self):
        exact = "tempo capture: kind=SessionStart; code=clock_unavailable; durability=committed"
        cases = [
            ("positive", "developer", "input_text", exact, {("SessionStart", "clock_unavailable", "committed")}),
            ("user_lookalike", "user", "input_text", exact, set()),
            ("assistant_lookalike", "assistant", "input_text", exact, set()),
            ("wrong_content", "developer", "output_text", exact, set()),
            ("unreviewed_kind", "developer", "input_text", exact.replace("SessionStart", "Stop"), set()),
            ("unknown_code", "developer", "input_text", exact.replace("clock_unavailable", "SECRET"), set()),
            ("unknown_durability", "developer", "input_text", exact.replace("committed", "SECRET"), set()),
            ("prefix", "developer", "input_text", "SECRET " + exact, set()),
            ("suffix", "developer", "input_text", exact + " SECRET", set()),
            ("newline", "developer", "input_text", exact + "\n", set()),
        ]
        with mock.patch.object(smoke.subprocess, "Popen", side_effect=AssertionError("unexpected process")), \
             mock.patch.object(smoke.threading.Thread, "start", side_effect=AssertionError("unexpected thread")), \
             mock.patch.object(smoke.http.server, "ThreadingHTTPServer", side_effect=AssertionError("unexpected socket")):
            for name, role, content_type, text, expected in cases:
                with self.subTest(name=name):
                    model = self.model([self.receipt("SessionStart"), self.receipt("UserPromptSubmit")])
                    body = self.request(smoke.CHILD_PROMPT)
                    body["input"].insert(0, {"role": role, "content": [{"type": content_type, "text": text}]})
                    with self.assertRaises(smoke.FixtureFailure): model.respond(body)
                    self.assertEqual(model.capture_contexts, expected)
                    self.assertNotIn("SECRET", repr(model.capture_contexts))
                    self.assertEqual(model.counts, {})

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

    def test_completed_capture_is_clean_before_interrupt(self):
        good = {"queued": 2, "uncertainties": 0, "uncertainty_details": [], "capture_reviews": 0}
        self.assertEqual(smoke.require_completed_capture(good), 2)
        for changes in ({"queued": 0}, {"uncertainties": 1}, {"uncertainty_details": [{}]}):
            with self.assertRaises(smoke.FixtureFailure): smoke.require_completed_capture(good | changes)
        for value in (None, False, True, "0", -1, 1, 128):
            with self.subTest(capture_reviews=value), self.assertRaises(smoke.FixtureFailure):
                smoke.require_completed_capture(good | {"capture_reviews": value})
        with self.assertRaises(smoke.FixtureFailure):
            smoke.require_completed_capture({key: value for key, value in good.items() if key != "capture_reviews"})

    def test_final_capture_requires_exact_bounded_interrupt_tail_and_preserved_queue(self):
        actor = {"key": {"computer_id": "computer", "source": "codex", "session_id": "session", "agent_id": "root"}, "generation": "2"}
        receipt = {"actor": actor}
        detail = {"actor": actor, "reason": "source_lost", "state": "unresolved", "bounded": True,
                  "resolution_present": False, "discarded": False}
        good = {"queued": 2, "uncertainties": 1, "uncertainty_details": [detail], "capture_reviews": 0}
        smoke.require_capture_effects(good, receipt, 2)
        cases = [good | {"queued": 1}, good | {"uncertainties": 0}, good | {"uncertainty_details": []},
                 good | {"uncertainties": 2, "uncertainty_details": [detail, detail]}]
        for changes in ({"actor": None}, {"actor": actor | {"generation": "1"}}, {"reason": "clock_changed"},
                        {"state": "resolved"}, {"bounded": False}, {"resolution_present": True}, {"discarded": True}):
            cases.append(good | {"uncertainty_details": [detail | changes]})
        for snapshot in cases:
            with self.subTest(snapshot=snapshot), self.assertRaises(smoke.FixtureFailure):
                smoke.require_capture_effects(snapshot, receipt, 2)
        with self.assertRaises(smoke.FixtureFailure): smoke.require_capture_effects(good, {"actor": None}, 2)
        for value in (None, False, True, "0", -1, 1, 128):
            with self.subTest(capture_reviews=value), self.assertRaises(smoke.FixtureFailure):
                smoke.require_capture_effects(good | {"capture_reviews": value}, receipt, 2)
        with self.assertRaises(smoke.FixtureFailure):
            smoke.require_capture_effects({key: value for key, value in good.items() if key != "capture_reviews"}, receipt, 2)

    def test_root_provider_turns_exclude_native_child_prompts(self):
        receipts = [self.receipt("SessionStart"), self.receipt("UserPromptSubmit"),
                    self.receipt("SubagentStart", agent_id="child", turn_id="child-turn"),
                    self.receipt("UserPromptSubmit", id="child-prompt", agent_id="child", turn_id="child-turn")]
        model = self.model(receipts)
        model.session, model.baseline = None, frozenset()
        self.assertEqual(model.respond(self.request())[0]["call_id"], "tempo-plan")
        self.assertEqual(model.turn, "turn-1")
        model.phase = "parent-complete"
        receipts.append(self.receipt("UserPromptSubmit", id="interrupt-prompt", turn_id="interrupt-turn"))
        self.assertEqual(model.respond(self.request(smoke.INTERRUPT_PROMPT))[2], "interrupt")
        self.assertEqual(model.interrupt_turn, "interrupt-turn")
        receipts.append(self.receipt("UserPromptSubmit", id="extra-root", turn_id="other-root"))
        with self.assertRaises(smoke.FixtureFailure): model.respond(self.request(smoke.INTERRUPT_PROMPT))

    def test_initial_gate_probe_counts_do_not_relax_the_native_barrier(self):
        old = self.receipt("SessionStart", id="baseline")
        start, prompt = self.receipt("SessionStart"), self.receipt("UserPromptSubmit")
        cases = [([], 0, "measured_session_ambiguous"),
                 ([start, prompt], 1, None),
                 ([start, prompt, start | {"id": "second-start"}], 2, "measured_session_ambiguous"),
                 ([start | {"disposition": "review_required"}, prompt], 0, "measured_session_ambiguous"),
                 ([start | {"durability": "unknown"}, prompt], 0, "measured_session_ambiguous"),
                 ([start], 1, "parent_prompt_identity")]
        for rows, count, failure in cases:
            with self.subTest(count=count, failure=failure):
                model = self.model([old, *rows])
                model.session, model.baseline = None, frozenset({"baseline"})
                if failure:
                    with self.assertRaisesRegex(smoke.FixtureFailure, "^" + failure + "$"):
                        model.respond(self.request())
                    self.assertEqual(model.requests, [])
                else:
                    self.assertEqual(model.respond(self.request())[0]["call_id"], "tempo-plan")
                probe = model.initial_receipt_probe
                self.assertEqual(probe["status"], "available")
                self.assertEqual(probe["receipt_count"], len(rows) + 1)
                self.assertEqual(probe["postbaseline_count"], len(rows))
                self.assertEqual(probe["accepted_session_starts"], count)
                self.assertEqual(sum(row["count"] for row in probe["counts"]), len(rows))
                for row in rows:
                    expected = sum((r["kind"], r["disposition"], r["durability"]) ==
                                   (row["kind"], row["disposition"], row["durability"]) for r in rows)
                    self.assertIn({"kind": row["kind"], "disposition": row["disposition"],
                                   "durability": row["durability"], "count": expected}, probe["counts"])

    def test_initial_gate_probe_is_bounded_redacted_and_distinguishes_read_failure(self):
        canary = "PRIVATE-PAYLOAD-PATH-CANARY"
        receipt = self.receipt("SessionStart", id=canary, session_id=canary, payload=canary, path=canary)
        probe = smoke.receipt_gate_probe([receipt], frozenset())
        self.assertEqual(probe["accepted_session_starts"], 1)
        self.assertNotIn(canary, json.dumps(probe))
        for changes in ({"kind": canary}, {"disposition": canary}, {"durability": canary}, {"id": None}):
            self.assertEqual(smoke.receipt_gate_probe([receipt | changes], frozenset()), {"status": "invalid_response"})
        self.assertEqual(smoke.receipt_gate_probe([receipt] * 129, frozenset()), {"status": "overflow", "receipt_count": 129})
        self.assertEqual(smoke.receipt_gate_probe([], None), {"status": "unarmed"})
        model = self.model([])
        model.session, model.baseline = None, frozenset()
        error = RuntimeError(canary)
        model.read = mock.Mock(side_effect=error)
        with self.assertRaises(RuntimeError) as raised:
            model.respond(self.request())
        self.assertIs(raised.exception, error)
        self.assertEqual(model.initial_receipt_probe, {"status": "read_failed"})
        self.assertEqual(model.requests, [])

    def test_cleanup_reports_join_completion_without_masking_primary_failure(self):
        for failure in (None, "terminal", "model", "provider"):
            terminal, model = mock.Mock(), mock.Mock()
            model.error = "provider_protocol_failed" if failure == "provider" else None
            if failure in ("terminal", "model"):
                (terminal if failure == "terminal" else model).close.side_effect = smoke.FixtureFailure("cleanup_failed")
            self.assertIs(smoke.close_native(terminal, model, primary_failure=True), failure not in ("terminal", "model"))
            terminal.close.assert_called_once()
            model.close.assert_called_once()
        for alive in (False, True):
            model = object.__new__(smoke.Model)
            model.shutdown, model.child_release, model.server, model.thread = (mock.Mock() for _ in range(4))
            model.thread.is_alive.return_value = alive
            if alive:
                with self.assertRaisesRegex(smoke.FixtureFailure, "^provider_join_failed$"):
                    model.close()
            else:
                model.close()
            model.shutdown.set.assert_called_once()
            model.child_release.set.assert_called_once()
            model.server.shutdown.assert_called_once()
            model.server.server_close.assert_called_once()
            model.thread.join.assert_called_once_with(timeout=2)

    def test_failure_profile_observation_skips_incomplete_cleanup_and_bounds_read(self):
        confirmed = {"revision": "1"}
        callback = mock.Mock(side_effect=RuntimeError("PRIVATE-HELPER-ERROR"))
        with mock.patch.object(smoke.time, "monotonic", return_value=100):
            for joined in (False, None):
                self.assertEqual(smoke.observe_failed_profile(callback, confirmed, Path("/tmp/project"), "0.159.3", 120, joined),
                                 {"status": "cleanup_incomplete"})
            self.assertEqual(smoke.observe_failed_profile(callback, None, Path("/tmp/project"), "0.159.3", 120, True),
                             {"status": "unconfirmed"})
            self.assertEqual(smoke.observe_failed_profile(callback, confirmed, Path("/tmp/project"), "0.159.3", 100, True),
                             {"status": "deadline_expired"})
            callback.assert_not_called()
            for deadline, timeout in ((140, 20), (103.5, 3.5)):
                callback.reset_mock()
                with self.assertRaisesRegex(smoke.FixtureFailure, "^measured_session_ambiguous$"):
                    try:
                        raise smoke.FixtureFailure("measured_session_ambiguous")
                    finally:
                        observed = smoke.observe_failed_profile(callback, confirmed, Path("/tmp/project"), "0.159.3", deadline, True)
                self.assertEqual(observed, {"status": "helper_failed"})
                callback.assert_called_once_with(timeout)


    def test_native_cleanup_preserves_primary_failure_and_still_vetoes_success(self):
        for primary in (True, False):
            for terminal_failure in (False, True):
                terminal, model = mock.Mock(), mock.Mock()
                model.error = "provider_disconnected"
                if terminal_failure: terminal.close.side_effect = smoke.FixtureFailure("terminal_cleanup_failed")
                if primary:
                    with self.assertRaisesRegex(smoke.FixtureFailure, "^parent_child_independence$"):
                        try:
                            raise smoke.FixtureFailure("parent_child_independence")
                        finally:
                            smoke.close_native(terminal, model, primary_failure=True)
                else:
                    with self.assertRaises(smoke.FixtureFailure):
                        smoke.close_native(terminal, model, primary_failure=False)
                terminal.close.assert_called_once()
                model.close.assert_called_once()

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


class InstallIntegrationTests(unittest.TestCase):
    def test_codex_installs_before_trust_and_confirms_current_status_before_measurement(self):
        class MeasurementReached(Exception): pass
        for invalid_install in (False, True):
            with self.subTest(invalid_install=invalid_install), tempfile.TemporaryDirectory(prefix="tempo-install-") as tmp:
                root = Path(tmp).resolve(); home = root / "synthetic-home"; home.mkdir()
                source, helper, runtime = root / "source-tempo", root / "helper", root / "runtime"
                for path in (source, helper, runtime): path.write_text("inert build")
                events, launches = [], []
                project = root / "project"
                def result(confirmed):
                    doc = self.profile(confirmed)
                    row = doc["hooks"][0]; row["path"] = str(project)
                    row["profile"]["context"]["path"] = str(project)
                    return doc
                def bounded(argv, _env, _cwd, **_kwargs):
                    if argv[0] == "/usr/bin/git": return b""
                    self.assertEqual(argv[:2], [str(helper), "fixture"])
                    action = argv[2]; events.append(action)
                    if action == "link": return b""
                    if action == "read": return json.dumps({"receipts": []}).encode()
                    self.assertEqual(argv[-4:], [str(project), str(runtime), str(root / "tempo"), "user"])
                    if action == "install":
                        _, _, definitions = self.definitions(root, "codex")
                        (home / ".codex/hooks.json").write_text(json.dumps(definitions))
                        doc = result(False)
                        if invalid_install: doc["hooks"][0]["profile"]["capture_eligible"] = True
                        return json.dumps(doc).encode()
                    return json.dumps(result(action == "confirm")).encode()
                def trust(*_args):
                    events.append("trust")
                    config = home / ".codex/config.toml"
                    config.write_text(config.read_text() + '\n[hooks.state."inert-review"]\nenabled = true\ntrusted_hash = "inert"\n')
                class Terminal:
                    def __init__(self, *_args):
                        launches.append(True)
                        if len(launches) == 2:
                            events.append("measure")
                            raise MeasurementReached()
                    def close(self): pass
                model = mock.Mock()
                model.server.server_port, model.requests, model.error = 43210, [], None
                model.counts, model.entry_count, model.title_count = {}, 0, 0
                model.initial_receipt_probe = {"status": "not_observed"}
                model.capture_contexts = set()
                model.admission_diagnostics, model.admission_diagnostics_saturated = [], False
                # No environment variable is changed and no process is started.
                # Only the Path returned for this read is redirected to an inert
                # fixture; all other reads/writes remain inside our temp tree.
                real_home = os.environ["HOME"]
                def fixture_path(value): return home if str(value) == real_home else Path(value)
                report = {}
                with mock.patch.dict(os.environ, {"RUNNER_TEMP": str(root.parent)}), \
                     mock.patch.object(smoke, "Path", side_effect=fixture_path), \
                     mock.patch.object(smoke, "hosted_precondition"), mock.patch.object(smoke, "require_absent"), \
                     mock.patch.object(smoke.tempfile, "mkdtemp", return_value=str(root)), \
                     mock.patch.object(smoke, "download_runtime", return_value=runtime), \
                     mock.patch.object(smoke, "bounded_run", side_effect=bounded), \
                     mock.patch.object(smoke, "Model", return_value=model), \
                     mock.patch.object(smoke, "Terminal", Terminal), mock.patch.object(smoke, "normal_trust", side_effect=trust), \
                     mock.patch.object(smoke, "observe_cpu_flags", return_value={"status":"unavailable","flags":{}}, create=True), \
                     mock.patch.object(smoke, "close_native"):
                    args = mock.Mock(tempo=str(source), helper=str(helper))
                    if invalid_install:
                        with self.assertRaisesRegex(smoke.FixtureFailure, "installed_profile_missing"):
                            smoke.run(args, report)
                        self.assertEqual(events, ["link", "install"])
                        self.assertEqual(launches, [])
                    else:
                        with self.assertRaises(MeasurementReached): smoke.run(args, report)
                        self.assertEqual(events[:7], ["link", "install", "trust", "status", "confirm", "read", "measure"])
                    self.assertEqual(report["hook_diagnostics"], {"status": "unavailable", "counts": []})
                    self.assertEqual(report["diagnostic_source"], "host_managed_stderr")
                    self.assertFalse((root / "hook-errors").exists())

    def diagnostic_profile(self):
        doc = self.profile(True)
        row = doc["hooks"][0]
        row["diagnostics"] = [{"code": "operator_declared_risk", "message": "PRIVATE-MESSAGE-CANARY"}]
        row["profile"].update(state="eligible", revision="3", diagnostic_code="operator_declared_risk")
        row["profile"]["context"]["artifacts"].extend([
            {"role": "configuration", "path": "/tmp/PRIVATE-CONFIG-CANARY", "sha256": "absent"},
            {"role": "repository", "path": "/tmp/PRIVATE-REPO-CANARY", "sha256": "c" * 64}])
        return doc

    def test_failure_profile_probe_compares_every_role_without_paths_or_hashes(self):
        doc = self.diagnostic_profile()
        confirmed = copy.deepcopy(doc["hooks"][0]["profile"])
        probe = smoke.failed_profile_probe(doc, confirmed, Path("/tmp/project"), "0.159.3")
        self.assertEqual(probe["status"], "available")
        self.assertEqual((probe["profile_state"], probe["capture_eligible"], probe["revision"]), ("eligible", True, "3"))
        self.assertTrue(probe["revision_matches"] and probe["fingerprint_matches"])
        self.assertEqual(probe["diagnostic_codes"], ["operator_declared_risk"])
        self.assertEqual(probe["inventory"], {"status": "available", "all_matches": True,
            "confirmed_count": 6, "current_count": 6,
            "roles": [{"role": role, "confirmed_count": 1, "current_count": 1, "matching_count": 1}
                      for role in ("runtime", "executable", "definitions", "skill", "configuration", "repository")]})
        serialized = json.dumps(probe)
        for private in ("/tmp/", "PRIVATE-", "a" * 64, "b" * 64, "c" * 64):
            self.assertNotIn(private, serialized)
        for index in range(6):
            drifted = copy.deepcopy(doc)
            item = drifted["hooks"][0]["profile"]["context"]["artifacts"][index]
            item["sha256"] = "d" * 64
            observed = smoke.failed_profile_probe(drifted, confirmed, Path("/tmp/project"), "0.159.3")
            self.assertFalse(observed["inventory"]["all_matches"])
            self.assertEqual(next(row for row in observed["inventory"]["roles"] if row["role"] == item["role"])["matching_count"], 0)
        reordered = copy.deepcopy(doc)
        reordered["hooks"][0]["profile"]["context"]["artifacts"].reverse()
        self.assertEqual(smoke.failed_profile_probe(reordered, confirmed, Path("/tmp/project"), "0.159.3"), probe)
        for change in ("remove", "add"):
            drifted = copy.deepcopy(doc)
            artifacts = drifted["hooks"][0]["profile"]["context"]["artifacts"]
            if change == "remove": artifacts.pop()
            else: artifacts.append({"role": "repository", "path": "/tmp/PRIVATE-NEW-CANARY", "sha256": "absent"})
            observed = smoke.failed_profile_probe(drifted, confirmed, Path("/tmp/project"), "0.159.3")["inventory"]
            self.assertFalse(observed["all_matches"])
            self.assertEqual(observed["current_count"], 5 if change == "remove" else 7)

    def test_failure_profile_probe_never_calls_retained_inventory_fresh(self):
        doc = self.diagnostic_profile()
        confirmed = copy.deepcopy(doc["hooks"][0]["profile"])
        for reason in ("inspection_failed", "pending", "not_installed"):
            bad = copy.deepcopy(doc)
            row = bad["hooks"][0]
            row["profile"].update(state="invalidated", capture_eligible=False, diagnostic_code="profile_invalidated", fingerprint="")
            if reason == "inspection_failed":
                row.update(state="unsupported", diagnostics=[{"code": "unsupported_contract", "message": "PRIVATE-ERROR"}])
            elif reason == "pending":
                row.update(state="needs_repair", pending={"request_id": "PRIVATE-REQUEST"})
            else:
                row.update(state="not_installed")
            observed = smoke.failed_profile_probe(bad, confirmed, Path("/tmp/project"), "0.159.3")
            self.assertEqual(observed["status"], "available")
            self.assertEqual(observed["inventory"], {"status": "unavailable"})
            self.assertFalse(observed["capture_eligible"])
            self.assertNotIn("PRIVATE-", json.dumps(observed))
        revoked = copy.deepcopy(doc)
        revoked["hooks"][0].update(state="approval_required", ordering="unavailable")
        revoked["hooks"][0]["profile"].update(state="revoked", revision="4", capture_eligible=False,
                                               diagnostic_code="profile_revoked", fingerprint="d" * 64)
        observed = smoke.failed_profile_probe(revoked, confirmed, Path("/tmp/project"), "0.159.3")
        self.assertEqual(observed["profile_state"], "revoked")
        self.assertFalse(observed["capture_eligible"] or observed["revision_matches"] or observed["fingerprint_matches"])
        self.assertTrue(observed["inventory"]["all_matches"])
        for mutate in (lambda row: row.update(state="PRIVATE-STATE"),
                       lambda row: row.update(diagnostics=[{"code": "PRIVATE-CODE"}]),
                       lambda row: row["profile"].update(revision="PRIVATE-REVISION"),
                       lambda row: row["profile"].update(revision="01"),
                       lambda row: row["profile"].update(revision=str(2**64)),
                       lambda row: row["profile"].update(capture_eligible=1)):
            bad = copy.deepcopy(doc); mutate(bad["hooks"][0])
            self.assertEqual(smoke.failed_profile_probe(bad, confirmed, Path("/tmp/project"), "0.159.3"),
                             {"status": "invalid_response"})

    def test_failed_run_status_is_read_only_after_join_and_cannot_replace_failure(self):
        for joined, status_fails in ((True, False), (True, True), (False, False)):
            with self.subTest(joined=joined, status_fails=status_fails), tempfile.TemporaryDirectory(prefix="tempo-install-") as tmp:
                root = Path(tmp).resolve(); home = root / "synthetic-home"; home.mkdir()
                source, helper, runtime = root / "source-tempo", root / "helper", root / "runtime"
                for path in (source, helper, runtime): path.write_text("inert build")
                project, events, launches = root / "project", [], []
                def result(confirmed):
                    doc = self.diagnostic_profile() if confirmed else self.profile(False)
                    row = doc["hooks"][0]; row["path"] = str(project)
                    row["profile"]["context"]["path"] = str(project)
                    return doc
                def bounded(argv, _env, cwd, **kwargs):
                    if argv[0] == "/usr/bin/git": return b""
                    self.assertEqual(argv[:2], [str(helper), "fixture"])
                    action = argv[2]; events.append(action)
                    self.assertEqual(cwd, project)
                    if action == "link": return b""
                    if action == "read": return json.dumps({"receipts": []}).encode()
                    self.assertEqual(argv[-4:], [str(project), str(runtime), str(root / "tempo"), "user"])
                    if action == "install":
                        _, _, definitions = self.definitions(root, "codex")
                        (home / ".codex/hooks.json").write_text(json.dumps(definitions))
                    if "joined" in events:
                        self.assertEqual(action, "status")
                        self.assertEqual(events[-2], "joined")
                        self.assertGreater(kwargs["timeout"], 0)
                        self.assertLessEqual(kwargs["timeout"], 20)
                        if status_fails: raise RuntimeError("PRIVATE-HELPER-STDERR")
                        return json.dumps(result(True)).encode()
                    return json.dumps(result(action == "confirm")).encode()
                class Terminal:
                    def __init__(self, *_args):
                        launches.append(True)
                        if len(launches) == 2: raise smoke.FixtureFailure("measured_session_ambiguous")
                    def close(self): pass
                def close_native(*_args, **kwargs):
                    self.assertIs(kwargs["primary_failure"], True)
                    events.append("joined")
                    return joined
                model = mock.Mock()
                model.server.server_port, model.requests, model.error = 43210, [], None
                model.counts, model.entry_count, model.title_count = {}, 0, 0
                model.initial_receipt_probe = {"status": "not_observed"}
                model.capture_contexts = set()
                model.admission_diagnostics, model.admission_diagnostics_saturated = [], False
                real_home = os.environ["HOME"]
                def fixture_path(value): return home if str(value) == real_home else Path(value)
                report = {}
                with mock.patch.dict(os.environ, {"RUNNER_TEMP": str(root.parent)}), \
                     mock.patch.object(smoke, "Path", side_effect=fixture_path), \
                     mock.patch.object(smoke, "hosted_precondition"), mock.patch.object(smoke, "require_absent"), \
                     mock.patch.object(smoke.tempfile, "mkdtemp", return_value=str(root)), \
                     mock.patch.object(smoke, "download_runtime", return_value=runtime), \
                     mock.patch.object(smoke, "bounded_run", side_effect=bounded), \
                     mock.patch.object(smoke, "Model", return_value=model), \
                     mock.patch.object(smoke, "Terminal", Terminal), mock.patch.object(smoke, "normal_trust"), \
                     mock.patch.object(smoke, "observe_cpu_flags", return_value={"status":"unavailable","flags":{}}, create=True), \
                     mock.patch.object(smoke, "close_native", side_effect=close_native):
                    with self.assertRaisesRegex(smoke.FixtureFailure, "^measured_session_ambiguous$"):
                        smoke.run(mock.Mock(tempo=str(source), helper=str(helper)), report)
                self.assertEqual(events[:5], ["link", "install", "status", "confirm", "read"])
                self.assertEqual(events.count("status"), 2 if joined else 1)
                expected = "helper_failed" if status_fails else "available" if joined else "cleanup_incomplete"
                self.assertEqual(report["failure_profile_probe"]["status"], expected)
                self.assertEqual(report["initial_receipt_probe"], {"status": "not_observed"})
                self.assertEqual(report["hook_diagnostics"], {"status": "unavailable", "counts": []})
                self.assertFalse((root / "hook-errors").exists())
                self.assertNotIn("PRIVATE-", json.dumps(report))


    def definitions(self, root, host):
        events = smoke.EVENT_ORDER if host == "codex" else (
            "SessionStart", "SessionEnd", "UserPromptSubmit", "Stop", "SubagentStart", "SubagentStop",
            "PreToolUse", "PostToolUse", "PostToolUseFailure", "PermissionRequest", "StopFailure", "TaskCreated", "TaskCompleted")
        command = "'" + str(root / "tempo") + "' hook " + host + " --input-stdin"
        return events, command, {"hooks": {event: [{"hooks": [{"type": "command", "command": command, "timeout": 2}]}] for event in events}}

    def test_exact_production_commands_and_full_inventory_without_redirection(self):
        with tempfile.TemporaryDirectory(prefix="tempo-install-") as tmp:
            root = Path(tmp)
            for host in ("codex", "claude"):
                events, command, doc = self.definitions(root, host)
                path = root / "settings.json"
                path.write_text(json.dumps(doc))
                self.assertEqual(smoke.require_installed_definitions(path, root / "tempo", host, events), command)
                self.assertNotIn("2>>", command)
                self.assertFalse((root / "hook-errors").exists())

    def test_installed_definitions_reject_missing_extra_edited_async_or_grants(self):
        with tempfile.TemporaryDirectory(prefix="tempo-install-") as tmp:
            root = Path(tmp); events, _, doc = self.definitions(root, "codex")
            mutations = (
                lambda d: d["hooks"].pop("Stop"),
                lambda d: d["hooks"].update(ForeignEvent=[]),
                lambda d: d["hooks"]["Stop"].append(copy.deepcopy(d["hooks"]["Stop"][0])),
                lambda d: d["hooks"]["Stop"][0]["hooks"][0].update(command="/tmp/other hook codex --input-stdin"),
                lambda d: d["hooks"]["Stop"][0]["hooks"][0].update(command="'" + str(root / "tempo") + "' hook codex --input-stdin 2>> /tmp/log"),
                lambda d: d["hooks"]["Stop"][0]["hooks"][0].update(**{"async": True}),
                lambda d: d["hooks"]["Stop"][0]["hooks"][0].update(timeout=1),
                lambda d: d.update(permissions={"allow": ["*"]}),
            )
            for mutate in mutations:
                candidate = copy.deepcopy(doc); mutate(candidate)
                path = root / "settings.json"; path.write_text(json.dumps(candidate))
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.require_installed_definitions(path, root / "tempo", "codex", events)
            # Duplicate JSON fields cannot conceal an edited definition.
            path.write_text('{"hooks":{},"hooks":' + json.dumps(doc["hooks"]) + '}')
            with self.assertRaises(smoke.FixtureFailure):
                smoke.require_installed_definitions(path, root / "tempo", "codex", events)

    def profile(self, confirmed):
        return {"contract_version": 1, "hooks": [{"host": "codex", "scope": "user", "path": "/tmp/project", "runtime_version": "0.159.3",
            "state": "awaiting_real_event" if confirmed else "approval_required", "ordering": "supported" if confirmed else "unavailable",
            "last_real_event": None, "profile": {"basis": "operator_declared" if confirmed else "none", "capture_eligible": confirmed,
                "fingerprint": "a" * 64, "declaration_version": "tempo-native-hooks-v1", "context": {"host": "codex", "scope": "user",
                "path": "/tmp/project", "runtime_version": "0.159.3", "surface": "local", "inventory_version": "tempo-installed-static-v1",
                "artifacts": [{"role": role, "path": "/tmp/" + role, "sha256": "b" * 64}
                              for role in ("runtime", "executable", "definitions", "skill")], "conflicts": []}}}]}

    def test_static_and_confirmed_profile_never_claim_receiving_or_real_event(self):
        for confirmed in (False, True):
            doc = self.profile(confirmed)
            result = smoke.require_installed_profile(doc, "codex", "user", Path("/tmp/project"), "0.159.3", confirmed)
            self.assertEqual(result["fingerprint"], "a" * 64)
            for mutate in (
                lambda d: d["hooks"][0].update(state="receiving"),
                lambda d: d["hooks"][0].update(last_real_event="2026-10-02T12:00:00Z"),
                lambda d: d["hooks"][0].update(scope="project"),
                lambda d: d["hooks"][0]["profile"]["context"].update(inventory_version=""),
                lambda d: d["hooks"][0]["profile"]["context"].update(conflicts=["foreign"]),
                lambda d: d["hooks"][0]["profile"].update(basis="host_observed"),
                lambda d: d["hooks"][0]["profile"].update(capture_eligible=not confirmed),
            ):
                bad = copy.deepcopy(doc); mutate(bad)
                with self.assertRaises(smoke.FixtureFailure):
                    smoke.require_installed_profile(bad, "codex", "user", Path("/tmp/project"), "0.159.3", confirmed)

    def test_complete_inventory_comparison_detects_all_roles_and_absent_source_arrival(self):
        original = self.profile(True)["hooks"][0]["profile"]
        original["context"]["artifacts"].append({"role": "configuration", "path": "/tmp/missing", "sha256": "absent"})
        self.assertEqual(smoke.require_unchanged_profile(original, copy.deepcopy(original)),
                         {"available": True, "all_matches": True, "artifact_count": 5, "sampled_by": "production_status"})
        for index in range(5):
            drifted = copy.deepcopy(original)
            drifted["context"]["artifacts"][index]["sha256"] = "c" * 64
            with self.assertRaisesRegex(smoke.FixtureFailure, "installed_profile_drift"):
                smoke.require_unchanged_profile(original, drifted)
        drifted = copy.deepcopy(original)
        drifted["context"]["artifacts"].append({"role": "repository", "path": "/tmp/new-boundary", "sha256": "c" * 64})
        with self.assertRaisesRegex(smoke.FixtureFailure, "installed_profile_drift"):
            smoke.require_unchanged_profile(original, drifted)



class TerminalFailureProbeTests(unittest.TestCase):
    """Old-API controls: real run/finally/close_native, no external effects."""

    def run_boundary(self, cached=0, *, success=False, close_failure=None,
                     missing=None, labels=None, raising_getter=False):
        from contextlib import ExitStack, nullcontext
        from types import SimpleNamespace
        import io
        import stat

        case = self
        events, launches, actions, memory, blocked_effects = [], [], [], {}, []
        primary = smoke.FixtureFailure("host_exited_early")
        report = {"status": "failed", "host": "codex"}
        command = "'/fixture/root/tempo' hook codex --input-stdin"
        definitions = json.dumps({"hooks": {event: [{"hooks": [
            {"type": "command", "command": command, "timeout": 2}]}]
            for event in smoke.INSTALLED_EVENTS}}).encode()

        def forbidden(*_args, **_kwargs):
            blocked_effects.append("external_effect")
            raise AssertionError("unexpected external effect")

        class MemoryPath:
            def __init__(self, value):
                self.value = str(value)
            def __str__(self):
                return self.value
            def __truediv__(self, part):
                return MemoryPath(self.value.rstrip("/") + "/" + str(part))
            @property
            def parent(self):
                return MemoryPath(self.value.rsplit("/", 1)[0])
            def with_name(self, name):
                return self.parent / name
            def resolve(self, strict=False):
                return self
            def is_file(self):
                return True
            def is_absolute(self):
                return self.value.startswith("/")
            def mkdir(self, **_kwargs):
                return None
            def chmod(self, _mode):
                return None
            def write_text(self, value):
                memory[self.value] = value
                return len(value)
            def read_text(self):
                if self.value.endswith("/codex-0.159.3.json"):
                    return json.dumps({"version": "0.159.3", "sha256": "a" * 64})
                case.assertIn(self.value, memory)
                return memory[self.value]

        class Source(io.BytesIO):
            def fileno(self):
                return 911

        def open_definitions(path, flags):
            case.assertEqual(str(path), "/fixture/home/.codex/hooks.json")
            case.assertEqual(flags, smoke.os.O_RDONLY | smoke.os.O_NOFOLLOW | smoke.os.O_NONBLOCK)
            return 911

        def fdopen(fd, mode):
            case.assertEqual((fd, mode), (911, "rb"))
            return Source(definitions)

        def profile(confirmed):
            value = InstallIntegrationTests.profile(case, confirmed)
            value["hooks"][0]["path"] = "/fixture/root/project"
            value["hooks"][0]["profile"]["context"]["path"] = "/fixture/root/project"
            return value

        def receipt(kind, actor, **extra):
            return {"id": kind, "kind": kind, "source": "codex", "origin": "unverified",
                    "session_id": "session", "turn_id": "parent-turn", "agent_id": "",
                    "disposition": "applied", "durability": "committed", "ordering": "supported",
                    "profile_basis": "operator_declared", "actor": actor, **extra}

        parent, child, interrupt = {"id": "parent"}, {"id": "child"}, {"id": "interrupt"}
        rows = [receipt(kind, parent) for kind in ("SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse", "Stop")]
        rows += [receipt("SubagentStart", child, agent_id="child", turn_id="child-turn"),
                 receipt("SubagentStop", child, agent_id="child", turn_id="child-turn"),
                 receipt("Interrupt", interrupt, turn_id="interrupt-turn"),
                 receipt("SessionEnd", interrupt, turn_id="interrupt-turn", disposition="stale")]
        detail = {"actor": interrupt, "reason": "source_lost", "state": "unresolved",
                  "bounded": True, "resolution_present": False, "discarded": False}
        read_count = 0

        def bounded(argv, _env, _cwd, **_kwargs):
            nonlocal read_count
            if argv[0] == "/usr/bin/git":
                case.assertEqual(argv[1:], ["-c", "credential.helper=", "init", "-q", "/fixture/root/project"])
                return b""
            case.assertEqual(argv[:2], ["/fixture/helper", "fixture"])
            action = argv[2]
            actions.append(action)
            if action == "link":
                return b""
            if action == "install" and missing == "terminal":
                raise primary
            if action in ("install", "status", "confirm"):
                return json.dumps(profile(action != "install" and (action == "confirm" or len(launches) == 2))).encode()
            case.assertEqual(action, "read")
            read_count += 1
            if read_count == 1:
                return b'{"receipts":[]}'
            final = read_count >= 5
            actor_rows = [{"ref": parent, "state": "wait_user"}, {"ref": child, "state": "working"},
                          {"ref": interrupt, "state": "interrupted"}]
            return json.dumps({"receipts": rows, "actors": actor_rows, "queued": 2,
                               "uncertainties": 1 if final else 0,
                               "uncertainty_details": [detail] if final else [],
                               "capture_reviews": 0}).encode()

        class Event:
            def is_set(self):
                return True
            def set(self):
                events.append("child_release")

        model = SimpleNamespace(server=SimpleNamespace(server_port=43210), requests=[], error=None,
            counts={"parent": 3, "child": 1, "interrupt": 1} if success else {}, entry_count=0, title_count=0,
            initial_receipt_probe={"status": "not_observed"}, capture_contexts=set(),
            admission_diagnostics=[], admission_diagnostics_saturated=False, lock=nullcontext(),
            baseline=None, session=None, phase="initial", child="child", child_turn="child-turn",
            interrupt_turn="interrupt-turn", child_seen=Event(), child_release=Event(), interrupt_seen=Event())

        class Proc:
            def __init__(self):
                self.cached = cached
            @property
            def returncode(self):
                events.append("cached_status_read")
                if raising_getter:
                    raise RuntimeError("PRIVATE-GETTER")
                return self.cached
            @returncode.setter
            def returncode(self, value):
                self.cached = value
            def poll(self):
                if not success:
                    return forbidden()
                events.append("normal_exit_poll")
                return self.cached
            wait = terminate = kill = forbidden

        class Terminal:
            def __init__(self, *_args):
                launches.append(self)
                if missing != "proc":
                    self.proc = Proc() if missing != "returncode" else SimpleNamespace()
                if labels is None:
                    self.last_wait, self.last_input_action = "browser_input_unavailable", "end"
                elif labels != "absent":
                    self.last_wait, self.last_input_action = labels
            def close(self):
                events.append("terminal_close")
                if missing not in ("proc", "returncode"):
                    self.proc.cached = -15 if type(cached) is int and cached == -9 else -9
                if close_failure in ("terminal", "both"):
                    raise RuntimeError("PRIVATE-TERMINAL-CLEANUP")
            def command(self, value):
                if value == smoke.PARENT_PROMPT:
                    model.session = "session"
                elif value == "/quit":
                    self.proc.cached = 0
                else:
                    case.assertEqual(value, smoke.INTERRUPT_PROMPT)
            def send(self, value):
                case.assertEqual(value, b"\x1b")
            def until(self, predicate, _code, seconds=20):
                case.assertTrue(predicate(""))
                return ""

        def model_close():
            events.append("model_close")
            if close_failure in ("model", "both"):
                raise RuntimeError("PRIVATE-MODEL-CLEANUP")
        model.close = model_close

        def trust(terminal, repo, actual_command, trusted, _workspace, _startup, probes):
            events.append("normal_trust")
            case.assertIs(terminal, launches[0])
            case.assertEqual((str(repo), actual_command), ("/fixture/root/project", command))
            if success:
                trusted.extend(smoke.EVENT_ORDER)
                return
            probes["browser"] = {"inventory_caption_present": True,
                                 "inventory_first_row": True, "inventory_last_row": False}
            raise primary

        fake_os = SimpleNamespace(environ={"HOME": "/fixture/home", "RUNNER_TEMP": "/fixture"},
            getuid=lambda: 123, open=open_definitions, fdopen=fdopen,
            fstat=lambda fd: SimpleNamespace(st_mode=stat.S_IFREG | 0o600, st_size=len(definitions)),
            O_RDONLY=os.O_RDONLY, O_NOFOLLOW=os.O_NOFOLLOW, O_NONBLOCK=os.O_NONBLOCK,
            read=forbidden, write=forbidden, close=forbidden, kill=forbidden, killpg=forbidden)
        env = {"TEMPO_STATE": "/fixture/state", "TEMPO_HOOK_STATE": "/fixture/policy"}
        caught = None
        with ExitStack() as stack:
            replacements = {"Path": MemoryPath, "os": fake_os,
                "platform": SimpleNamespace(system=lambda: "Linux", machine=lambda: "x86_64"),
                "pwd": SimpleNamespace(getpwuid=lambda _: SimpleNamespace(pw_dir="/fixture/home")),
                "time": SimpleNamespace(monotonic=lambda: 100.0, sleep=forbidden),
                "tempfile": SimpleNamespace(mkdtemp=lambda **_: "/fixture/root"),
                "hosted_precondition": lambda *_: None, "require_absent": lambda *_: None,
                "child_environment": lambda *_: env,
                "download_runtime": lambda *_: MemoryPath("/fixture/runtime"),
                "digest": lambda *_: "b" * 64, "bounded_run": bounded, "Model": lambda *_: model,
                "Terminal": Terminal, "normal_trust": trust,
                "observe_cpu_flags": lambda _: {"status": "unavailable", "flags": {}},
                "artifact_match_probe": lambda _: {"available": True}}
            for name, value in replacements.items():
                stack.enter_context(mock.patch.object(smoke, name, value))
            stack.enter_context(mock.patch.object(smoke.shutil, "copy2", return_value=None))
            for owner, name in ((smoke.subprocess, "Popen"), (smoke.subprocess, "run"),
                                (smoke.pty, "openpty"), (smoke.threading.Thread, "start"),
                                (smoke.http.server, "ThreadingHTTPServer"),
                                (smoke.urllib.request, "urlopen")):
                stack.enter_context(mock.patch.object(owner, name, side_effect=forbidden))
            try:
                smoke.run(SimpleNamespace(tempo="/fixture/source-tempo", helper="/fixture/helper"), report)
            except smoke.FixtureFailure as exc:
                caught = exc
            if success:
                self.assertIsNone(caught)
                self.assertEqual((report["status"], report["stage"]), ("passed", "complete"))
                self.assertEqual(events.count("terminal_close"), 2)
                self.assertEqual(events.count("model_close"), 1)
                self.assertEqual(events.count("normal_exit_poll"), 1)
                self.assertEqual(read_count, 6)
            else:
                self.assertIs(caught, primary)
                self.assertEqual(caught.args, ("host_exited_early",))
                self.assertEqual(report["status"], "failed")
                expected_stage = "production_install" if missing == "terminal" else "normal_trust_ui"
                self.assertEqual(report["stage"], expected_stage)
                self.assertEqual(actions, ["link", "install"])
                self.assertEqual(len(launches), 0 if missing == "terminal" else 1)
                self.assertEqual(events.count("normal_trust"), 0 if missing == "terminal" else 1)
                closes = [event for event in events if event in ("terminal_close", "model_close")]
                self.assertEqual(closes, ["model_close"] if missing == "terminal" else ["terminal_close", "model_close"])
                self.assertNotIn("receipts", report)
                self.assertNotIn("profile_fingerprint", report)
                if missing != "terminal":
                    self.assertEqual(report["trusted_events"], [])
                self.assertEqual(report["initial_receipt_probe"], {"status": "not_observed"})
                self.assertEqual(report["failure_profile_probe"]["status"],
                                 "cleanup_incomplete" if close_failure else "unconfirmed")
            self.assertEqual(blocked_effects, [])
            self.assertNotIn("PRIVATE-", json.dumps(report))
        return report, events

    @staticmethod
    def projection(status, termination="unavailable", returncode=None, signal_number=None,
                   wait="browser_input_unavailable", action="end"):
        return {"source": "cached_process_status_before_cleanup", "status": status,
                "termination": termination, "returncode": returncode, "signal": signal_number,
                "last_wait": wait, "last_input_action": action}

    def test_failed_run_cached_status_is_copied_before_cleanup(self):
        for label, value, termination, signal_number in (
                ("zero", 0, "exit", None), ("one", 1, "exit", None),
                ("exit137", 137, "exit", None), ("signal9", -9, "signal", 9),
                ("signal15", -15, "signal", 15), ("unobserved", None, "unavailable", None)):
            with self.subTest(case=label):
                report, events = self.run_boundary(value)
                status = "not_observed" if value is None else "observed"
                # Baseline failure, stage, cleanup and absent-receipt witnesses
                # have already passed. Old source fails only this observation.
                self.assertEqual(report.get("terminal_failure_probe"),
                                 self.projection(status, termination, value, signal_number))
                self.assertEqual(events.count("cached_status_read"), 1)
                self.assertLess(events.index("cached_status_read"), events.index("terminal_close"))

    def test_failed_run_rejects_nonprimitive_status_without_formatting(self):
        class Trap:
            calls = 0
            def forbidden(self, *_):
                self.calls += 1
                raise AssertionError("private object was inspected")
            __str__ = __repr__ = __int__ = __index__ = __hash__ = __eq__ = forbidden
        trap = Trap()
        for label, value in (("bool", True), ("string", "PRIVATE-STATUS"),
                             ("too_large", 256), ("too_small", -256), ("object", trap)):
            with self.subTest(case=label):
                report, _ = self.run_boundary(value)
                self.assertEqual(trap.calls, 0)
                self.assertEqual(report.get("terminal_failure_probe"), self.projection("invalid_status"))

    def test_failed_run_metadata_fallback_and_cleanup_preserve_primary(self):
        for label, options, expected in (
                ("terminal_close", {"cached": -9, "close_failure": "terminal"}, self.projection("observed", "signal", -9, 9)),
                ("model_close", {"cached": -9, "close_failure": "model"}, self.projection("observed", "signal", -9, 9)),
                ("both_close", {"cached": -9, "close_failure": "both"}, self.projection("observed", "signal", -9, 9)),
                ("getter", {"raising_getter": True}, self.projection("unavailable", wait="none", action="none")),
                ("no_terminal", {"missing": "terminal"}, self.projection("unavailable", wait="none", action="none")),
                ("no_proc", {"missing": "proc"}, self.projection("unavailable", wait="none", action="none")),
                ("no_returncode", {"missing": "returncode"}, self.projection("unavailable", wait="none", action="none")),
                ("no_labels", {"labels": "absent"}, self.projection("observed", "exit", 0, wait="none", action="none"))):
            with self.subTest(case=label):
                report, _ = self.run_boundary(**options)
                self.assertEqual(report.get("terminal_failure_probe"), expected)

    def test_failed_run_label_projection_rejects_custom_objects(self):
        class Trap:
            calls = 0
            def forbidden(self, *_):
                self.calls += 1
                raise AssertionError("private label was inspected")
            __str__ = __repr__ = __hash__ = __eq__ = forbidden
        class StringSubclass(str):
            calls = 0
            def __hash__(self):
                type(self).calls += 1
                raise AssertionError("string subclass hashed")
            def __eq__(self, _):
                type(self).calls += 1
                raise AssertionError("string subclass compared")
        trap = Trap()
        for label, value in (("unknown", "PRIVATE-LABEL"), ("unhashable", []),
                             ("object", trap), ("subclass", StringSubclass("end"))):
            with self.subTest(case=label):
                report, _ = self.run_boundary(labels=(value, value))
                self.assertEqual(trap.calls, 0)
                self.assertEqual(StringSubclass.calls, 0)
                self.assertEqual(report.get("terminal_failure_probe"),
                                 self.projection("observed", "exit", 0, wait="other", action="other"))

    def test_normal_return_omits_failure_probe(self):
        report, _ = self.run_boundary(success=True)
        self.assertNotIn("terminal_failure_probe", report)

    def test_existing_until_retains_poll_predicate_and_timeout_order(self):
        from types import SimpleNamespace
        for label, value in (("zero", 0), ("one", 1), ("signal", -9)):
            with self.subTest(case=label):
                terminal = smoke.Terminal.__new__(smoke.Terminal)
                terminal.deadline, terminal.screen = 20, smoke.Screen()
                terminal.pump = mock.Mock()
                terminal.proc = SimpleNamespace(returncode=None)
                def poll():
                    terminal.proc.returncode = value
                    return value
                terminal.proc.poll = mock.Mock(side_effect=poll)
                with mock.patch.object(smoke.time, "monotonic", return_value=1):
                    with self.assertRaisesRegex(smoke.FixtureFailure, "^host_exited_early$"):
                        terminal.until(lambda _: False, "browser_input_unavailable", seconds=2)
                terminal.pump.assert_called_once_with()
                terminal.proc.poll.assert_called_once_with()
                self.assertEqual(terminal.proc.returncode, value)
                self.assertEqual(getattr(terminal, "last_wait", None), "browser_input_unavailable")
        for label in ("predicate_first", "normal_exit", "timeout"):
            with self.subTest(case=label):
                terminal = smoke.Terminal.__new__(smoke.Terminal)
                terminal.deadline, terminal.screen = 20, smoke.Screen()
                terminal.pump = mock.Mock()
                terminal.proc = mock.Mock(returncode=None)
                terminal.proc.poll.return_value = 0 if label == "normal_exit" else None
                clock = [0, 1, 3] if label == "timeout" else [0, 1]
                with mock.patch.object(smoke.time, "monotonic", side_effect=clock):
                    if label == "timeout":
                        with self.assertRaisesRegex(smoke.FixtureFailure, "^browser_input_unavailable$"):
                            terminal.until(lambda _: False, "browser_input_unavailable", seconds=2)
                    else:
                        predicate = (lambda _: True) if label == "predicate_first" else (lambda _: terminal.proc.poll() is not None)
                        terminal.until(predicate, "normal_exit_missing", seconds=2)
                self.assertEqual(terminal.proc.poll.call_count, 0 if label == "predicate_first" else 1)
                terminal.pump.assert_called_once_with()

    def test_existing_send_keeps_bytes_and_ignores_query_reply_labels(self):
        pairs = [(b"\r", "enter"), (b"\x1b", "escape"), (b"\x1b[A", "up"),
                 (b"\x1b[B", "down"), (b"\x1b[F", "end"), (b"\x1b[H", "home"),
                 (b"1", "review_shortcut"), (b"t", "trust_shortcut"),
                 (b"\x1b[200~PRIVATE-PASTE\x1b[201~", "command_paste"), (b"\xffPRIVATE-BYTES", "other")]
        for index, (data, expected) in enumerate(pairs):
            with self.subTest(case=index):
                terminal = smoke.Terminal.__new__(smoke.Terminal)
                terminal.master, terminal.last_input_action = 987, "none"
                with mock.patch.object(smoke.os, "write", return_value=len(data)) as write:
                    terminal.send(data)
                write.assert_called_once_with(987, data)
                self.assertEqual(getattr(terminal, "last_input_action", None), expected)
        terminal = smoke.Terminal.__new__(smoke.Terminal)
        terminal.master, terminal.last_input_action = 987, "none"
        payloads = [b"\x1b[F", b"\x1b[1;1R", b"\x1b[?1;2c", b"\x1b[>0;0;0c"]
        with mock.patch.object(smoke.os, "write") as write:
            for data in payloads:
                terminal.send(data)
            self.assertEqual(write.call_args_list, [mock.call(987, data) for data in payloads])
        self.assertEqual(getattr(terminal, "last_input_action", None), "end")


if __name__ == "__main__":
    unittest.main()
