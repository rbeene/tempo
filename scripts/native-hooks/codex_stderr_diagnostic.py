"""Auxiliary, explicitly non-acceptance observation using a tagged test binary.

The production installer inventories that actual binary. The original smoke
runner owns every host/helper/model; no command, trust, PTY or budget is changed.
Only typed stderr summaries from the test executable may leave the private sink.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
import tempfile


LIMIT = 256
SLOTS = 128
ENV_KEYS = ("TEMPO_NATIVE_DIAGNOSTIC", "TEMPO_NATIVE_DIAGNOSTIC_DIR",
            "TEMPO_NATIVE_DIAGNOSTIC_DEV", "TEMPO_NATIVE_DIAGNOSTIC_INO")
CODES = frozenset(("validation", "unsupported_contract", "state_corrupt", "state_busy",
    "local_write_unknown", "clock_unavailable", "clock_conflict", "binding_unavailable",
    "event_conflict", "event_gap", "ordering_unavailable", "profile_required",
    "profile_invalidated", "profile_revoked", "untracked", "review_required", "source_lost",
    "restart_unknown", "incomplete_wait", "source_loss_while_waiting", "internal"))
DURABILITIES = frozenset(("committed", "not_committed", "unknown"))
ROW_KEYS = frozenset(("schema_version", "diagnostic_only", "acceptance_eligible", "status",
                      "code", "durability", "exit_code"))
FAILURES = frozenset(("measured_session_ambiguous", "child_start_barrier_missing",
    "parent_stop_missing", "child_stop_missing", "prompt_receipt_missing",
    "overall_deadline", "fixture_cancelled", "normal_exit_missing",
    "unexpected_pretrust_inference", "installed_definitions_mismatch",
    "installed_profile_missing", "provider_failed"))
STAGES = frozenset(("hosted_preconditions", "pinned_runtime_download", "production_link",
    "production_install", "normal_trust_ui", "production_policy_confirmation",
    "measured_restart", "parent_plan_and_child", "child_completion", "ordinary_interrupt",
    "normal_session_end", "complete"))


class DiagnosticUnavailable(Exception):
    """Fixed category only; never carries a path or underlying exception."""


def require(value):
    if not value:
        raise DiagnosticUnavailable("diagnostic_records_unavailable")


def unique_fields(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result)
        result[key] = value
    return result


def typed_row(data):
    require(len(data) <= LIMIT)
    try:
        row = json.loads(data, object_pairs_hook=unique_fields)
    except (ValueError, UnicodeError):
        raise DiagnosticUnavailable("diagnostic_records_unavailable") from None
    require(isinstance(row, dict) and set(row) == ROW_KEYS)
    require(type(row["schema_version"]) is int and row["schema_version"] == 1
            and row["diagnostic_only"] is True and row["acceptance_eligible"] is False
            and type(row["exit_code"]) is int and 0 <= row["exit_code"] <= 255)
    require(type(row["status"]) is str and type(row["code"]) is str
            and type(row["durability"]) is str)
    if row["status"] == "safe_error":
        require(row["code"] in CODES and row["durability"] in DURABILITIES)
    else:
        require(row["status"] in ("empty", "unavailable")
                and row["code"] == "" and row["durability"] == "")
    return row


class Sink:
    """One parent-owned private inode and a finite exclusive child namespace."""
    def __init__(self, parent):
        self.fd = None
        self.path = Path(tempfile.mkdtemp(prefix="tempo-stderr-diagnostic-", dir=parent)).resolve()
        try:
            self.fd = os.open(self.path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            self.info = os.fstat(self.fd)
            require(stat.S_ISDIR(self.info.st_mode) and stat.S_IMODE(self.info.st_mode) == 0o700
                    and self.info.st_uid == os.getuid())
        except BaseException:
            if self.fd is not None:
                owned, self.fd = self.fd, None
                os.close(owned)
            raise

    def environment(self):
        return dict(zip(ENV_KEYS, ("1", str(self.path), str(self.info.st_dev), str(self.info.st_ino))))

    def verify(self):
        require(self.fd is not None)
        current = os.fstat(self.fd)
        named = os.stat(self.path, follow_symlinks=False)
        for value in (current, named):
            require(stat.S_ISDIR(value.st_mode) and stat.S_IMODE(value.st_mode) == 0o700
                    and value.st_uid == os.getuid()
                    and (value.st_dev, value.st_ino) == (self.info.st_dev, self.info.st_ino))

    def names(self):
        names = []
        with os.scandir(self.fd) as entries:
            for entry in entries:
                require(len(names) < SLOTS and re.fullmatch(r"r(?:0[0-9][0-9]|1[01][0-9]|12[0-7])\.json", entry.name))
                names.append(entry.name)
        return sorted(names)

    def collect(self):
        self.verify()
        counts, empty, unavailable, total = {}, 0, 0, 0
        for name in self.names():
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self.fd)
            try:
                before = os.fstat(fd)
                require(stat.S_ISREG(before.st_mode) and stat.S_IMODE(before.st_mode) == 0o600
                        and before.st_uid == os.getuid() and before.st_nlink == 1 and 0 < before.st_size <= LIMIT)
                data = os.read(fd, LIMIT + 1)
                after = os.fstat(fd)
                require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                        == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
                        and len(data) == before.st_size)
                row = typed_row(data)
            finally:
                os.close(fd)
            total += 1
            if row["status"] == "safe_error":
                key = (row["code"], row["durability"])
                counts[key] = counts.get(key, 0) + 1
            elif row["status"] == "empty":
                empty += 1
            else:
                unavailable += 1
        self.verify()
        return {"status": "observed" if total else "no_records", "record_count": total, "empty_count": empty,
                "unclassified_count": unavailable, "saturated": total == SLOTS,
                "counts": [{"code": code, "durability": durability, "count": count}
                           for (code, durability), count in sorted(counts.items())]}

    def close(self):
        # Typed private records stay in the ephemeral owned runner directory.
        # Never recursively remove a path after a failed identity/cleanup check.
        # The artifact upload explicitly excludes this directory.
        if self.fd is not None:
            owned, self.fd = self.fd, None
            os.close(owned)


def observe(smoke, args, sink):
    scratch = {}
    result = {"schema_version": 1, "diagnostic_only": True, "acceptance_eligible": False,
              "host": "codex", "executable_origin": "instrumented_go_test_executable",
              "status": "failed", "failure_category": "harness_failed", "stage": "unknown",
              "owners_joined": False, "completeness": "not_established",
              "event_attribution": "unavailable",
              "stderr_observation": {"status": "unavailable", "reason": "owner_cleanup_unconfirmed"}}
    old_environment, old_close = smoke.child_environment, smoke.close_native
    joined = False

    def environment(parent, root):
        env = old_environment(parent, root)
        env.update(sink.environment())
        return env

    def close_native(*positional, **keywords):
        nonlocal joined
        joined = old_close(*positional, **keywords)
        return joined

    smoke.child_environment, smoke.close_native = environment, close_native
    try:
        try:
            smoke.run(args, scratch)
            result["status"], result["failure_category"] = "completed", "none"
        except Exception as exc:
            category = str(exc) if isinstance(exc, smoke.FixtureFailure) else "harness_failed"
            result["failure_category"] = category if category in FAILURES else "other_fixture_failure"
    finally:
        smoke.child_environment, smoke.close_native = old_environment, old_close
    result["owners_joined"] = joined is True
    result["stage"] = scratch.get("stage") if scratch.get("stage") in STAGES else "unknown"
    # Hashes identify the actual installed test executable and runtime; no path,
    # raw smoke report, payload, request, terminal text or profile ID is exported.
    for key in ("tempo_sha256", "runtime_sha256"):
        value = scratch.get(key)
        if isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value):
            result[key] = value
    if joined is True:
        try:
            result["stderr_observation"] = sink.collect()
        except Exception:
            result["stderr_observation"] = {"status": "unavailable", "reason": "diagnostic_records_unavailable"}
    return result


def load_smoke():
    spec = importlib.util.spec_from_file_location("codex_smoke", Path(__file__).with_name("codex_smoke.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tempo", required=True)
    parser.add_argument("--helper", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    smoke = load_smoke()
    sink, result = None, None
    def alarm(*_): raise smoke.FixtureFailure("overall_deadline")
    def cancelled(*_): raise smoke.FixtureFailure("fixture_cancelled")
    old_handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}
    try:
        signal.signal(signal.SIGALRM, alarm)
        signal.signal(signal.SIGTERM, cancelled)
        signal.signal(signal.SIGINT, cancelled)
        signal.alarm(250)
        sink = Sink(os.environ["RUNNER_TEMP"])
        result = observe(smoke, args, sink)
    except Exception:
        if result is None:
            result = {"schema_version": 1, "diagnostic_only": True, "acceptance_eligible": False,
                      "status": "failed", "failure_category": "diagnostic_setup_or_cleanup_failed",
                      "completeness": "not_established"}
    finally:
        signal.alarm(0)
        try:
            if sink is not None:
                try:
                    sink.close()
                except Exception:
                    result["sink_cleanup"] = "unavailable"
        finally:
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)
    try:
        Path(args.evidence).write_text(json.dumps(result, indent=2) + "\n")
    except Exception:
        print("native_codex_stderr_diagnostic: evidence_unavailable")
        return 1
    print("native_codex_stderr_diagnostic: non_acceptance")
    # Completing diagnostic collection never produces an acceptance PASS.
    return 0 if result.get("stderr_observation", {}).get("status") == "observed" and "sink_cleanup" not in result else 1


if __name__ == "__main__":
    sys.exit(main())
