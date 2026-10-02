"""Finite Claude application acceptance, only on the clean hosted Linux job.

Import is inert. No callback payload is authored here: real Claude invokes the
built Tempo command. Raw provider/output/configuration data is never exported.
"""
import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path, PurePosixPath
import platform
import pwd
import re
import shutil
import signal
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request

# Generic safety primitives only; never reuse Codex protocol/configuration.
from codex_smoke import (FixtureFailure, require, require_absent, digest,
                         bounded_run, hook_diagnostic_probe)

VERSION = '2.1.286'
MODEL = 'claude-sonnet-4-6'
TOKEN = 'tempo-ci-invalid-synthetic-key'
PARENT_PROMPT = 'tempo-native-parent-case'
CHILD_PROMPT = 'tempo-native-child-case'
READ_RESULT = 'tempo-fixture-read-ok'
EVENTS = ('SessionStart', 'UserPromptSubmit', 'PreToolUse', 'PostToolUse',
          'SubagentStart', 'SubagentStop', 'Stop', 'SessionEnd')


def hosted_precondition(env, system, machine, user_home):
    require(system == 'Linux' and machine == 'x86_64', 'hosted_platform_required')
    require(env.get('GITHUB_ACTIONS') == 'true' and env.get('RUNNER_ENVIRONMENT') == 'github-hosted'
            and env.get('RUNNER_OS') == 'Linux' and env.get('RUNNER_ARCH') == 'X64', 'hosted_runner_required')
    require(env.get('HOME') == user_home == '/home/runner', 'unchanged_runner_home_required')
    require('CODEX_HOME' not in env, 'unexpected_codex_home')
    require(not any(k.startswith(('CLAUDE', 'ANTHROPIC')) for k in env), 'unexpected_host_environment')
    require(bool(env.get('RUNNER_TEMP')) and Path(env['RUNNER_TEMP']).is_absolute(), 'runner_temp_required')


def child_environment(parent, root, port):
    require('CODEX_HOME' not in parent and parent.get('HOME') == '/home/runner', 'unchanged_runner_home_required')
    require(type(port) is int and 0 < port < 65536, 'invalid_loopback_port')
    return {'HOME': parent['HOME'], 'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8',
            'TMPDIR': str(root / 'tmp'), 'TEMPO_STATE': str(root / 'activity.json'),
            'TEMPO_HOOK_STATE': str(root / 'hooks-state.json'), 'ANTHROPIC_API_KEY': TOKEN,
            'ANTHROPIC_BASE_URL': 'http://127.0.0.1:' + str(port) + '/claude',
            'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1',
            'CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL': '1',
            'CLAUDE_CODE_DISABLE_AUTO_MEMORY': '1', 'CLAUDE_CODE_DISABLE_CLAUDE_MDS': '1',
            'CLAUDE_CODE_SKIP_PROMPT_HISTORY': '1', 'DISABLE_TELEMETRY': '1',
            'DISABLE_ERROR_REPORTING': '1', 'DISABLE_AUTOUPDATER': '1'}


def host_state_paths(home, project):
    paths = [home / '.claude', home / '.claude.json', home / '.config/claude',
             home / '.config/claude-code', home / '.anthropic', Path('/etc/claude-code')]
    for ancestor in project.parents:
        paths.extend(ancestor / name for name in ('.claude', '.mcp.json', 'CLAUDE.md'))
    return list(dict.fromkeys(paths))


def extract_runtime(archive, dest, pin):
    require(archive.stat().st_size == pin['size'] and digest(archive) == pin['sha256'], 'runtime_integrity')
    dest.mkdir(mode=0o700)
    names, binary = set(), None
    # Stream members: never collect unbounded archive metadata or trust extractall.
    with tarfile.open(archive, 'r|gz') as bundle:
        for item in bundle:
            name = item.name.rstrip('/')
            p = PurePosixPath(name)
            require(len(names) < 16 and len(name) <= 128 and name not in names and name
                    and not p.is_absolute() and '..' not in p.parts and '\\' not in name
                    and str(p) == name and len(p.parts) <= 2
                    and (item.isdir() or item.isreg()) and not item.pax_headers,
                    'runtime_archive_entry')
            names.add(name)
            if item.isdir():
                require(len(p.parts) == 1, 'runtime_package_layout')
                continue
            require(binary is None and p.name == 'claude' and item.size == pin['binary_size'], 'runtime_package_layout')
            binary = dest / 'claude'
            with bundle.extractfile(item) as source, binary.open('xb') as output:
                remaining = pin['binary_size']
                while remaining:
                    chunk = source.read(min(1024 * 1024, remaining))
                    require(bool(chunk), 'runtime_binary_integrity')
                    output.write(chunk); remaining -= len(chunk)
                require(not source.read(1), 'runtime_binary_integrity')
    require(binary is not None and binary.stat().st_size == pin['binary_size']
            and digest(binary) == pin['binary_sha256'], 'runtime_binary_integrity')
    binary.chmod(0o700)
    return binary


def download_runtime(root, pin):
    require(pin['version'] == VERSION and pin['platform'] == 'linux-x64'
            and pin['url'] == 'https://github.com/anthropics/claude-code/releases/download/v2.1.286/claude-linux-x64.tar.gz'
            and 0 < pin['size'] <= 128 << 20 and 0 < pin['binary_size'] <= 320 << 20,
            'runtime_pin_contract')
    require(all(isinstance(pin[k], str) and re.fullmatch('[a-f0-9]{64}', pin[k])
                for k in ('sha256', 'binary_sha256')), 'runtime_pin_contract')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    archive = root / 'claude.tar.gz'
    total = 0
    with opener.open(pin['url'], timeout=30) as source, archive.open('xb') as output:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk: break
            total += len(chunk)
            require(total <= pin['size'], 'runtime_download_bound')
            output.write(chunk)
    return extract_runtime(archive, root / 'runtime', pin)


def claude_argv(runtime, project):
    agents = {'tempo-fixture-child': {'description': 'Synthetic lifecycle child',
              'prompt': 'Complete the supplied synthetic lifecycle case.',
              'tools': ['Read'], 'model': MODEL, 'background': True}}
    return [str(runtime), '--print', '--setting-sources', 'project,local', '--tools', 'Read,Agent',
            '--strict-mcp-config', '--mcp-config', str(project / 'mcp.json'), '--no-session-persistence',
            '--model', MODEL, '--max-turns', '4', '--agents', json.dumps(agents), PARENT_PROMPT]


def project_receipt(value):
    require(value.get('source') == 'claude' and value.get('origin') == 'unverified', 'receipt_provenance')
    result = {'source': 'claude', 'origin': 'unverified'}
    enums = {'kind': EVENTS, 'disposition': ('applied', 'duplicate', 'stale', 'untracked', 'review_required'),
             'ordering': ('supported', 'review_required', 'unavailable'),
             'durability': ('committed', 'not_committed', 'unknown'),
             'profile_basis': ('operator_declared', '', 'unknown', 'unverified')}
    for key, allowed in enums.items():
        require(value.get(key, '') in allowed, 'receipt_status')
        result[key] = value.get(key, '')
    for key in ('id', 'session_id', 'turn_id', 'agent_id', 'tool_id', 'snapshot_revision', 'profile_revision'):
        v = value.get(key, '')
        require(isinstance(v, str) and re.fullmatch(r'[A-Za-z0-9_.:/=-]{0,256}', v), 'receipt_identifier')
        result[key] = v
    fp = value.get('fingerprint', '')
    require(isinstance(fp, str) and re.fullmatch('[a-f0-9]{64}|', fp), 'receipt_fingerprint')
    result['fingerprint'] = fp
    return result


def accepted(row):
    return (row.get('source') == 'claude' and row.get('origin') == 'unverified'
            and row.get('disposition') in ('applied', 'duplicate') and row.get('ordering') == 'supported'
            and row.get('durability') == 'committed' and row.get('profile_basis') == 'operator_declared')


def exact_receipt(rows, kind, session=None, turn=None, agent=None, tool=None):
    matches = [r for r in rows if r.get('kind') == kind
               and all(v is None or r.get(k) == v for k, v in
                       (('session_id', session), ('turn_id', turn), ('agent_id', agent), ('tool_id', tool)))]
    require(len(matches) == 1 and accepted(matches[0]), 'native_receipt_barrier')
    return matches[0]


def actor(snapshot, receipt):
    ref = receipt.get('actor')
    matches = [a for a in snapshot['actors'] if ref is not None and a['ref'] == ref]
    require(len(matches) == 1, 'actor_projection_missing')
    return matches[0]


def require_independence(snapshot, parent, child):
    require(parent.get('actor') != child.get('actor'), 'parent_child_actor_collision')
    root_actor, child_actor = actor(snapshot, parent), actor(snapshot, child)
    require(root_actor['state'] == 'wait_user' and child_actor['state'] == 'working'
            and root_actor['health'] == child_actor['health'] == 'continuous', 'parent_child_independence')


def text_blocks(content):
    if isinstance(content, str): return [content]
    require(isinstance(content, list) and len(content) <= 128, 'provider_content_contract')
    return [v['text'] for v in content if isinstance(v, dict) and v.get('type') == 'text' and isinstance(v.get('text'), str)]


def request_case(body):
    messages = body.get('messages')
    require(isinstance(messages, list) and 0 < len(messages) <= 64, 'provider_messages_contract')
    # Walk actual user text; tool_result content and nested tool arguments cannot
    # classify a request. Later tool-only user messages retain the previous turn.
    for message in reversed(messages):
        if message.get('role') != 'user': continue
        text = '\n'.join(text_blocks(message.get('content', [])))
        cases = [case for case, marker in (('parent', PARENT_PROMPT), ('child', CHILD_PROMPT))
                 if re.search(r'(?<![\w-])' + marker + r'(?![\w-])', text)]
        if cases:
            require(len(cases) == 1, 'provider_turn_ambiguous')
            return cases[0]
        if text.strip(): raise FixtureFailure('unexpected_provider_turn')
    raise FixtureFailure('unexpected_provider_turn')


def agent_error_category(content):
    """Classify bounded error text in memory; never return text or captures.

    These fixed fragments come from the pinned 2.1.286 Agent launch and tool
    permission paths. They are diagnostic hints only, never success evidence.
    """
    if isinstance(content, str):
        parts = [content]
    elif isinstance(content, list) and len(content) <= 128:
        parts = [v['text'] for v in content if isinstance(v, dict)
                 and v.get('type') == 'text' and isinstance(v.get('text'), str)]
    else:
        return 'agent_tool_error_unclassified'
    if sum(len(v) for v in parts) > 16384:
        return 'agent_tool_error_unclassified'
    text = '\n'.join(parts)
    categories = (
        ('type_unavailable', ("Agent type 'tempo-fixture-child' not found.",
                              "Agent type 'tempo-fixture-child' is not offered in this session.")),
        ('permission', ("Agent type 'tempo-fixture-child' has been denied by permission rule ",
                        "Agent type 'tempo-fixture-child' is unavailable because every tool it may use is denied",
                        'Permission to use Agent has been denied', 'Tool permission request failed',
                        'Agent tool requires permission to spawn subagents.')),
        ('executor_unavailable', ('Agent: launching needs the executor (call.runEngine)',)),
        ('depth_limit', ('Subagent nesting limit reached (depth ',)),
        ('concurrency_limit', ('Concurrent subagent limit reached.',)),
        ('background_unavailable', ('In-process teammates cannot spawn background agents',)),
    )
    for category, fragments in categories:
        if any(fragment in text for fragment in fragments):
            return 'agent_tool_error_' + category
    return 'agent_tool_error_unclassified'


def require_tool_result(body, tool_id, marker=None):
    matches = []
    for msg in body.get('messages', []):
        if msg.get('role') != 'user' or not isinstance(msg.get('content'), list): continue
        matches.extend(v for v in msg['content'] if isinstance(v, dict) and v.get('type') == 'tool_result'
                       and v.get('tool_use_id') == tool_id)
    require(bool(matches), 'actual_tool_result_missing')
    require(len(matches) == 1, 'actual_tool_result_duplicate')
    is_error = matches[0].get('is_error', False)
    require(type(is_error) is bool, 'actual_tool_result_invalid_error_flag')
    if is_error:
        raise FixtureFailure(agent_error_category(matches[0].get('content'))
                             if tool_id == 'tempo-agent' else 'actual_tool_result_error')
    if marker is not None:
        require(marker in '\n'.join(text_blocks(matches[0].get('content', []))), 'actual_read_result_missing')


def tool_item(body, name, tool_id, arguments):
    tools = body.get('tools', [])
    require(isinstance(tools, list) and len(tools) <= 16, 'tool_schema_mismatch')
    found = [t for t in tools if isinstance(t, dict) and t.get('name') == name]
    require(len(found) == 1, 'required_tool_schema_unavailable')
    schema = found[0].get('input_schema', {})
    props, required = schema.get('properties', {}), schema.get('required', [])
    require(schema.get('type') == 'object' and isinstance(props, dict) and isinstance(required, list)
            and all(isinstance(k, str) for k in required) and set(required) <= arguments.keys()
            and arguments.keys() <= props.keys(), 'tool_schema_mismatch')
    for key, value in arguments.items():
        item = props[key]
        require(isinstance(item, dict) and item.get('type') == ('boolean' if type(value) is bool else 'string')
                and ('enum' not in item or value in item['enum']), 'tool_schema_mismatch')
    return {'type': 'tool_use', 'id': tool_id, 'name': name, 'input': arguments}


def sse_events(block):
    tool = block['type'] == 'tool_use'
    start = dict(block, input={}) if tool else {'type': 'text', 'text': ''}
    delta = {'type': 'input_json_delta', 'partial_json': json.dumps(block['input'])} if tool else {'type': 'text_delta', 'text': block['text']}
    return [{'type': 'message_start', 'message': {'id': 'msg_tempo_fixture', 'type': 'message', 'role': 'assistant',
             'model': MODEL, 'content': [], 'stop_reason': None, 'stop_sequence': None,
             'usage': {'input_tokens': 1, 'output_tokens': 0}}},
            {'type': 'content_block_start', 'index': 0, 'content_block': start},
            {'type': 'content_block_delta', 'index': 0, 'delta': delta},
            {'type': 'content_block_stop', 'index': 0},
            {'type': 'message_delta', 'delta': {'stop_reason': 'tool_use' if tool else 'end_turn', 'stop_sequence': None},
             'usage': {'output_tokens': 1}}, {'type': 'message_stop'}]


class Conversation:
    def __init__(self, read, project):
        self.read, self.project = read, project
        self.lock = threading.Lock()
        self.session = self.turn = self.child = self.child_turn = None
        self.phase = 'initial'
        self.counts, self.requests = {}, []
        self.independence_observed = False

    def respond(self, body):
        require(body.get('stream') is True and body.get('model') == MODEL, 'provider_request_contract')
        category = request_case(body)
        with self.lock:
            snapshot = self.read()
            rows = snapshot['receipts']
            require(len(rows) <= 64 and len(snapshot['actors']) <= 4, 'native_snapshot_bound')
            for row in rows: project_receipt(row)
            if self.session is None:
                require(category == 'parent' and self.phase == 'initial', 'request_before_parent')
                start = exact_receipt(rows, 'SessionStart')
                prompt = exact_receipt(rows, 'UserPromptSubmit', session=start['session_id'], agent='')
                require(start['session_id'] and prompt['turn_id'] and prompt.get('actor'), 'prompt_identity_missing')
                current = actor(snapshot, prompt)
                require(current['state'] == 'working' and current['health'] == 'continuous', 'prompt_actor_barrier_missing')
                self.session, self.turn = start['session_id'], prompt['turn_id']
            exact_receipt(rows, 'SessionStart', session=self.session)
            root = exact_receipt(rows, 'UserPromptSubmit', self.session, self.turn, '')
            if category == 'parent':
                current = actor(snapshot, root)
                require(current['state'] == 'working' and current['health'] == 'continuous', 'prompt_actor_barrier_missing')
            if category == 'child':
                require(self.phase in ('agent', 'parent_done') and 'child' not in self.counts, 'unexpected_child_request')
                exact_receipt(rows, 'PreToolUse', self.session, self.turn, '', 'tempo-agent')
                child = exact_receipt(rows, 'SubagentStart', session=self.session)
                require(child['agent_id'] and child['turn_id'] and child.get('actor') != root.get('actor'), 'child_identity_missing')
                current = actor(snapshot, child)
                require(current['state'] == 'working' and current['health'] == 'continuous', 'child_start_barrier_missing')
                self.child, self.child_turn = child['agent_id'], child['turn_id']
                block, hold = {'type': 'text', 'text': 'tempo-child-complete'}, True
            elif self.phase == 'initial':
                block = tool_item(body, 'Read', 'tempo-read', {'file_path': str(self.project / 'fixture.txt')})
                self.phase, hold = 'read', False
            elif self.phase == 'read':
                require_tool_result(body, 'tempo-read', READ_RESULT)
                for kind in ('PreToolUse', 'PostToolUse'):
                    exact_receipt(rows, kind, self.session, self.turn, '', 'tempo-read')
                block = tool_item(body, 'Agent', 'tempo-agent', {'description': 'Synthetic lifecycle child',
                                  'prompt': CHILD_PROMPT, 'subagent_type': 'tempo-fixture-child', 'run_in_background': True})
                self.phase, hold = 'agent', False
            elif self.phase == 'agent':
                require_tool_result(body, 'tempo-agent')
                for kind in ('PreToolUse', 'PostToolUse'):
                    exact_receipt(rows, kind, self.session, self.turn, '', 'tempo-agent')
                block, hold = {'type': 'text', 'text': 'tempo-parent-complete'}, False
                self.phase = 'parent_done'
            else:
                raise FixtureFailure('extra_parent_request')
            self.counts[category] = self.counts.get(category, 0) + 1
            require(sum(self.counts.values()) <= 4, 'provider_response_bound')
            self.requests.append({'case': category, 'index': len(self.requests) + 1, 'receipt_count': len(rows)})
            return block, hold

    def observe_parent_stop(self):
        with self.lock:
            snap = self.read()
            stops = [r for r in snap['receipts'] if r['kind'] == 'Stop' and r['session_id'] == self.session
                     and r['turn_id'] == self.turn and r['agent_id'] == '']
            if not stops: return False
            stop = exact_receipt(snap['receipts'], 'Stop', self.session, self.turn, '')
            child = exact_receipt(snap['receipts'], 'SubagentStart', self.session, self.child_turn, self.child)
            require_independence(snap, stop, child)
            self.independence_observed = True
            return True


class ProviderBudget:
    def __init__(self):
        self.lock, self.active, self.requests = threading.Lock(), 0, 0

    def claim(self):
        with self.lock:
            if self.active >= 4: return False
            self.active += 1
            return True

    def release(self):
        with self.lock: self.active -= 1

    def enter_request(self):
        with self.lock:
            self.requests += 1
            require(self.requests <= 8, 'provider_request_bound')


def validate_http_request(path, headers):
    require(path in ('/claude/v1/messages', '/claude/v1/messages?beta=true'), 'unexpected_provider_endpoint')
    require(headers.get('x-api-key') == TOKEN and headers.get('Authorization') is None, 'synthetic_provider_auth')
    require(headers.get('Transfer-Encoding') is None and headers.get('Content-Encoding') is None, 'unexpected_request_encoding')
    if hasattr(headers, 'get_all'):
        require(all(len(headers.get_all(k, [])) <= 1 for k in ('Content-Length', 'x-api-key')), 'provider_header_contract')
    length = headers.get('Content-Length', '')
    require(isinstance(length, str) and re.fullmatch('[0-9]{1,8}', length), 'provider_request_bound')
    require(0 < int(length) <= 2 * 1024 * 1024, 'provider_request_bound')
    return int(length)


class DeadlineReader:
    """One absolute input deadline, including peers that send a slow trickle."""
    def __init__(self, source, connection, deadline, now=time.monotonic):
        self.source, self.connection, self.deadline, self.now = source, connection, deadline, now
        self.pending = bytearray()

    def fill(self, limit):
        remaining = self.deadline - self.now()
        require(remaining > 0, 'provider_input_deadline')
        self.connection.settimeout(min(5, remaining))
        chunk = self.source.read1(min(4096, limit))
        require(self.now() < self.deadline, 'provider_input_deadline')
        self.pending.extend(chunk)
        return bool(chunk)

    def read(self, size):
        require(0 <= size <= 2 * 1024 * 1024, 'provider_request_bound')
        result = bytearray()
        while len(result) < size:
            if not self.pending and not self.fill(size - len(result)): break
            amount = min(size - len(result), len(self.pending))
            result.extend(self.pending[:amount]); del self.pending[:amount]
        return bytes(result)

    def readline(self, limit=-1):
        limit = min(limit if limit >= 0 else 16385, 16385)
        while True:
            end = self.pending.find(b'\n')
            if end >= 0 or len(self.pending) >= limit:
                count = min(end + 1 if end >= 0 else limit, limit)
                result = bytes(self.pending[:count]); del self.pending[:count]
                return result
            if not self.fill(limit - len(self.pending)):
                result = bytes(self.pending); self.pending.clear()
                return result

    def close(self): self.source.close()


class HeaderReader:
    def __init__(self, source): self.source, self.total = source, 0
    def readline(self, limit=-1):
        data = self.source.readline(min(limit if limit >= 0 else 16385, 16385))
        self.total += len(data)
        require(self.total <= 16384, 'provider_header_bound')
        return data


def make_handler(provider):
    class Handler(http.server.BaseHTTPRequestHandler):
        def setup(self):
            self.request.settimeout(5)
            super().setup()
            self.rfile = DeadlineReader(self.rfile, self.connection, time.monotonic() + 5)
        def log_message(self, *_): pass
        def log_error(self, *_): provider.error = 'provider_protocol_failed'
        def send_error(self, code, message=None, explain=None):
            provider.error = 'provider_protocol_failed'
            super().send_error(code, 'synthetic fixture rejected request', 'request rejected')
        def parse_request(self):
            require(len(self.raw_requestline) <= 2048, 'provider_header_bound')
            original = self.rfile
            self.rfile = HeaderReader(original)
            try: return super().parse_request()
            finally: self.rfile = original
        def handle(self):
            try: super().handle()
            except Exception as exc:
                provider.error = str(exc) if isinstance(exc, FixtureFailure) else 'provider_protocol_failed'
                self.close_connection = True
        def do_POST(self):
            try:
                provider.budget.enter_request()
                size = validate_http_request(self.path, self.headers)
                data = self.rfile.read(size)
                require(len(data) == size, 'provider_request_bound')
                body = json.loads(data)
                require(isinstance(body, dict), 'provider_request_contract')
                require(not provider.shutdown.is_set(), 'provider_request_after_shutdown')
                block, hold = provider.conversation.respond(body)
                if hold:
                    until = min(provider.deadline, time.monotonic() + 30)
                    while time.monotonic() < until and not provider.shutdown.is_set():
                        if provider.conversation.observe_parent_stop(): break
                        provider.shutdown.wait(.05)
                    require(provider.conversation.independence_observed and not provider.shutdown.is_set(), 'parent_stop_missing')
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Connection', 'close'); self.end_headers()
                for event in sse_events(block):
                    self.wfile.write(('event: ' + event['type'] + '\ndata: ' + json.dumps(event) + '\n\n').encode())
                    self.wfile.flush()
            except Exception as exc:
                provider.error = str(exc) if isinstance(exc, FixtureFailure) else 'provider_protocol_failed'
                self.send_response(400); self.send_header('Connection', 'close'); self.end_headers()
        def reject(self):
            provider.error = 'unexpected_provider_endpoint'
            self.send_response(404); self.send_header('Connection', 'close'); self.end_headers()
        do_GET = do_HEAD = do_PUT = do_DELETE = do_OPTIONS = reject
    return Handler


class Provider:
    def __init__(self, conversation, deadline):
        self.conversation, self.deadline = conversation, deadline
        self.shutdown, self.budget, self.error = threading.Event(), ProviderBudget(), None
        provider = self
        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = False
            block_on_close = True
            def process_request(self, request, address):
                if not provider.budget.claim():
                    provider.error = 'provider_concurrency_bound'
                    self.shutdown_request(request)
                    return
                try: super().process_request(request, address)
                except BaseException:
                    provider.budget.release()
                    raise
            def process_request_thread(self, request, address):
                try: super().process_request_thread(request, address)
                finally: provider.budget.release()
            def handle_error(self, *_): provider.error = 'provider_protocol_failed'
        self.server = Server(('127.0.0.1', 0), make_handler(self))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        try: self.thread.start()
        except BaseException:
            self.server.server_close()
            raise

    def close(self):
        self.shutdown.set()
        try: self.server.shutdown()
        finally:
            try: self.server.server_close()  # Joins bounded owned handlers.
            finally: self.thread.join(timeout=2)
        require(self.error is None, self.error or 'provider_failed')


def require_runtime_version(output):
    require(output in (b'2.1.286 (Claude Code)', b'2.1.286 (Claude Code)\n'), 'runtime_version_mismatch')


def prepare_settings(tempo, diagnostics):
    require(tempo.is_absolute() and diagnostics.is_absolute() and tempo.parent == diagnostics.parent
            and all(re.fullmatch(r'[A-Za-z0-9/_-]+', str(p)) for p in (tempo, diagnostics)), 'unsafe_fixture_path')
    command = str(tempo) + ' hook claude --input-stdin 2>> ' + str(diagnostics)
    require(len(command) <= 250, 'unsafe_fixture_path')
    try:
        fd = os.open(diagnostics, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try: os.fchmod(fd, 0o600)
        finally: os.close(fd)
    except OSError:
        raise FixtureFailure('diagnostic_setup_failed')
    return {'hooks': {event: [{'hooks': [{'type': 'command', 'command': command, 'timeout': 2}]}]
                      for event in EVENTS}}


def require_final(conversation, snapshot):
    require(conversation.counts == {'parent': 3, 'child': 1} and conversation.independence_observed,
            'native_provider_coverage_missing')
    rows = snapshot['receipts']
    require(len(rows) == 10 and len(snapshot['actors']) == 2, 'native_event_coverage_mismatch')
    for row in rows: project_receipt(row)
    c = conversation
    exact_receipt(rows, 'SessionStart', c.session, '', '')
    prompt = exact_receipt(rows, 'UserPromptSubmit', c.session, c.turn, '')
    for tool in ('tempo-read', 'tempo-agent'):
        for kind in ('PreToolUse', 'PostToolUse'):
            phase = exact_receipt(rows, kind, c.session, c.turn, '', tool)
            require(phase.get('actor') == prompt.get('actor'), 'root_tool_actor_mismatch')
    stop = exact_receipt(rows, 'Stop', c.session, c.turn, '')
    child = exact_receipt(rows, 'SubagentStart', c.session, c.child_turn, c.child)
    child_stop = exact_receipt(rows, 'SubagentStop', c.session, c.child_turn, c.child)
    end = exact_receipt(rows, 'SessionEnd', c.session, '', '')
    require(end['disposition'] == 'applied' and end.get('actor') == stop.get('actor') == prompt.get('actor')
            and child_stop.get('actor') == child.get('actor') and child.get('actor') != prompt.get('actor'),
            'terminal_actor_mismatch')
    require(actor(snapshot, end)['state'] == 'interrupted'
            and actor(snapshot, child_stop)['state'] == 'wait_user', 'terminal_actor_effect_missing')
    require(type(snapshot['queued']) is int and 0 < snapshot['queued'] <= 4
            and snapshot['uncertainties'] == 0 and snapshot['capture_reviews'] == 0, 'native_capture_effects_missing')


def run(args, report):
    started, root, provider, conversation = time.monotonic(), None, None, None
    diagnostics = diagnostic_identity = None
    report['stage'] = 'hosted_preconditions'
    hosted_precondition(os.environ, platform.system(), platform.machine(), pwd.getpwuid(os.getuid()).pw_dir)
    sha = os.environ.get('GITHUB_SHA', '')
    require(re.fullmatch('[a-f0-9]{40}', sha), 'build_identity_missing')
    report['checkout_sha'] = sha
    home = Path(os.environ['HOME'])
    # Check external inputs before creating fixture paths; never inspect contents.
    require_absent(host_state_paths(home, Path(os.environ['RUNNER_TEMP']) / 'project'))
    pin = json.loads(Path(__file__).with_name('claude-2.1.286.json').read_text())
    try:
        root = Path(tempfile.mkdtemp(prefix='tempo-claude-', dir=os.environ['RUNNER_TEMP']))
        (root / 'tmp').mkdir(mode=0o700)
        project = root / 'project'; project.mkdir(mode=0o700)
        require_absent(host_state_paths(home, project))
        report['stage'] = 'pinned_runtime_download'
        runtime = download_runtime(root, pin)
        tempo, helper = Path(args.tempo).resolve(strict=True), Path(args.helper).resolve(strict=True)
        require(tempo.is_file() and helper.is_file(), 'built_binaries_required')
        shutil.copy2(tempo, root / 'tempo'); tempo = root / 'tempo'
        diagnostics = root / 'hook-errors'
        settings = prepare_settings(tempo, diagnostics)
        diagnostic_identity = diagnostics.stat()
        (project / '.claude').mkdir(mode=0o700)
        definitions = project / '.claude/settings.json'
        definitions.write_text(json.dumps(settings) + '\n'); definitions.chmod(0o600)
        (project / 'mcp.json').write_text('{"mcpServers":{}}\n')
        (project / 'fixture.txt').write_text(READ_RESULT + '\n')
        # No parent environment is forwarded, even to the test-only Go helper.
        env = child_environment(os.environ, root, 9)
        def helper_call(action, *extra):
            selector = [] if action == 'link' else ['--host', 'claude']
            output = bounded_run([str(helper), 'fixture', action, env['TEMPO_STATE'], env['TEMPO_HOOK_STATE'],
                                  *map(str, extra), *selector], env, project, timeout=8)
            return json.loads(output) if output else None
        report['stage'] = 'production_link_and_policy'
        helper_call('link', project)
        profile = helper_call('confirm', project, runtime, tempo, definitions)
        require(profile['basis'] == 'operator_declared' and re.fullmatch('[a-f0-9]{64}', profile['fingerprint']),
                'native_profile_missing')
        require(helper_call('read')['receipts'] == [], 'nonempty_native_baseline')
        conversation = Conversation(lambda: helper_call('read'), project)
        provider = Provider(conversation, started + 240)
        env = child_environment(os.environ, root, provider.server.server_port)
        report.update({'runtime_version': VERSION, 'archive_sha256': pin['sha256'], 'runtime_sha256': digest(runtime),
                       'tempo_sha256': digest(tempo), 'definitions_sha256': digest(definitions),
                       'profile_fingerprint': profile['fingerprint'], 'configuration_origin': 'fixture_authored',
                       'delivery_origin': 'actual_claude_process', 'profile_basis': 'operator_declared',
                       'product_receipt_origin': 'unverified'})
        report['stage'] = 'runtime_version'
        require_runtime_version(bounded_run([str(runtime), '--version'], env, project, timeout=8))
        require(provider.budget.requests == 0 and provider.error is None, 'unexpected_version_inference')
        # Profile hashes the ordinary settings exactly; there is no trust record,
        # installer shortcut, permission allow rule, or callback injection.
        report['stage'] = 'native_print_turn'
        bounded_run(claude_argv(runtime, project), env, project, timeout=110)
        report['stage'] = 'native_terminal_receipts'
        snapshot = helper_call('read')
        require_final(conversation, snapshot)
        report.update({'receipts': [project_receipt(r) for r in snapshot['receipts']],
                       'queued_count': snapshot['queued'], 'uncertainty_count': snapshot['uncertainties'],
                       'capture_review_count': snapshot['capture_reviews'],
                       'assertions': ['empty_initial_receipts', 'normal_print_settings', 'prompt_before_provider',
                                      'actual_read_result', 'actual_child_request', 'parent_stop_child_still_working',
                                      'exact_child_stop', 'normal_session_end', 'successful_host_exit', 'production_capture_effects']})
    finally:
        try:
            if provider is not None: provider.close()
        finally:
            try:
                if provider is not None: report['provider_entry_count'] = min(provider.budget.requests, 9)
                if conversation is not None:
                    report['request_counts'] = conversation.counts
                    report['requests'] = conversation.requests
                    if 'receipts' not in report:
                        try:
                            rows = conversation.read()['receipts']
                            require(len(rows) <= 64, 'native_snapshot_bound')
                            report['receipts'] = [project_receipt(r) for r in rows]
                        except Exception:
                            report['receipt_observation_status'] = 'unavailable'
                if diagnostics is not None and diagnostic_identity is not None:
                    report['hook_diagnostics'] = hook_diagnostic_probe(diagnostics, diagnostic_identity)
                report['elapsed_ms'] = int((time.monotonic() - started) * 1000)
            finally:
                # Only our fresh temporary tree is removed. Host-generated HOME
                # files are left for ephemeral runner teardown, never read/copied.
                if root is not None: shutil.rmtree(root)
    # All processes are reaped, provider handlers joined and cleanup succeeded.
    report['status'], report['stage'] = 'passed', 'complete'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tempo', required=True)
    parser.add_argument('--helper', required=True)
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    report = {'schema_version': 1, 'status': 'failed', 'host': 'claude', 'coverage': 'finite_hosted_linux_cases'}
    def alarm(*_): raise FixtureFailure('overall_deadline')
    def cancelled(*_): raise FixtureFailure('fixture_cancelled')
    old = {sig: signal.getsignal(sig) for sig in (signal.SIGALRM, signal.SIGTERM, signal.SIGINT)}
    signal.signal(signal.SIGALRM, alarm)
    signal.signal(signal.SIGTERM, cancelled); signal.signal(signal.SIGINT, cancelled)
    signal.alarm(250)
    try:
        run(args, report)
    except Exception as exc:
        report['status'] = 'failed'
        report['failure_category'] = str(exc) if isinstance(exc, FixtureFailure) else 'harness_failed'
    finally:
        signal.alarm(0)
        for sig, handler in old.items(): signal.signal(sig, handler)
        Path(args.evidence).write_text(json.dumps(report, indent=2) + '\n')
    print('native_claude_smoke: ' + report['status'])
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
