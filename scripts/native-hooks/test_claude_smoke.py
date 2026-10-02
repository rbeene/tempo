"""Pure fixture tests: inert archives/processes only; never runs Claude/Codex."""
import copy
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import signal
import threading
import sys
import tarfile
import tempfile
import time
import types
import unittest
from unittest import mock

MODULE = Path(__file__).with_name('claude_smoke.py')
# Missing implementation is an explicit RED, with each behavioral case reported.
if MODULE.exists():
    spec = importlib.util.spec_from_file_location('claude_smoke', MODULE)
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)
else:
    smoke = types.SimpleNamespace()


def hosted_env():
    return {'GITHUB_ACTIONS': 'true', 'RUNNER_ENVIRONMENT': 'github-hosted',
            'RUNNER_OS': 'Linux', 'RUNNER_ARCH': 'X64', 'HOME': '/home/runner',
            'RUNNER_TEMP': '/home/runner/work/_temp'}


def receipt(kind, agent='', turn='prompt-1', tool=''):
    actor = {'session_id': 'mapped-session', 'agent_id': 'root' if not agent else 'child:'+agent, 'generation': '1'}
    return {'source': 'claude', 'origin': 'unverified', 'kind': kind, 'session_id': 'session-1',
            'turn_id': '' if kind in ('SessionStart', 'SessionEnd') else turn, 'agent_id': agent,
            'tool_id': tool, 'id': kind+'-'+agent+'-'+tool, 'snapshot_revision': '1',
            'disposition': 'applied', 'ordering': 'supported', 'durability': 'committed',
            'profile_basis': 'operator_declared', 'profile_revision': '1', 'fingerprint': 'a'*64,
            'actor': None if kind == 'SessionStart' else actor}


def snapshot(receipts):
    return {'receipts': receipts, 'actors': [{'ref': r['actor'], 'state': 'working', 'health': 'continuous'}
             for r in receipts if r['actor'] is not None and r['kind'] in ('UserPromptSubmit', 'SubagentStart')],
            'queued': 0, 'uncertainties': 0, 'capture_reviews': 0}


def tool_schema(name, props, required):
    return {'name': name, 'input_schema': {'type': 'object', 'properties': props, 'required': required}}


def request(text='tempo-native-parent-case', results=None):
    return {'model': 'claude-sonnet-4-6', 'stream': True, 'messages': [{'role': 'user', 'content':
             [{'type': 'text', 'text': text}] + (results or [])}], 'tools': [
             tool_schema('Read', {'file_path': {'type': 'string'}}, ['file_path']),
             tool_schema('Agent', {'description': {'type': 'string'}, 'prompt': {'type': 'string'},
               'subagent_type': {'type': 'string'}, 'run_in_background': {'type': 'boolean'}},
               ['description', 'prompt', 'subagent_type'])]}


def result(tool, text='tempo-fixture-read-ok'):
    return {'type': 'tool_result', 'tool_use_id': tool, 'content': [{'type': 'text', 'text': text}]}


class IsolationTests(unittest.TestCase):
    def test_hosted_guard_rejects_modified_home_platform_and_host_variables(self):
        smoke.hosted_precondition(hosted_env(), 'Linux', 'x86_64', '/home/runner')
        for key, value in [('HOME', '/tmp/home'), ('CODEX_HOME', ''), ('CLAUDE_CONFIG_DIR', ''),
                           ('CLAUDECODE', '1'), ('ANTHROPIC_API_KEY', 'canary'), ('CLAUDE_CODE_SESSION_ID', 'x'),
                           ('RUNNER_ENVIRONMENT', 'self-hosted')]:
            env = hosted_env(); env[key] = value
            with self.subTest(key=key), self.assertRaises(smoke.FixtureFailure):
                smoke.hosted_precondition(env, 'Linux', 'x86_64', '/home/runner')
        with self.assertRaises(smoke.FixtureFailure):
            smoke.hosted_precondition(hosted_env(), 'Darwin', 'arm64', '/home/runner')

    def test_child_env_has_only_synthetic_auth_and_loopback(self):
        env = hosted_env(); env.update({'GITHUB_TOKEN': 'canary', 'AWS_SESSION_TOKEN': 'canary',
          'HARVEST_TOKEN': 'canary', 'HTTPS_PROXY': 'canary', 'SSH_AUTH_SOCK': 'canary', 'PATH': 'canary'})
        child = smoke.child_environment(env, Path('/tmp/owned'), 12345)
        self.assertEqual(child['HOME'], '/home/runner')
        self.assertEqual(child['ANTHROPIC_API_KEY'], 'tempo-ci-invalid-synthetic-key')
        self.assertEqual(child['ANTHROPIC_BASE_URL'], 'http://127.0.0.1:12345/claude')
        self.assertNotIn('canary', json.dumps(child)); self.assertNotIn('CODEX_HOME', child)
        self.assertNotIn('CLAUDE_CONFIG_DIR', child)
        for key in ('CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC', 'CLAUDE_CODE_DISABLE_OFFICIAL_MARKETPLACE_AUTOINSTALL'):
            self.assertEqual(child[key], '1')

    def test_preexisting_files_symlinks_rejected_without_opening(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/'state'; p.symlink_to(Path(d)/'absent')
            with mock.patch('builtins.open', side_effect=AssertionError('must not open')):
                with self.assertRaises(smoke.FixtureFailure): smoke.require_absent([p])
                smoke.require_absent([Path(d)/'new'])

    def test_import_does_not_spawn_download_bind_or_write(self):
        spec = importlib.util.spec_from_file_location('pure_import', MODULE)
        with mock.patch('subprocess.Popen', side_effect=AssertionError('spawn')), \
             mock.patch('urllib.request.OpenerDirector.open', side_effect=AssertionError('download')), \
             mock.patch('socket.socket.bind', side_effect=AssertionError('bind')), \
             mock.patch('pathlib.Path.write_text', side_effect=AssertionError('write')):
            spec.loader.exec_module(importlib.util.module_from_spec(spec))

    def test_normal_print_argv_has_only_exact_fixture_tool_preapproval(self):
        argv = smoke.claude_argv(Path('/tmp/claude'), Path('/tmp/project'))
        self.assertIn('--print', argv); self.assertIn('--no-session-persistence', argv)
        self.assertEqual(argv[argv.index('--setting-sources')+1], 'project,local')
        self.assertEqual(argv[argv.index('--tools')+1], 'Read,Agent')
        self.assertEqual(argv[argv.index('--allowedTools')+1], 'Read(//tmp/project/fixture.txt),Agent')
        self.assertIn('--strict-mcp-config', argv)
        for forbidden in ('--bare', '--safe-mode', '--permission-mode', '--dangerously-skip-permissions'):
            self.assertNotIn(forbidden, argv)


class ArchiveTests(unittest.TestCase):
    def archive(self, root, entries):
        archive = root/'inert.tar.gz'
        with tarfile.open(archive, 'w:gz') as tar:
            for name, data, kind in entries:
                member = tarfile.TarInfo(name); member.type = kind; member.size = len(data) if kind == tarfile.REGTYPE else 0
                if kind == tarfile.SYMTYPE: member.linkname = '/outside'
                tar.addfile(member, io.BytesIO(data) if kind == tarfile.REGTYPE else None)
        data = archive.read_bytes()
        return archive, {'size': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                         'binary_size': 5, 'binary_sha256': hashlib.sha256(b'inert').hexdigest()}

    def test_verified_inert_binary_and_exact_size_digest(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d); a, pin = self.archive(root, [('claude', b'inert', tarfile.REGTYPE)])
            exe = smoke.extract_runtime(a, root/'good', pin)
            self.assertEqual(exe.read_bytes(), b'inert')
            for key, value in [('size', pin['size']+1), ('sha256', '0'*64), ('binary_size', 6), ('binary_sha256', '0'*64)]:
                bad = dict(pin); bad[key] = value
                with self.subTest(key=key), self.assertRaises(smoke.FixtureFailure):
                    smoke.extract_runtime(a, root/key, bad)

    def test_unsafe_archive_members_and_ambiguous_layout(self):
        cases = [[('../claude', b'inert', tarfile.REGTYPE)], [('/claude', b'inert', tarfile.REGTYPE)],
                 [('dir\\claude', b'inert', tarfile.REGTYPE)], [('claude', b'', tarfile.SYMTYPE)],
                 [('claude', b'', tarfile.CHRTYPE)], [('claude', b'inert', tarfile.REGTYPE)]*2,
                 [('a/claude', b'inert', tarfile.REGTYPE), ('b/claude', b'inert', tarfile.REGTYPE)],
                 [('a/b/claude', b'inert', tarfile.REGTYPE)], [('claude', b'inert'*9, tarfile.REGTYPE)]]
        for entries in cases:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as d:
                root=Path(d); a,pin=self.archive(root,entries)
                with self.assertRaises(smoke.FixtureFailure): smoke.extract_runtime(a,root/'out',pin)


    def test_archive_member_count_is_bounded(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            entries=[('dir'+str(i),b'',tarfile.DIRTYPE) for i in range(17)]
            entries.append(('claude',b'inert',tarfile.REGTYPE))
            archive,pin=self.archive(root,entries)
            with self.assertRaises(smoke.FixtureFailure):smoke.extract_runtime(archive,root/'out',pin)


class ProtocolTests(unittest.TestCase):
    def test_distinct_response_ids_survive_child_hold_and_both_request_orders(self):
        # Exercise the real request-to-SSE handler with inert byte streams. The
        # child response is allocated before release, while parent completion
        # can serialize first; a late shared-counter lookup must not alias IDs.
        for child_first in (True, False):
            with self.subTest(child_first=child_first):
                rows = [receipt('SessionStart'), receipt('UserPromptSubmit')]
                snap = snapshot(rows)
                conversation = smoke.Conversation(lambda: snap, Path('/tmp/project'))
                provider = types.SimpleNamespace(conversation=conversation,
                    budget=smoke.ProviderBudget(), shutdown=threading.Event(),
                    deadline=time.monotonic()+5, error=None)
                held = threading.Event()
                original_observe = conversation.observe_parent_stop
                def observe():
                    held.set()
                    return original_observe()
                conversation.observe_parent_stop = observe
                handler_type = smoke.make_handler(provider)
                def make_request(body):
                    data = json.dumps(body).encode()
                    handler = handler_type.__new__(handler_type)
                    handler.path = '/claude/v1/messages'
                    handler.headers = {'Content-Length': str(len(data)), 'x-api-key': smoke.TOKEN}
                    handler.rfile, handler.wfile = io.BytesIO(data), io.BytesIO()
                    handler.codes = []
                    handler.send_response = handler.codes.append
                    handler.send_header = lambda *_: None
                    handler.end_headers = lambda: None
                    return handler
                def events(handler):
                    self.assertEqual(handler.codes, [200])
                    return [json.loads(line[6:]) for line in handler.wfile.getvalue().decode().splitlines()
                            if line.startswith('data: ')]
                first = make_request(request()); first.do_POST()
                rows.extend([receipt('PreToolUse', tool='tempo-read'), receipt('PostToolUse', tool='tempo-read')])
                second = make_request(request(results=[result('tempo-read')]))
                second.do_POST()
                rows.extend([receipt('PreToolUse', tool='tempo-agent'), receipt('PostToolUse', tool='tempo-agent')])
                child_receipt = receipt('SubagentStart', 'native-child', turn='child-turn')
                rows.append(child_receipt)
                snap['actors'].append({'ref': child_receipt['actor'], 'state': 'working', 'health': 'continuous'})
                parent = make_request(request(results=[result('tempo-agent', 'synthetic child started')]))
                child = make_request(request(smoke.CHILD_PROMPT))
                worker = threading.Thread(target=child.do_POST)
                try:
                    if child_first:
                        worker.start()
                        self.assertTrue(held.wait(2), 'child handler never reached real hold')
                        self.assertEqual(child.wfile.getvalue(), b'')
                        parent.do_POST()
                    else:
                        parent.do_POST()
                        worker.start()
                        self.assertTrue(held.wait(2), 'child handler never reached real hold')
                    # The child must remain held even after parent SSE completes.
                    self.assertEqual(parent.codes, [200])
                    self.assertEqual(child.wfile.getvalue(), b'')
                    with conversation.lock:
                        rows.append(receipt('Stop'))
                        snap['actors'][0]['state'] = 'wait_user'
                    worker.join(2)
                    self.assertFalse(worker.is_alive(), 'child failed to finish after actual stop barrier')
                    self.assertIsNone(provider.error)
                    streams = [events(h) for h in (first, second, parent, child)]
                    message_ids = [stream[0]['message']['id'] for stream in streams]
                    self.assertEqual(len(set(message_ids)), 4, 'distinct Message objects shared an identity')
                    self.assertTrue(all(isinstance(i, str) and i.startswith('msg_') for i in message_ids))
                    for stream in streams:
                        self.assertEqual([e['type'] for e in stream], ['message_start', 'content_block_start',
                            'content_block_delta', 'content_block_stop', 'message_delta', 'message_stop'])
                        self.assertEqual(sum('message' in e for e in stream), 1)
                    self.assertEqual(streams[0][1]['content_block']['id'], 'tempo-read')
                    self.assertEqual(streams[1][1]['content_block']['id'], 'tempo-agent')
                    self.assertEqual(streams[2][2]['delta']['text'], 'tempo-parent-complete')
                    self.assertEqual(streams[3][2]['delta']['text'], 'tempo-child-complete')
                    self.assertEqual(conversation.counts, {'parent': 3, 'child': 1})
                    self.assertTrue(conversation.independence_observed)
                finally:
                    provider.shutdown.set()
                    if worker.ident is not None:
                        worker.join(2)
                    self.assertFalse(worker.is_alive())

    def test_sse_has_complete_text_and_tool_lifecycle(self):
        for block in ({'type':'text','text':'done'}, {'type':'tool_use','id':'tempo-read','name':'Read','input':{'file_path':'/tmp/fixture'}}):
            events=smoke.sse_events(block, "msg_tempo_fixture_1")
            self.assertEqual([e['type'] for e in events], ['message_start','content_block_start','content_block_delta',
                             'content_block_stop','message_delta','message_stop'])
            self.assertEqual(events[-2]['delta']['stop_reason'], 'tool_use' if block['type']=='tool_use' else 'end_turn')
            if block['type']=='tool_use': self.assertEqual(json.loads(events[2]['delta']['partial_json']),block['input'])

    def test_tool_schema_required_fields_and_types_checked_before_call(self):
        body=request(); args={'file_path':'/tmp/fixture'}
        self.assertEqual(smoke.tool_item(body,'Read','tempo-read',args)['input'],args)
        mutations=[lambda b:b['tools'].clear(),lambda b:b['tools'].append(b['tools'][0]),
                   lambda b:b['tools'][0]['input_schema']['required'].append('unknown'),
                   lambda b:b['tools'][0]['input_schema']['properties']['file_path'].update(type='integer'),
                   lambda b:b['tools'][0]['input_schema']['properties']['file_path'].update(enum=['no'])]
        for mutate in mutations:
            bad=copy.deepcopy(body);mutate(bad)
            with self.assertRaises(smoke.FixtureFailure):smoke.tool_item(bad,'Read','tempo-read',args)

    def test_tool_results_do_not_classify_child_as_parent_or_supply_read_text(self):
        child=request('tempo-native-child-case',[result('tempo-agent','tempo-native-parent-case')])
        self.assertEqual(smoke.request_case(child),'child')
        nested=request('unrecognized',[result('tempo-agent','tempo-native-child-case')])
        with self.assertRaises(smoke.FixtureFailure):smoke.request_case(nested)
        with self.assertRaises(smoke.FixtureFailure):smoke.require_tool_result(request(), 'tempo-read','tempo-fixture-read-ok')
        with self.assertRaises(smoke.FixtureFailure):smoke.require_tool_result(request(results=[result('other')]),'tempo-read','tempo-fixture-read-ok')
        bad=result('tempo-read');bad['is_error']=True
        with self.assertRaises(smoke.FixtureFailure):smoke.require_tool_result(request(results=[bad]),'tempo-read','tempo-fixture-read-ok')

    def test_tool_result_failure_shapes_remain_distinct_and_fail_closed(self):
        good=result('tempo-agent','private-canary')
        cases=[([], 'actual_tool_result_missing'),
               ([good,good], 'actual_tool_result_duplicate'),
               ([dict(good,is_error='true')], 'actual_tool_result_invalid_error_flag'),
               ([dict(good,is_error=0)], 'actual_tool_result_invalid_error_flag'),
               ([dict(good,is_error=None)], 'actual_tool_result_invalid_error_flag'),
               ([dict(good,is_error=True)], 'agent_tool_error_unclassified_text_blocks')]
        for results,category in cases:
            with self.subTest(category=category),self.assertRaises(smoke.FixtureFailure) as failure:
                smoke.require_tool_result(request(results=results),'tempo-agent')
            self.assertEqual(str(failure.exception),category)
        for value in (good,dict(good,is_error=False)):
            smoke.require_tool_result(request(results=[value]),'tempo-agent')
        with self.assertRaises(smoke.FixtureFailure) as failure:
            smoke.require_tool_result(request(results=[dict(result('tempo-read'),is_error=True)]),'tempo-read',smoke.READ_RESULT)
        self.assertEqual(str(failure.exception),'actual_tool_result_error')

    def test_agent_error_classification_exports_only_fixed_categories(self):
        cases=[("Agent type 'tempo-fixture-child' not found. Available agents: private-canary",'type_unavailable'),
               ("Agent type 'tempo-fixture-child' has been denied by permission rule 'private-canary'",'permission'),
               ('Permission to use Agent has been denied: private-canary','permission'),
               ('Tool permission request failed: private-canary','permission'),
               ('Agent: launching needs the executor (call.runEngine)','executor_unavailable'),
               ('Subagent nesting limit reached (depth 5 of 5). private-canary','depth_limit'),
               ('Concurrent subagent limit reached. private-canary','concurrency_limit'),
               ('In-process teammates cannot spawn background agents. private-canary','background_unavailable'),
               ('private-canary','unclassified')]
        for text,category in cases:
            for content in (text,[{'type':'text','text':'<tool_use_error>'+text+'</tool_use_error>'}]):
                bad=dict(result('tempo-agent'),is_error=True,content=content)
                with self.subTest(category=category),self.assertRaises(smoke.FixtureFailure) as failure:
                    smoke.require_tool_result(request(results=[bad]),'tempo-agent')
                expected=category if category!='unclassified' else 'unclassified_text_'+('string' if isinstance(content,str) else 'blocks')
                self.assertEqual(str(failure.exception),'agent_tool_error_'+expected)
                self.assertNotIn('private-canary',str(failure.exception))
        for content,category in ((None,'unsupported_content'),({'error':'private-canary'},'unsupported_content'),
                                ([{'type':'image','text':cases[0][0]}],'no_text'),
                                ('private-canary'*2000,'oversized_text'),
                                ([{'type':'text','text':cases[0][0]}]*129,'too_many_blocks')):
            with self.assertRaises(smoke.FixtureFailure) as failure:
                smoke.require_tool_result(request(results=[dict(result('tempo-agent'),is_error=True,content=content)]),'tempo-agent')
            self.assertEqual(str(failure.exception),'agent_tool_error_'+category)

    def test_errored_agent_result_cannot_pass_complete_native_receipt_barriers(self):
        rows=[receipt('SessionStart'),receipt('UserPromptSubmit')]
        snap=snapshot(rows);model=smoke.Conversation(lambda:snap,Path('/tmp/project'))
        model.respond(request())
        rows.extend([receipt('PreToolUse',tool='tempo-read'),receipt('PostToolUse',tool='tempo-read')])
        model.respond(request(results=[result('tempo-read')]))
        rows.extend([receipt('PreToolUse',tool='tempo-agent'),receipt('PostToolUse',tool='tempo-agent')])
        child=receipt('SubagentStart','native-child',turn='child-prompt');rows.append(child)
        snap['actors'].append({'ref':child['actor'],'state':'working','health':'continuous'})
        model.respond(request(smoke.CHILD_PROMPT))
        with self.assertRaises(smoke.FixtureFailure) as failure:
            model.respond(request(results=[dict(result('tempo-agent','private-canary'),is_error=True)]))
        self.assertEqual(str(failure.exception),'agent_tool_error_unclassified_text_blocks')
        self.assertEqual(model.phase,'agent');self.assertEqual(model.counts,{'parent':2,'child':1})

    def test_shared_dispatch_errors_have_bounded_fixed_diagnostics(self):
        cases=[("Claude requested permissions to use Agent, but you haven't granted it yet.",'permission'),
               ('InputValidationError: private-canary','input_validation'),
               ('Execution stopped by PreToolUse hook: private-canary','hook_stopped'),
               ('Cancelled: Claude ended the conversation','cancelled'),
               ('Task output has no file in a diskless session','diskless_output'),
               ('A subagent cannot be started from here in this session.','cwd_unavailable'),
               ('Cannot read properties of undefined (reading taskRegistry)','runtime_type_task_registry'),
               ('undefined is not an object (evaluating e.options.toolCatalog)','runtime_type_tool_catalog'),
               ('e.agentLifecycle.markTypeInvoked is not a function','runtime_type_agent_lifecycle'),
               ('s.getSystemPrompt is not a function','runtime_type_system_prompt'),
               ('e.session.withProject is not a function','runtime_type_project_context'),
               ("Cannot destructure property 'sessionScratch' from null or undefined value",'runtime_type_session_scratch'),
               ('private-canary is not a function','runtime_type_other'),
               ("Can't find variable: private-canary",'runtime_reference'),
               ('private-canary is not defined','runtime_reference'),
               ('ENOENT: no such file or directory, open private-canary','filesystem'),
               ('EACCES: permission denied, open private-canary','filesystem'),
               ('Error calling tool (Agent): private-canary','dispatch_exception'),
               ('private-canary contains XENOENTZ and a taskRegistry mention','unclassified_text_blocks')]
        for text,category in cases:
            with self.subTest(category=category),self.assertRaises(smoke.FixtureFailure) as failure:
                smoke.require_tool_result(request(results=[dict(result('tempo-agent',text+' private-canary'),is_error=True)]),'tempo-agent')
            self.assertEqual(str(failure.exception),'agent_tool_error_'+category)
        # The aggregate text cap must apply before recognizing any fragment.
        oversized=[{'type':'text','text':cases[0][0]}]+[{'type':'text','text':'x'*200}]*127
        self.assertEqual(smoke.agent_error_category(oversized),'agent_tool_error_oversized_text')

    def test_permission_formatter_variants_and_empty_content_stay_fixed(self):
        permission=["Claude requested permissions to use Task, but you haven't granted it yet.",
                    "Claude requested permissions to use Agent(private-canary), but you haven't granted it yet.",
                    'Permission to use private-canary has been denied',
                    "Permission rule 'private-canary' requires approval for this Task command",
                    'User rejected tool use', 'User denied permission']
        for text in permission:
            self.assertEqual(smoke.agent_error_category(text),'agent_tool_error_permission')
        for content,category in (([],'no_text'),('', 'empty_text'),(' \n\t','empty_text'),
                                 ([{'type':'text','text':''}],'empty_text'),
                                 ([{'type':'text','text':17}],'no_text'),
                                 ('private-canary','unclassified_text_string'),
                                 ([{'type':'text','text':'private-canary'}],'unclassified_text_blocks'),
                                 ('Permission to use private-canary','unclassified_text_string'),
                                 ('private-canary has been denied','unclassified_text_string')):
            self.assertEqual(smoke.agent_error_category(content),'agent_tool_error_'+category)

    def test_initial_response_requires_unique_accepted_native_start_and_prompt(self):
        base=[receipt('SessionStart'),receipt('UserPromptSubmit')]
        for rows in ([],base[:1],base[1:],base+[dict(base[0],id='second')],base+[dict(base[1],turn_id='other')],
                     [base[0],dict(base[1],durability='not_committed')], [base[0],dict(base[1],ordering='unavailable')],
                     [base[0],dict(base[1],session_id='other')], [base[0],dict(base[1],turn_id='')]):
            with self.subTest(rows=rows):
                model=smoke.Conversation(lambda:snapshot(rows),Path('/tmp/project'))
                with self.assertRaises(smoke.FixtureFailure):model.respond(request())
                self.assertEqual(model.counts,{})
        model=smoke.Conversation(lambda:snapshot(base),Path('/tmp/project'))
        block,hold,_=model.respond(request());self.assertEqual(block['name'],'Read');self.assertFalse(hold)


    def test_initial_provider_response_rejects_nonworking_or_stale_root(self):
        for state,health in (('wait_user','continuous'),('working','stale'),('interrupted','continuous')):
            snap=snapshot([receipt('SessionStart'),receipt('UserPromptSubmit')])
            snap['actors'][0].update(state=state,health=health)
            model=smoke.Conversation(lambda:snap,Path('/tmp/project'))
            with self.assertRaises(smoke.FixtureFailure):model.respond(request())
            self.assertEqual(model.counts,{})

    def test_child_response_requires_actual_start_and_independent_working_actor(self):
        rows=[receipt('SessionStart'),receipt('UserPromptSubmit')]
        snap=snapshot(rows);model=smoke.Conversation(lambda:snap,Path('/tmp/project'))
        model.respond(request())
        rows.extend([receipt('PreToolUse',tool='tempo-read'),receipt('PostToolUse',tool='tempo-read')])
        block,_,_=model.respond(request(results=[result('tempo-read')]))
        self.assertEqual(block['name'],'Agent')
        with self.assertRaises(smoke.FixtureFailure):model.respond(request('tempo-native-child-case'))
        rows.append(receipt('PreToolUse',tool='tempo-agent'))
        start=receipt('SubagentStart','native-child',turn='child-prompt');rows.append(start)
        snap['actors'].append({'ref':start['actor'],'state':'working','health':'continuous'})
        block,hold,_=model.respond(request('tempo-native-child-case'));self.assertTrue(hold)
        self.assertEqual(model.child,'native-child');self.assertEqual(model.child_turn,'child-prompt')
        with self.assertRaises(smoke.FixtureFailure):model.respond(request('tempo-native-child-case'))


    def test_parent_final_requires_actual_agent_result_and_both_tool_receipts(self):
        for child_first in (False,True):
            rows=[receipt('SessionStart'),receipt('UserPromptSubmit')]
            snap=snapshot(rows);model=smoke.Conversation(lambda:snap,Path('/tmp/project'))
            model.respond(request())
            rows.extend([receipt('PreToolUse',tool='tempo-read'),receipt('PostToolUse',tool='tempo-read')])
            model.respond(request(results=[result('tempo-read')]))
            with self.assertRaises(smoke.FixtureFailure):model.respond(request(results=[result('tempo-agent','launched')]))
            rows.append(receipt('PreToolUse',tool='tempo-agent'))
            start=receipt('SubagentStart','native-child',turn='child-prompt');rows.append(start)
            snap['actors'].append({'ref':start['actor'],'state':'working','health':'continuous'})
            if child_first:model.respond(request('tempo-native-child-case'))
            rows.append(receipt('PostToolUse',tool='tempo-agent'))
            block,hold,_=model.respond(request(results=[result('tempo-agent','launched')]))
            self.assertEqual(block['type'],'text');self.assertFalse(hold)
            if not child_first:model.respond(request('tempo-native-child-case'))
            self.assertEqual(model.counts,{'parent':3,'child':1})
            with self.assertRaises(smoke.FixtureFailure):model.respond(request())

    def test_parent_stop_child_working_oracle_rejects_cascade_and_collision(self):
        parent=receipt('Stop');child=receipt('SubagentStart','child',turn='child-turn')
        snap=snapshot([parent,child]);snap['actors'].append({'ref':parent['actor'],'state':'wait_user','health':'continuous'})
        smoke.require_independence(snap,parent,child)
        for state in ('wait_user','interrupted','finished'):
            bad=copy.deepcopy(snap);bad['actors'][0]['state']=state
            with self.assertRaises(smoke.FixtureFailure):smoke.require_independence(bad,parent,child)
        with self.assertRaises(smoke.FixtureFailure):smoke.require_independence(snap,parent,dict(child,actor=parent['actor']))

    def test_receipt_projection_discards_raw_fields_and_rejects_arbitrary_identifiers(self):
        row=receipt('Stop');row.update(prompt='canary',transcript_path='canary',error='canary',diagnostic_code='canary')
        projected=smoke.project_receipt(row);self.assertNotIn('canary',json.dumps(projected))
        for key,val in [('origin','host_observed'),('agent_id','bad\ncanary'),('kind','canary')]:
            with self.assertRaises(smoke.FixtureFailure):smoke.project_receipt(dict(row,**{key:val}))

    def test_http_request_contract_bounds_and_endpoint_allowlist(self):
        headers={'Content-Length':'2','x-api-key':'tempo-ci-invalid-synthetic-key'}
        self.assertEqual(smoke.validate_http_request('/claude/v1/messages?beta=true',headers),2)
        for path,headers2 in [('/other',headers),('/claude/v1/messages?anything=yes',headers),
              ('/claude/v1/messages',dict(headers,**{'Content-Length':str(2*1024*1024+1)})),
              ('/claude/v1/messages',dict(headers,**{'Transfer-Encoding':'chunked'})),
              ('/claude/v1/messages',dict(headers,**{'Content-Encoding':'gzip'})),
              ('/claude/v1/messages',dict(headers,**{'x-api-key':'real-key'}))]:
            with self.assertRaises(smoke.FixtureFailure):smoke.validate_http_request(path,headers2)


    def test_http_connection_and_request_budgets_are_finite(self):
        budget=smoke.ProviderBudget()
        for _ in range(4):self.assertTrue(budget.claim())
        self.assertFalse(budget.claim())
        budget.release();self.assertTrue(budget.claim())
        for _ in range(8):budget.enter_request()
        with self.assertRaises(smoke.FixtureFailure):budget.enter_request()

    def test_http_socket_timeout_precedes_header_parsing_and_errors_stay_fixed(self):
        events=[]
        class Socket:
            def settimeout(self,n):events.append(('timeout',n))
            def makefile(self,*_):
                events.append(('read',None))
                return io.BytesIO(b'POST /unknown HTTP/1.1\r\nHost: localhost\r\nContent-Length: 2\r\n\r\n{}')
            def sendall(self,data):events.append(('write',len(data)))
        provider=types.SimpleNamespace(error=None,budget=smoke.ProviderBudget())
        handler=smoke.make_handler(provider)
        handler(Socket(),('127.0.0.1',1),types.SimpleNamespace())
        self.assertEqual(events[0],('timeout',5))
        self.assertEqual(provider.error,'unexpected_provider_endpoint')

    def test_provider_cleanup_joins_and_late_failure_vetoes_pass(self):
        provider=smoke.Provider.__new__(smoke.Provider)
        provider.shutdown=threading.Event();provider.error=None
        provider.server=mock.Mock();provider.thread=mock.Mock()
        def late_failure():provider.error='provider_protocol_failed'
        provider.server.server_close.side_effect=late_failure
        with self.assertRaises(smoke.FixtureFailure):provider.close()
        provider.server.shutdown.assert_called_once();provider.server.server_close.assert_called_once()
        provider.thread.join.assert_called_once()
        self.assertTrue(provider.shutdown.is_set())


class ProcessTests(unittest.TestCase):
    def test_inert_process_output_cap_and_deadline(self):
        env={'PATH':'/usr/bin:/bin'}
        started=time.monotonic()
        with self.assertRaises(smoke.FixtureFailure):
            smoke.bounded_run([sys.executable,'-c','import os,time;os.write(1,b"x"*1100000);time.sleep(8)'],env,Path('/tmp'),timeout=2)
        self.assertLess(time.monotonic()-started,4)
        with self.assertRaises(smoke.FixtureFailure):
            smoke.bounded_run([sys.executable,'-c','import time;time.sleep(8)'],env,Path('/tmp'),timeout=.05)


    def test_main_cleanup_failure_cannot_retain_pass_or_export_raw_errors(self):
        with tempfile.TemporaryDirectory() as d:
            evidence=Path(d)/'evidence.json'
            def failure(args,report):
                report['status']='passed'
                raise RuntimeError('private-canary')
            with mock.patch.object(smoke,'run',side_effect=failure), mock.patch.object(sys,'argv',
                 ['claude_smoke','--tempo','inert','--helper','inert','--evidence',str(evidence)]):
                self.assertEqual(smoke.main(),1)
            data=evidence.read_text();self.assertNotIn('private-canary',data)
            self.assertEqual(json.loads(data)['status'],'failed')

    def test_main_exports_agent_failure_category_without_raw_result_or_canary(self):
        for text,category in [('private-canary','agent_tool_error_unclassified_text_blocks'),
                              ('Permission to use Agent has been denied: private-canary','agent_tool_error_permission'),
                              ('private-canary is not a function','agent_tool_error_runtime_type_other'),
                              ('ENOENT private-canary','agent_tool_error_filesystem')]:
            with tempfile.TemporaryDirectory() as d:
                evidence=Path(d)/'evidence.json'
                def failure(args,report):
                    smoke.require_tool_result(request(results=[dict(result('tempo-agent',text),is_error=True)]),'tempo-agent')
                with mock.patch.object(smoke,'run',side_effect=failure),mock.patch.object(sys,'argv',
                     ['claude_smoke','--tempo','inert','--helper','inert','--evidence',str(evidence)]):
                    self.assertEqual(smoke.main(),1)
                data=evidence.read_text()
                self.assertNotIn('private-canary',data);self.assertNotIn('Permission to use Agent',data)
                self.assertEqual(json.loads(data)['failure_category'],category)
                self.assertEqual(json.loads(data)['status'],'failed')

    def test_cancellation_uses_cleanup_failure_path(self):
        with tempfile.TemporaryDirectory() as d:
            evidence=Path(d)/'evidence.json';handlers={}
            def install(sig,fn):handlers[sig]=fn
            def cancelled(args,report):handlers[signal.SIGTERM](None,None)
            with mock.patch.object(smoke,'run',side_effect=cancelled),mock.patch('signal.signal',side_effect=install), \
                 mock.patch('signal.alarm'),mock.patch.object(sys,'argv',
                 ['claude_smoke','--tempo','inert','--helper','inert','--evidence',str(evidence)]):
                self.assertEqual(smoke.main(),1)
            self.assertEqual(json.loads(evidence.read_text())['failure_category'],'fixture_cancelled')



class AcceptanceTests(unittest.TestCase):
    def complete(self):
        rows=[receipt('SessionStart'),receipt('UserPromptSubmit'),
              receipt('PreToolUse',tool='tempo-read'),receipt('PostToolUse',tool='tempo-read'),
              receipt('PreToolUse',tool='tempo-agent'),receipt('PostToolUse',tool='tempo-agent'),
              receipt('SubagentStart','child',turn='child-turn'),receipt('Stop'),
              receipt('SubagentStop','child',turn='child-turn'),receipt('SessionEnd')]
        snap=snapshot(rows)
        snap['actors'][0]['state']='interrupted';snap['actors'][1]['state']='wait_user';snap['queued']=1
        model=smoke.Conversation(lambda:snap,Path('/tmp/project'))
        model.session='session-1';model.turn='prompt-1';model.child='child';model.child_turn='child-turn'
        model.counts={'parent':3,'child':1};model.independence_observed=True
        return model,snap

    def test_final_acceptance_requires_every_exact_terminal_and_effect(self):
        model,snap=self.complete();smoke.require_final(model,snap)
        changes=[lambda m,s:s['receipts'].pop(),
                 lambda m,s:s['receipts'][-2].update(agent_id='other'),
                 lambda m,s:s['receipts'][-2].update(turn_id='parent-turn'),
                 lambda m,s:s['receipts'][-1].update(disposition='stale'),
                 lambda m,s:s['actors'][1].update(state='interrupted'),
                 lambda m,s:s['actors'][0].update(state='working'),
                 lambda m,s:s.update(uncertainties=1),lambda m,s:s.update(capture_reviews=1),
                 lambda m,s:s.update(queued=0),lambda m,s:setattr(m,'independence_observed',False),
                 lambda m,s:m.counts.update(parent=4),
                 lambda m,s:s['receipts'][-1].update(actor={'foreign':'actor'})]
        for change in changes:
            model,snap=self.complete();change(model,snap)
            with self.assertRaises(smoke.FixtureFailure):smoke.require_final(model,snap)

    def test_runtime_version_is_exact_and_never_relabels_unknown_output(self):
        smoke.require_runtime_version(b'2.1.286 (Claude Code)\n')
        for value in (b'2.1.285 (Claude Code)\n',b'canary 2.1.286',b'2.1.286 (Claude Code)\ncanary'):
            with self.assertRaises(smoke.FixtureFailure):smoke.require_runtime_version(value)

    def test_run_rejects_nonhosted_before_any_resource_creation(self):
        with mock.patch.object(smoke.os,'environ',{}),mock.patch.object(smoke.platform,'system',return_value='Darwin'), \
             mock.patch.object(smoke,'download_runtime') as download,mock.patch.object(smoke.tempfile,'mkdtemp') as mkdir, \
             mock.patch.object(smoke,'Provider') as provider:
            with self.assertRaisesRegex(smoke.FixtureFailure,'hosted_platform_required'):
                smoke.run(types.SimpleNamespace(),{})
            download.assert_not_called();mkdir.assert_not_called();provider.assert_not_called()

    def test_settings_have_only_direct_synchronous_hooks_and_no_grants(self):
        with tempfile.TemporaryDirectory(prefix='tempo-') as d:
            root=Path(d);(root/'tempo').write_text('inert')
            settings=smoke.prepare_settings(root/'tempo',root/'errors')
            self.assertEqual(set(settings),{'hooks'})
            self.assertEqual(set(settings['hooks']),set(smoke.EVENTS))
            for groups in settings['hooks'].values():
                self.assertEqual(groups,[{'hooks':[{'type':'command','command':str(root/'tempo')+' hook claude --input-stdin 2>> '+str(root/'errors'),'timeout':2}]}])
            self.assertEqual((root/'errors').stat().st_mode & 0o777,0o600)
            with self.assertRaises(smoke.FixtureFailure):smoke.prepare_settings(root/'tempo',root/'errors')


    def test_http_slow_trickle_cannot_extend_total_input_deadline(self):
        clock=[0.0]
        class Source:
            calls=0
            def read1(self,n):self.calls+=1;clock[0]+=2;return b'x'
            def close(self):pass
        for operation in ('read','readline'):
            clock[0]=0;source=Source();sock=mock.Mock()
            reader=smoke.DeadlineReader(source,sock,deadline=5,now=lambda:clock[0])
            with self.assertRaises(smoke.FixtureFailure):getattr(reader,operation)(100)
            self.assertLessEqual(source.calls,3)

    def test_provider_headers_have_aggregate_bound(self):
        source=io.BytesIO(b'a'*16000+b'\r\n'+b'b'*1000+b'\r\n')
        reader=smoke.HeaderReader(source);reader.readline(65537)
        with self.assertRaises(smoke.FixtureFailure):reader.readline(65537)

    def test_unsupported_method_and_malformed_json_veto_provider(self):
        class Socket:
            def __init__(self,data):self.data=data
            def settimeout(self,n):pass
            def makefile(self,*_):return io.BytesIO(self.data)
            def sendall(self,data):pass
        for data in (b'PATCH /claude/v1/messages HTTP/1.1\r\nHost: local\r\n\r\n',
                     b'POST /claude/v1/messages HTTP/1.1\r\nContent-Length: 1\r\nx-api-key: tempo-ci-invalid-synthetic-key\r\n\r\n{'):
            provider=types.SimpleNamespace(error=None,budget=smoke.ProviderBudget())
            smoke.make_handler(provider)(Socket(data),('127.0.0.1',1),types.SimpleNamespace())
            self.assertIsNotNone(provider.error)

class NativeApprovalDiagnosticTests(unittest.TestCase):
    KEYS = ('model', 'validation', 'permission', 'session', 'background', 'api')
    VOCABULARY = {
        'model': ('model', 'models', 'model_access'),
        'validation': ('validation', 'invalid', 'schema', 'parameter', 'parameters', 'argument',
                       'arguments', 'required', 'inputvalidationerror'),
        'permission': ('permission', 'permissions', 'approval', 'denied', 'rejected', 'allowlist'),
        'session': ('session', 'sessions', 'diskless', 'persistence'),
        'background': ('background', 'run_in_background', 'asynchronous', 'concurrent', 'nesting'),
        'api': ('api', 'authentication_error', 'permission_error', 'invalid_request_error',
                'rate_limit_error', 'connection', 'timed out'),
    }

    def domains(self, *enabled):
        return {key: key in enabled for key in self.KEYS}

    def agent_model(self):
        rows = [receipt('SessionStart'), receipt('UserPromptSubmit')]
        snap = snapshot(rows)
        model = smoke.Conversation(lambda: snap, Path('/tmp/project'))
        read, hold, _ = model.respond(request())
        self.assertEqual(read, {'type': 'tool_use', 'id': 'tempo-read', 'name': 'Read',
                                'input': {'file_path': '/tmp/project/fixture.txt'}})
        self.assertFalse(hold)
        rows.extend([receipt('PreToolUse', tool='tempo-read'), receipt('PostToolUse', tool='tempo-read')])
        child, hold, _ = model.respond(request(results=[result('tempo-read')]))
        self.assertEqual(child, {'type': 'tool_use', 'id': 'tempo-agent', 'name': 'Agent', 'input': {
            'description': 'Synthetic lifecycle child', 'prompt': smoke.CHILD_PROMPT,
            'subagent_type': 'tempo-fixture-child', 'run_in_background': True}})
        self.assertFalse(hold)
        rows.extend([receipt('PreToolUse', tool='tempo-agent'), receipt('PostToolUse', tool='tempo-agent')])
        return model

    def test_exact_argv_and_rejected_rule_paths(self):
        project = Path('/tmp/tempo-fixture/project')
        argv = smoke.claude_argv(Path('/tmp/runtime'), project)
        agents = {'tempo-fixture-child': {'description': 'Synthetic lifecycle child',
                  'prompt': 'Complete the supplied synthetic lifecycle case.', 'tools': ['Read'],
                  'model': smoke.MODEL, 'background': True}}
        self.assertEqual(argv, ['/tmp/runtime', '--print', '--setting-sources', 'project,local',
            '--tools', 'Read,Agent', '--allowedTools', 'Read(//tmp/tempo-fixture/project/fixture.txt),Agent',
            '--strict-mcp-config', '--mcp-config', str(project / 'mcp.json'), '--no-session-persistence',
            '--model', smoke.MODEL, '--max-turns', '4', '--agents', json.dumps(agents), smoke.PARENT_PROMPT])
        for path in ('relative', '/tmp/private path', '/tmp/private,Agent', '/tmp/private(*)',
                     '/tmp/private*', '/tmp/private\ncanary', '/tmp/private\\canary', '/tmp/private?'):
            with self.subTest(path=path), self.assertRaises(smoke.FixtureFailure):
                smoke.claude_argv(Path('/tmp/runtime'), Path(path))

    def test_provider_blocks_are_complete_fixed_fixture_invocations(self):
        model = self.agent_model()
        self.assertEqual(model.phase, 'agent')
        self.assertEqual(model.counts, {'parent': 2})

    def test_closed_domain_vocabulary_has_fixed_boolean_schema_and_identifier_boundaries(self):
        for domain, words in self.VOCABULARY.items():
            for word in words:
                with self.subTest(domain=domain, word=word):
                    actual = smoke.agent_error_domains('PRIVATE_CANARY: (' + word.upper() + ').')
                    self.assertEqual(list(actual), list(self.KEYS))
                    self.assertTrue(all(type(value) is bool for value in actual.values()))
                    self.assertEqual(actual, self.domains(domain))
        self.assertEqual(smoke.agent_error_domains('invalid model approval in diskless background API'), self.domains(*self.KEYS))
        for text in ('PRIVATE_CANARY', 'modelName', 'private-model-canary', 'model_access_extra',
                     'sessionScratch', 'backgrounded', 'permissions_extra', 'schema2', 'api_key_private'):
            with self.subTest(text=text):
                self.assertEqual(smoke.agent_error_domains(text), self.domains())

    def test_domain_bounds_and_mixed_content_preserve_shape_categories(self):
        self.assertEqual(smoke.agent_error_domains('model ' + 'x' * 16378), self.domains('model'))
        self.assertEqual(smoke.agent_error_domains('model ' + 'x' * 16379), self.domains())
        blocks = [{'type': 'text', 'text': ''}] * 127 + [{'type': 'text', 'text': 'model'}]
        self.assertEqual(smoke.agent_error_domains(blocks), self.domains('model'))
        self.assertEqual(smoke.agent_error_domains(blocks + [{'type': 'text', 'text': 'permission'}]), self.domains())
        mixed = [None, 42, {'type': 'image', 'text': 'model PRIVATE_CANARY'},
                 {'type': 'text', 'text': False}, {'type': 'text', 'text': 'permission'}]
        self.assertEqual(smoke.agent_error_domains(mixed), self.domains('permission'))
        malformed = [(None, 'unsupported_content'), ({'text': 'model'}, 'unsupported_content'),
                     ([], 'no_text'), ([{'type': 'text', 'text': False}], 'no_text'),
                     (' ', 'empty_text'), ('model ' + 'x' * 16379, 'oversized_text'),
                     (blocks + [{'type': 'text', 'text': 'permission'}], 'too_many_blocks')]
        for content, category in malformed:
            with self.subTest(category=category):
                self.assertEqual(smoke.agent_error_domains(content), self.domains())
                bad = dict(result('tempo-agent'), is_error=True, content=content)
                with self.assertRaises(smoke.AgentToolFailure) as failure:
                    smoke.require_tool_result(request(results=[bad]), 'tempo-agent')
                self.assertEqual(str(failure.exception), 'agent_tool_error_' + category)
                self.assertEqual(failure.exception.domains, self.domains())

    def test_agent_failure_constructor_and_first_error_retention_never_expose_raw_input(self):
        forged = smoke.AgentToolFailure('PRIVATE_CANARY', {'model': 1, 'permission': True, 'PRIVATE_CANARY': 'PRIVATE_CANARY'})
        self.assertIsInstance(forged, smoke.FixtureFailure)
        self.assertEqual(str(forged), 'agent_tool_error_unclassified_text_string')
        self.assertEqual(forged.domains, self.domains('permission'))
        self.assertNotIn('PRIVATE_CANARY', repr(forged))
        self.assertNotIn('PRIVATE_CANARY', json.dumps(forged.domains))
        prefixed = smoke.AgentToolFailure('agent_tool_error_PRIVATE_CANARY', None)
        self.assertEqual(str(prefixed), 'agent_tool_error_unclassified_text_string')
        self.assertEqual(prefixed.domains, self.domains())
        model = self.agent_model()
        self.assertIsNone(model.error_domains)
        for index, text in enumerate(('unrecognized model PRIVATE_CANARY', 'permission session PRIVATE_CANARY')):
            bad = dict(result('tempo-agent'), is_error=True, content=text)
            with self.assertRaises(smoke.AgentToolFailure) as failure:
                model.respond(request(results=[bad]))
            self.assertNotIn('PRIVATE_CANARY', str(failure.exception))
            self.assertEqual(model.error_domains, self.domains('model'))
            self.assertEqual(model.phase, 'agent')
            self.assertEqual(model.counts, {'parent': 2})
        for text in ('PRIVATE_CANARY', 'model validation permission session background api PRIVATE_CANARY'):
            bad = dict(result('tempo-agent'), is_error=True, content=text)
            with self.assertRaises(smoke.AgentToolFailure):
                smoke.require_tool_result(request(results=[bad]), 'tempo-agent')

    def test_actual_run_finally_exports_only_retained_fixed_domains(self):
        for fail_agent in (False, True):
            with self.subTest(fail_agent=fail_agent), tempfile.TemporaryDirectory(prefix='tempo-approval-qa-') as tmp:
                fixture = Path(tmp).resolve()
                root, home = fixture / 'owned', fixture / 'home'
                home.mkdir()
                runtime, tempo, helper = (fixture / name for name in ('runtime', 'tempo', 'helper'))
                for path in (runtime, tempo, helper):
                    path.write_text('inert file')
                model = self.agent_model()
                if fail_agent:
                    bad = dict(result('tempo-agent'), is_error=True, content='novel model PRIVATE_CANARY')
                    with self.assertRaises(smoke.AgentToolFailure) as caught:
                        model.respond(request(results=[bad]))
                    failure = caught.exception
                    # Fault injection at the report boundary must not permit
                    # copied dynamic keys or bool-like native values to escape.
                    model.error_domains.update({'PRIVATE_CANARY': 'PRIVATE_CANARY', 'api': 1})
                else:
                    failure = smoke.FixtureFailure('fixture_cancelled')
                model.read = lambda: {'receipts': []}
                provider = types.SimpleNamespace(server=types.SimpleNamespace(server_port=43210),
                    budget=smoke.ProviderBudget(), error=None, close=lambda: None)
                def bounded(argv, *_args, **_kwargs):
                    if argv[0] == str(runtime):
                        if argv[1:] == ['--version']:
                            return b'2.1.286 (Claude Code)\n'
                        raise failure
                    action = argv[2]
                    if action == 'confirm':
                        return json.dumps({'basis': 'operator_declared', 'fingerprint': 'a' * 64}).encode()
                    if action == 'read':
                        return b'{"receipts":[]}'
                    self.assertEqual(action, 'link')
                    return b''
                def owned_root(**_kwargs):
                    root.mkdir()
                    return str(root)
                real_home = os.environ['HOME']
                def boundary_path(value):
                    return home if str(value) == real_home else Path(value)
                report = {}
                with mock.patch.dict(os.environ, {'RUNNER_TEMP': str(fixture), 'GITHUB_SHA': 'd' * 40}), \
                     mock.patch.object(smoke, 'Path', side_effect=boundary_path), \
                     mock.patch.object(smoke, 'hosted_precondition'), mock.patch.object(smoke, 'require_absent'), \
                     mock.patch.object(smoke, 'child_environment', return_value={'TEMPO_STATE': str(root / 'state'), 'TEMPO_HOOK_STATE': str(root / 'policy')}), \
                     mock.patch.object(smoke.tempfile, 'mkdtemp', side_effect=owned_root), \
                     mock.patch.object(smoke, 'download_runtime', return_value=runtime), \
                     mock.patch.object(smoke, 'bounded_run', side_effect=bounded), \
                     mock.patch.object(smoke, 'Conversation', return_value=model), \
                     mock.patch.object(smoke, 'Provider', return_value=provider):
                    with self.assertRaises(smoke.FixtureFailure):
                        smoke.run(types.SimpleNamespace(tempo=str(tempo), helper=str(helper)), report)
                if fail_agent:
                    self.assertEqual(report['agent_error_domains'], self.domains('model'))
                    self.assertEqual(list(report['agent_error_domains']), list(self.KEYS))
                else:
                    self.assertNotIn('agent_error_domains', report)
                self.assertNotIn('PRIVATE_CANARY', json.dumps(report))
                self.assertNotEqual(report.get('status'), 'passed')
                self.assertEqual(os.environ['HOME'], real_home)
                self.assertFalse(root.exists())


class ProviderFirstRejectionTests(unittest.TestCase):
    FAMILIES = ('messages', 'count_tokens', 'other')
    CATEGORIES = ('unexpected_provider_endpoint', 'synthetic_provider_auth', 'unexpected_request_encoding',
        'provider_header_contract', 'provider_request_bound', 'provider_input_deadline', 'provider_header_bound',
        'provider_request_contract', 'provider_request_after_shutdown', 'provider_concurrency_bound',
        'unexpected_provider_turn', 'provider_turn_ambiguous', 'actual_tool_result_missing',
        'actual_tool_result_duplicate', 'actual_tool_result_invalid_error_flag', 'actual_tool_result_error',
        'actual_read_result_missing', 'tool_schema_mismatch', 'required_tool_schema_unavailable',
        'provider_response_bound', 'parent_stop_missing')

    def provider(self, conversation=None):
        return types.SimpleNamespace(conversation=conversation or smoke.Conversation(
            lambda: snapshot([receipt('SessionStart'), receipt('UserPromptSubmit')]), Path('/tmp/project')),
            budget=smoke.ProviderBudget(), shutdown=threading.Event(), deadline=time.monotonic()+2, error=None)

    def handler(self, provider, body=None, path='/claude/v1/messages', raw=None, reader=None):
        data = json.dumps(body if body is not None else request()).encode() if raw is None else raw
        handler = smoke.make_handler(provider).__new__(smoke.make_handler(provider))
        handler.path, handler.headers = path, {'Content-Length': str(len(data)), 'x-api-key': smoke.TOKEN}
        handler.rfile, handler.wfile = reader or io.BytesIO(data), io.BytesIO()
        handler.codes, handler.output_headers = [], []
        handler.send_response = lambda code, *_: handler.codes.append(code)
        handler.send_header = lambda *args: handler.output_headers.append(args)
        handler.end_headers = lambda: None
        return handler

    def record(self, category, family='messages', stream='unavailable', model='unavailable'):
        return {'category': category, 'endpoint_family': family, 'stream': stream, 'model': model}

    def projected(self, provider):
        return provider.budget.project_first_rejections()

    def assert_empty_rejection(self, handler, code=400):
        self.assertEqual(handler.codes, [code])
        self.assertIn(('Connection', 'close'), handler.output_headers)
        self.assertEqual(handler.wfile.getvalue(), b'')

    def test_actual_handler_preserves_unread_auxiliary_and_earlier_messages_before_agent_error(self):
        rows = [receipt('SessionStart'), receipt('UserPromptSubmit')]
        conversation = smoke.Conversation(lambda: snapshot(rows), Path('/tmp/project'))
        provider = self.provider(conversation)
        class Unread:
            def read(self, *_): raise AssertionError('auxiliary body was read for diagnostics')
        aux = self.handler(provider, path='/claude/v1/messages/count_tokens?beta=true', reader=Unread())
        aux.do_POST(); self.assert_empty_rejection(aux)
        bad = self.handler(provider, dict(request(), stream=False, model='PRIVATE_CANARY_MODEL'))
        bad.do_POST(); self.assert_empty_rejection(bad)
        first = self.handler(provider); first.do_POST(); self.assertEqual(first.codes, [200])
        rows.extend([receipt('PreToolUse', tool='tempo-read'), receipt('PostToolUse', tool='tempo-read')])
        second = self.handler(provider, request(results=[result('tempo-read')]))
        second.do_POST(); self.assertEqual(second.codes, [200])
        agent = self.handler(provider, request(results=[dict(result('tempo-agent', 'PRIVATE_CANARY'), is_error=True)]))
        agent.do_POST(); self.assert_empty_rejection(agent)
        self.assertEqual(provider.error, 'agent_tool_error_unclassified_text_blocks')
        self.assertEqual(conversation.counts, {'parent': 2})
        self.assertEqual(provider.budget.requests, 5)
        records = self.projected(provider)
        self.assertEqual(records, {'messages': self.record('provider_request_contract', stream='false', model='other'),
            'count_tokens': self.record('unexpected_provider_endpoint', 'count_tokens')})
        self.assertEqual(list(records), ['messages', 'count_tokens'])
        self.assertNotIn('PRIVATE_CANARY', json.dumps(records))

    def test_successful_read_agent_and_completion_do_not_create_rejections(self):
        rows = [receipt('SessionStart'), receipt('UserPromptSubmit')]
        provider = self.provider(smoke.Conversation(lambda: snapshot(rows), Path('/tmp/project')))
        first = self.handler(provider); first.do_POST()
        rows.extend([receipt('PreToolUse', tool='tempo-read'), receipt('PostToolUse', tool='tempo-read')])
        second = self.handler(provider, request(results=[result('tempo-read')])); second.do_POST()
        rows.extend([receipt('PreToolUse', tool='tempo-agent'), receipt('PostToolUse', tool='tempo-agent')])
        third = self.handler(provider, request(results=[result('tempo-agent')])); third.do_POST()
        self.assertEqual([h.codes for h in (first, second, third)], [[200]]*3)
        self.assertIsNone(provider.error)
        self.assertEqual(self.projected(provider), {})

    def test_actual_handler_exact_endpoint_families_without_auxiliary_body_read(self):
        class Unread:
            def read(self, *_): raise AssertionError('rejected endpoint body read')
        cases = [('/claude/v1/messages', 'messages', False), ('/claude/v1/messages?beta=true', 'messages', False),
            ('/claude/v1/messages/count_tokens', 'count_tokens', True),
            ('/claude/v1/messages/count_tokens?beta=true', 'count_tokens', True),
            ('/claude/v1/messages?PRIVATE_CANARY=yes', 'other', True),
            ('/claude/v1/messages/count_tokens?beta=false', 'other', True),
            ('/claude/v1/messages/count_tokens/PRIVATE_CANARY', 'other', True),
            ('/PRIVATE_CANARY', 'other', True)]
        for path, family, unread in cases:
            with self.subTest(path=path):
                provider = self.provider()
                h = self.handler(provider, dict(request(), stream=False), path, reader=Unread() if unread else None)
                h.do_POST(); self.assert_empty_rejection(h)
                expected = self.record('unexpected_provider_endpoint', family) if unread else self.record(
                    'provider_request_contract', family, 'false', 'fixture')
                self.assertEqual(self.projected(provider), {family: expected})
                self.assertNotIn('PRIVATE_CANARY', json.dumps(self.projected(provider)))

    def test_actual_handler_unparsed_and_nondict_bodies_remain_unavailable(self):
        for raw, category in ((b'{PRIVATE_CANARY', 'provider_protocol_failed'), (b'[]', 'provider_request_contract'),
                (b'null', 'provider_request_contract'), (b'"PRIVATE_CANARY"', 'provider_request_contract'),
                (b'', 'provider_request_bound')):
            with self.subTest(raw=raw):
                provider = self.provider(); h = self.handler(provider, raw=raw)
                h.do_POST(); self.assert_empty_rejection(h)
                self.assertEqual(self.projected(provider), {'messages': self.record(category)})
        provider = self.provider(); h = self.handler(provider, raw=b'{}')
        h.headers['Content-Length'] = '9'; h.do_POST()
        self.assertEqual(self.projected(provider), {'messages': self.record('provider_request_bound')})

    def test_actual_handler_stream_and_model_relations_are_exact_enums(self):
        missing = object()
        for value, stream in ((True, 'true'), (False, 'false'), (missing, 'missing'), (1, 'other'),
                (0, 'other'), ('true', 'other'), (None, 'other'), ([], 'other')):
            for model_value, model in ((smoke.MODEL, 'fixture'), ('PRIVATE_CANARY', 'other'),
                    (missing, 'missing'), (1, 'other_type'), (None, 'other_type'), ({}, 'other_type')):
                with self.subTest(stream=stream, model=model):
                    body = {'stream': value, 'model': model_value}
                    if value is missing: del body['stream']
                    if model_value is missing: del body['model']
                    provider = self.provider()
                    # A deterministic later failure permits observing valid relations too.
                    provider.conversation.respond = mock.Mock(side_effect=smoke.FixtureFailure('provider_request_contract'))
                    h = self.handler(provider, body); h.do_POST(); self.assert_empty_rejection(h)
                    self.assertEqual(self.projected(provider), {'messages': self.record(
                        'provider_request_contract', stream=stream, model=model)})
                    self.assertNotIn('PRIVATE_CANARY', json.dumps(self.projected(provider)))

    def test_closed_category_table_agent_precedence_and_arbitrary_exception_fallbacks(self):
        for category in self.CATEGORIES:
            provider = self.provider()
            smoke.record_failure(provider, smoke.FixtureFailure(category), '/claude/v1/messages', request())
            self.assertEqual(self.projected(provider), {'messages': self.record(category, stream='true', model='fixture')})
        failures = [(smoke.AgentToolFailure('agent_tool_error_unclassified_text_string', None), 'agent_result_error'),
            (smoke.FixtureFailure('PRIVATE_CANARY'), 'fixture_oracle_failed'),
            (smoke.FixtureFailure('provider_request_contract PRIVATE_CANARY'), 'fixture_oracle_failed'),
            (RuntimeError('provider_request_contract'), 'provider_protocol_failed'),
            (RuntimeError('PRIVATE_CANARY'), 'provider_protocol_failed')]
        for failure, category in failures:
            provider = self.provider(); smoke.record_failure(provider, failure)
            self.assertEqual(self.projected(provider), {'other': self.record(category, 'other')})
            self.assertNotIn('PRIVATE_CANARY', json.dumps(self.projected(provider)))

    def test_all_seven_error_sites_keep_http_semantics_and_terminal_error_veto(self):
        for site in ('log_error', 'send_error', 'handle', 'do_POST', 'reject', 'process_request', 'handle_error'):
            with self.subTest(site=site):
                if site in ('process_request', 'handle_error'):
                    with mock.patch.object(smoke.http.server.ThreadingHTTPServer, '__init__', return_value=None), \
                         mock.patch.object(smoke.threading.Thread, 'start'):
                        provider = smoke.Provider(mock.Mock(), time.monotonic()+2)
                    provider.server.shutdown_request = mock.Mock()
                    if site == 'process_request':
                        provider.budget.active = 4
                        provider.server.process_request(object(), ('127.0.0.1', 1))
                        provider.server.shutdown_request.assert_called_once()
                        category = 'provider_concurrency_bound'
                    else:
                        provider.server.handle_error(object(), ('127.0.0.1', 1))
                        category = 'provider_protocol_failed'
                    family = 'other'
                else:
                    provider = self.provider(); h = self.handler(provider, path='/PRIVATE_CANARY')
                    family, category = 'other', 'provider_protocol_failed'
                    if site == 'log_error': h.log_error('PRIVATE_CANARY %s', 'PRIVATE_CANARY')
                    elif site == 'send_error':
                        h.request_version, h.command = 'HTTP/1.1', 'PATCH'
                        h.send_error(501, 'PRIVATE_CANARY', 'PRIVATE_CANARY')
                        self.assertEqual(h.codes, [501]); self.assertNotIn(b'PRIVATE_CANARY', h.wfile.getvalue())
                    elif site == 'handle':
                        with mock.patch.object(smoke.http.server.BaseHTTPRequestHandler, 'handle', side_effect=RuntimeError('PRIVATE_CANARY')):
                            h.handle()
                        self.assertTrue(h.close_connection)
                    elif site == 'do_POST':
                        h.do_POST(); self.assert_empty_rejection(h); category = 'unexpected_provider_endpoint'
                    else:
                        h.reject(); self.assert_empty_rejection(h, 404); category = 'unexpected_provider_endpoint'
                self.assertEqual(provider.error, category)
                self.assertEqual(self.projected(provider), {family: self.record(category, family)})

    def test_actual_parser_failure_has_no_unavailable_path_or_body_canary(self):
        class Socket:
            def __init__(self, data): self.data, self.sent = data, []
            def settimeout(self, *_): pass
            def makefile(self, *_): return io.BytesIO(self.data)
            def sendall(self, data): self.sent.append(data)
        for data in (b'PRIVATE_CANARY\r\n', b'POST /PRIVATE_CANARY HTTP/7.8\r\n\r\n',
                     b'POST /PRIVATE_CANARY HTTP/1.1\r\nX: '+b'a'*17000+b'\r\n\r\n'):
            provider = self.provider(); sock = Socket(data)
            smoke.make_handler(provider)(sock, ('127.0.0.1', 1), types.SimpleNamespace())
            records = self.projected(provider)
            self.assertEqual(list(records), ['other'])
            self.assertEqual(records['other']['stream'], 'unavailable')
            self.assertEqual(records['other']['model'], 'unavailable')
            self.assertNotIn('PRIVATE_CANARY', json.dumps(records))
            self.assertIsNotNone(provider.error)

    def test_concurrent_winners_are_bounded_stable_and_detached_from_caller_state(self):
        provider = self.provider(); barrier = threading.Barrier(13); errors = []
        bodies = [{'stream': bool(i % 2), 'model': smoke.MODEL if i % 2 else 'PRIVATE_CANARY'} for i in range(12)]
        paths = ['/claude/v1/messages', '/claude/v1/messages/count_tokens', '/PRIVATE_CANARY']
        def worker(i):
            try:
                barrier.wait(2)
                smoke.record_failure(provider, smoke.FixtureFailure(self.CATEGORIES[i]), paths[i % 3], bodies[i])
            except BaseException as exc: errors.append(exc)
        workers = [threading.Thread(target=worker, args=(i,)) for i in range(12)]
        for w in workers: w.start()
        barrier.wait(2)
        for w in workers: w.join(2)
        self.assertFalse(any(w.is_alive() for w in workers)); self.assertEqual(errors, [])
        retained = self.projected(provider)
        self.assertEqual(list(retained), list(self.FAMILIES)); self.assertEqual(len(retained), 3)
        for family, row in retained.items():
            self.assertEqual(set(row), {'category', 'endpoint_family', 'stream', 'model'})
            self.assertEqual(row['endpoint_family'], family)
            self.assertIn(row['category'], self.CATEGORIES)
            winner = self.CATEGORIES.index(row['category'])
            self.assertLess(winner, 12)
            self.assertEqual(self.FAMILIES[winner % 3], family)
            self.assertEqual(row['stream'], 'true' if winner % 2 else 'false')
            self.assertEqual(row['model'], 'fixture' if winner % 2 else 'other')
        for body in bodies: body.update(stream='PRIVATE_CANARY', model=[])
        for path in paths: smoke.record_failure(provider, RuntimeError('PRIVATE_CANARY'), path, {})
        self.assertEqual(self.projected(provider), retained)
        detached = self.projected(provider); detached['messages']['category'] = 'PRIVATE_CANARY'
        detached['PRIVATE_CANARY'] = {}
        self.assertEqual(self.projected(provider), retained)
        self.assertEqual(provider.error, 'provider_protocol_failed')

    def run_report(self, injected=None, terminal=False):
        with tempfile.TemporaryDirectory(prefix='tempo-provider-qa-') as tmp:
            fixture = Path(tmp).resolve(); root, home = fixture/'owned', fixture/'home'; home.mkdir()
            runtime, tempo, helper = (fixture/name for name in ('runtime', 'tempo', 'helper'))
            for path in (runtime, tempo, helper): path.write_text('inert file')
            model = smoke.Conversation(lambda: {'receipts': []}, fixture/'project')
            provider = smoke.Provider.__new__(smoke.Provider)
            provider.budget, provider.error, provider.shutdown = smoke.ProviderBudget(), None, threading.Event()
            provider.server, provider.thread = mock.Mock(server_port=43210), mock.Mock()
            def close_site():
                if injected is not None: provider.budget.first_rejections = copy.deepcopy(injected)
                if terminal: provider.error = 'provider_protocol_failed'
            provider.server.server_close.side_effect = close_site
            def bounded(argv, *_args, **_kwargs):
                if argv[0] == str(runtime):
                    return b'2.1.286 (Claude Code)\n' if argv[1:] == ['--version'] else b''
                if argv[2] == 'confirm': return json.dumps({'basis': 'operator_declared', 'fingerprint': 'a'*64}).encode()
                return b'{"receipts":[],"queued":0,"uncertainties":0,"capture_reviews":0}' if argv[2] == 'read' else b''
            def owned_root(**_kwargs): root.mkdir(); return str(root)
            real_home = os.environ['HOME']
            def boundary_path(value): return home if str(value) == real_home else Path(value)
            report = {}
            with mock.patch.dict(os.environ, {'RUNNER_TEMP': str(fixture), 'GITHUB_SHA': 'd'*40}), \
                 mock.patch.object(smoke, 'Path', side_effect=boundary_path), \
                 mock.patch.object(smoke, 'hosted_precondition'), mock.patch.object(smoke, 'require_absent'), \
                 mock.patch.object(smoke, 'child_environment', return_value={'TEMPO_STATE': str(root/'state'), 'TEMPO_HOOK_STATE': str(root/'policy')}), \
                 mock.patch.object(smoke.tempfile, 'mkdtemp', side_effect=owned_root), \
                 mock.patch.object(smoke, 'download_runtime', return_value=runtime), \
                 mock.patch.object(smoke, 'bounded_run', side_effect=bounded), \
                 mock.patch.object(smoke, 'Conversation', return_value=model), \
                 mock.patch.object(smoke, 'Provider', return_value=provider), mock.patch.object(smoke, 'require_final'):
                if terminal:
                    with self.assertRaisesRegex(smoke.FixtureFailure, '^provider_protocol_failed$'):
                        smoke.run(types.SimpleNamespace(tempo=str(tempo), helper=str(helper)), report)
                else: smoke.run(types.SimpleNamespace(tempo=str(tempo), helper=str(helper)), report)
            self.assertEqual(os.environ['HOME'], real_home); self.assertFalse(root.exists())
            provider.server.shutdown.assert_called_once(); provider.server.server_close.assert_called_once()
            provider.thread.join.assert_called_once(); self.assertTrue(provider.shutdown.is_set())
            self.assertEqual(report.get('status') == 'passed', not terminal)
            return report

    def test_actual_run_finally_omits_empty_records_and_retains_late_rejections_with_cleanup(self):
        self.assertNotIn('provider_first_rejections', self.run_report())
        injected = {family: self.record('provider_protocol_failed', family) for family in reversed(self.FAMILIES)}
        report = self.run_report(injected, terminal=True)
        self.assertEqual(report['provider_first_rejections'], injected)
        self.assertEqual(list(report['provider_first_rejections']), list(self.FAMILIES))

    def test_actual_run_finally_drops_malformed_records_and_unhashable_values_without_leaks(self):
        good = self.record('provider_request_contract', 'messages', 'false', 'other')
        faults = [None, [], 'PRIVATE_CANARY', {'PRIVATE_CANARY': good},
            {'messages': []}, {'messages': dict(good, endpoint_family='other')},
            {'messages': {k: v for k, v in good.items() if k != 'model'}}]
        for field in good:
            for invalid in ('PRIVATE_CANARY', None, 1, True, [], {}):
                faults.append({'messages': dict(good, **{field: invalid})})
        for fault in faults:
            with self.subTest(fault=fault):
                report = self.run_report(fault, terminal=True)
                self.assertNotIn('provider_first_rejections', report)
                self.assertNotIn('PRIVATE_CANARY', json.dumps(report))
        dirty = {'PRIVATE_CANARY': good, 'other': self.record('provider_protocol_failed', 'other'),
            'messages': dict(good, PRIVATE_CANARY='PRIVATE_CANARY'), 'count_tokens': {'stream': []}}
        report = self.run_report(dirty, terminal=True)
        self.assertEqual(report['provider_first_rejections'], {'messages': good,
            'other': self.record('provider_protocol_failed', 'other')})
        self.assertNotIn('PRIVATE_CANARY', json.dumps(report))


if __name__=='__main__':unittest.main()
