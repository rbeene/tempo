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

    def test_normal_print_argv_has_no_permission_grants(self):
        argv = smoke.claude_argv(Path('/tmp/claude'), Path('/tmp/project'))
        self.assertIn('--print', argv); self.assertIn('--no-session-persistence', argv)
        self.assertEqual(argv[argv.index('--setting-sources')+1], 'project,local')
        self.assertEqual(argv[argv.index('--tools')+1], 'Read,Agent')
        self.assertIn('--strict-mcp-config', argv)
        for forbidden in ('--bare', '--safe-mode', '--allowedTools', '--permission-mode', '--dangerously-skip-permissions'):
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
    def test_sse_has_complete_text_and_tool_lifecycle(self):
        for block in ({'type':'text','text':'done'}, {'type':'tool_use','id':'tempo-read','name':'Read','input':{'file_path':'/tmp/fixture'}}):
            events=smoke.sse_events(block)
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
               ([dict(good,is_error=True)], 'agent_tool_error_unclassified')]
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
                self.assertEqual(str(failure.exception),'agent_tool_error_'+category)
                self.assertNotIn('private-canary',str(failure.exception))
        for content in (None,{'error':'private-canary'},[{'type':'image','text':cases[0][0]}],
                        'private-canary'*2000,[{'type':'text','text':cases[0][0]}]*129):
            with self.assertRaises(smoke.FixtureFailure) as failure:
                smoke.require_tool_result(request(results=[dict(result('tempo-agent'),is_error=True,content=content)]),'tempo-agent')
            self.assertEqual(str(failure.exception),'agent_tool_error_unclassified')

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
        self.assertEqual(str(failure.exception),'agent_tool_error_unclassified')
        self.assertEqual(model.phase,'agent');self.assertEqual(model.counts,{'parent':2,'child':1})

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
        block,hold=model.respond(request());self.assertEqual(block['name'],'Read');self.assertFalse(hold)


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
        block,_=model.respond(request(results=[result('tempo-read')]))
        self.assertEqual(block['name'],'Agent')
        with self.assertRaises(smoke.FixtureFailure):model.respond(request('tempo-native-child-case'))
        rows.append(receipt('PreToolUse',tool='tempo-agent'))
        start=receipt('SubagentStart','native-child',turn='child-prompt');rows.append(start)
        snap['actors'].append({'ref':start['actor'],'state':'working','health':'continuous'})
        block,hold=model.respond(request('tempo-native-child-case'));self.assertTrue(hold)
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
            block,hold=model.respond(request(results=[result('tempo-agent','launched')]))
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
        for text,category in [('private-canary','agent_tool_error_unclassified'),
                              ('Permission to use Agent has been denied: private-canary','agent_tool_error_permission')]:
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

if __name__=='__main__':unittest.main()
