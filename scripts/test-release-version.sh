#!/bin/sh
# Independent release-version acceptance tests; mutate temporary Git repos only.
set -eu
TEMPO_RELEASE_VERSION_SCRIPT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/release-version.sh
export TEMPO_RELEASE_VERSION_SCRIPT
exec python3 - <<'PY'
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(os.environ['TEMPO_RELEASE_VERSION_SCRIPT'])

class ReleaseVersionAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='tempo-release-version-qa-')
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name) / 'repo with spaces'
        self.repo.mkdir()
        self.env = dict(os.environ, GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_AUTHOR_NAME='Tempo QA', GIT_AUTHOR_EMAIL='qa@example.invalid',
                        GIT_COMMITTER_NAME='Tempo QA', GIT_COMMITTER_EMAIL='qa@example.invalid')
        for key in ['GIT_DIR','GIT_WORK_TREE','GIT_INDEX_FILE','GIT_CONFIG_COUNT']:
            self.env.pop(key,None)
        self.git('init','--quiet')
        self.git('config','core.hooksPath',os.devnull)
        self.commit()

    def git(self,*args):
        return subprocess.check_output(['git',*args],cwd=self.repo,env=self.env,text=True,stderr=subprocess.PIPE).strip()

    def commit(self):
        self.git('commit','--quiet','--allow-empty','--no-gpg-sign','-m','synthetic QA commit')

    def tag(self,name,annotated=False):
        if annotated:
            self.git('-c','tag.gpgSign=false','tag','-a',name,'-m','synthetic QA tag')
        else:
            self.git('-c','tag.gpgSign=false','tag',name)

    def snapshot(self):
        return (self.git('rev-parse','HEAD'), self.git('for-each-ref','--format=%(refname) %(objectname)'),
                self.git('status','--porcelain'))

    def run_script(self):
        before=self.snapshot()
        result=subprocess.run(['sh',str(SCRIPT)],cwd=self.repo,env=self.env,text=True,capture_output=True,timeout=5)
        self.assertEqual(self.snapshot(),before,'version calculation mutated Git state')
        return result

    def expect(self,version):
        p=self.run_script()
        self.assertEqual(p.returncode,0,p.stdout+p.stderr)
        self.assertEqual(p.stdout,version+'\n','stdout must be exactly one canonical tag')

    def test_first_release_without_tags(self):
        self.expect('v0.1.0')

    def test_first_release_ignores_malformed_and_prerelease_tags(self):
        for name in ['1.2.3','v01.2.3','v1.02.3','v1.2.03','v1.2','v1.2.3.4',
                     'v9.9.9-rc1','v9.9.9+build','release-v9.9.9','V9.9.9']:
            self.tag(name)
        self.expect('v0.1.0')

    def test_highest_version_uses_numeric_order_and_increments_patch(self):
        for name in ['v1.99.999','v2.9.99','v2.10.9','v2.10.10','v2.10.2','v2.2.99']:
            self.tag(name)
        self.commit()
        self.expect('v2.10.11')

    def test_major_and_patch_ordering(self):
        for name in ['v2.999.999','v3.0.9','v3.0.99']:
            self.tag(name)
        self.commit()
        self.expect('v3.0.100')

    def test_same_commit_retry_reuses_lightweight_tag(self):
        self.tag('v1.2.3')
        self.expect('v1.2.3')
        self.expect('v1.2.3')

    def test_same_commit_retry_reuses_annotated_tag(self):
        self.tag('v1.2.3',annotated=True)
        self.expect('v1.2.3')

    def test_head_tag_wins_over_higher_tag_on_another_commit(self):
        self.tag('v9.0.0')
        self.commit()
        self.tag('v1.2.3')
        self.tag('v99.0.0-preview')
        self.expect('v1.2.3')

    def test_ignored_head_tags_do_not_prevent_increment(self):
        self.tag('v1.2.3',annotated=True)
        self.commit()
        for name in ['v9.9.9-rc1','v09.9.9','v1.2.4+meta']:
            self.tag(name)
        self.expect('v1.2.4')

    def test_multiple_stable_tags_at_head_fail_without_version_output(self):
        self.tag('v1.2.3')
        self.tag('v1.2.4',annotated=True)
        p=self.run_script()
        self.assertNotEqual(p.returncode,0,'ambiguous same-commit release was accepted')
        self.assertEqual(p.stdout,'','ambiguous version leaked a publishable tag on stdout')
        self.assertTrue(p.stderr.strip(),'ambiguous tag failure needs a diagnostic')

if __name__=='__main__':unittest.main(verbosity=2)
PY
