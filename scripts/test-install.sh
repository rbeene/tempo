#!/bin/sh
# Independent installer acceptance tests: all downloads and destinations are local.
set -eu
TEMPO_INSTALLER_TEST_SCRIPT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/install.sh
export TEMPO_INSTALLER_TEST_SCRIPT
exec python3 - <<'PY'
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

INSTALLER = Path(os.environ['TEMPO_INSTALLER_TEST_SCRIPT'])
VERSION = 'v1.2.3'
BASE = 'https://github.com/rbeene/tempo/releases/download/v1.2.3/'

class InstallerAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='tempo-installer-qa-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fake = self.root / 'fake commands'
        self.assets = self.root / 'assets'
        self.home = self.root / 'home with spaces'
        self.bin = self.root / 'bin with spaces'
        for p in [self.fake, self.assets, self.home, self.bin, self.root/'tmp']:
            p.mkdir()
        self.log = self.root / 'requests.jsonl'
        self.payload = b'#!/bin/sh\necho executed > "$HOME/unexpected-execution"\nexit 91\n'
        self.env = dict(os.environ, HOME=str(self.home), TMPDIR=str(self.root/'tmp'),
                        PATH=str(self.fake)+os.pathsep+os.environ['PATH'],
                        QA_ASSETS=str(self.assets), QA_LOG=str(self.log),
                        QA_OS='Darwin', QA_ARCH='arm64')
        self.command('uname', '#!/bin/sh\ncase "$1" in -s) echo "$QA_OS";; -m) echo "$QA_ARCH";; *) exit 92;; esac\n')
        self.command('curl', '''#!/usr/bin/env python3
import json,os,pathlib,sys
a=sys.argv[1:]
with open(os.environ['QA_LOG'],'a') as f:f.write(json.dumps(a)+'\\n')
def value(k):
    try:return a[a.index(k)+1]
    except (ValueError,IndexError):return None
if value('--proto')!='=https' or value('--proto-redir')!='=https' or '-k' in a or '--insecure' in a:
    sys.stderr.write('QA: insecure curl protocol options\\n');sys.exit(93)
urls=[s for s in a if s.startswith('https://') or s.startswith('http://')]
if len(urls)!=1 or not urls[0].startswith('https://github.com/rbeene/tempo/releases/download/v1.2.3/'):
    sys.stderr.write('QA: unexpected URL\\n');sys.exit(94)
out=value('--output') or value('-o')
if not out:sys.stderr.write('QA: download must name an output path\\n');sys.exit(95)
src=pathlib.Path(os.environ['QA_ASSETS'])/urls[0].rsplit('/',1)[-1]
if not src.exists():sys.exit(22)
pathlib.Path(out).write_bytes(src.read_bytes())
''')
        self.command('sudo', '#!/bin/sh\necho forbidden-sudo >&2\nexit 96\n')
        self.make_assets()

    def command(self, name, text):
        p=self.fake/name;p.write_text(text);p.chmod(0o755)

    def make_assets(self, system='darwin', arch='arm64', members=None, checksum='valid'):
        name=f'tempo_1.2.3_{system}_{arch}.tar.gz'
        archive=self.assets/name
        with tarfile.open(archive,'w:gz') as tar:
            if members is None: members=[('tempo','file')]
            for member,kind in members:
                info=tarfile.TarInfo(member);info.mode=0o755
                if kind=='symlink':info.type=tarfile.SYMTYPE;info.linkname='../../outside'
                elif kind=='hardlink':info.type=tarfile.LNKTYPE;info.linkname='../../outside'
                elif kind=='dir':info.type=tarfile.DIRTYPE
                else:info.size=len(self.payload)
                tar.addfile(info,io.BytesIO(self.payload) if kind=='file' else None)
        digest=hashlib.sha256(archive.read_bytes()).hexdigest()
        line=f'{digest}  {name}\n'
        if checksum=='corrupt':line='0'*64+'  '+name+'\n'
        elif checksum=='missing':line=f'{digest}  different.tar.gz\n'
        elif checksum=='duplicate':line*=2
        elif checksum=='conflicting':line+='0'*64+'  '+name+'\n'
        (self.assets/f'checksums-{system}.txt').write_text(line)

    def run_installer(self, args=None):
        if args is None:args=['--version',VERSION,'--bin-dir',str(self.bin)]
        return subprocess.run(['sh',str(INSTALLER),*args],env=self.env,capture_output=True,text=True,timeout=10)

    def assert_no_execution(self):
        self.assertFalse((self.home/'unexpected-execution').exists(),'installer executed downloaded binary')

    def assert_rejected_preserves_old(self):
        old=self.bin/'tempo';old.write_bytes(b'old trusted binary');old.chmod(0o755)
        p=self.run_installer()
        self.assertNotEqual(p.returncode,0,p.stdout+p.stderr)
        self.assertTrue(self.log.exists(),'rejection never exercised downloaded fixture')
        self.assertEqual(old.read_bytes(),b'old trusted binary','failed install replaced existing binary')
        self.assert_no_execution()

    def test_success_platform_mapping_and_space_path(self):
        for system,wire in [('Darwin','darwin'),('Linux','linux')]:
            for arch,mapped in [('arm64','arm64'),('aarch64','arm64'),('x86_64','amd64'),('amd64','amd64')]:
                with self.subTest(system=system,arch=arch):
                    self.env.update(QA_OS=system,QA_ARCH=arch)
                    self.make_assets(wire,mapped)
                    self.log.unlink(missing_ok=True)
                    p=self.run_installer()
                    self.assertEqual(p.returncode,0,p.stdout+p.stderr)
                    binary=self.bin/'tempo'
                    self.assertEqual(binary.read_bytes(),self.payload)
                    self.assertTrue(os.access(binary,os.X_OK))
                    self.assert_no_execution()
                    requests=[json.loads(s) for s in self.log.read_text().splitlines()]
                    urls=[v for a in requests for v in a if v.startswith('https://')]
                    self.assertCountEqual(urls,[BASE+f'tempo_1.2.3_{wire}_{mapped}.tar.gz',BASE+f'checksums-{wire}.txt'])

    def test_default_bin_directory(self):
        p=self.run_installer(['--version',VERSION])
        self.assertEqual(p.returncode,0,p.stdout+p.stderr)
        self.assertEqual((self.home/'.local/bin/tempo').read_bytes(),self.payload)
        self.assert_no_execution()

    def test_release_archive_allows_documented_readme_and_commands(self):
        self.make_assets(members=[('tempo','file'),('README.md','file'),('docs','dir'),('docs/commands.md','file')])
        p=self.run_installer()
        self.assertEqual(p.returncode,0,p.stdout+p.stderr)
        self.assertEqual((self.bin/'tempo').read_bytes(),self.payload)
        self.assertEqual(sorted(p.name for p in self.bin.iterdir()),['tempo'])
        self.assert_no_execution()

    def test_checksum_failures_preserve_existing_binary(self):
        for checksum in ['corrupt','missing','duplicate','conflicting']:
            with self.subTest(checksum=checksum):
                self.make_assets(checksum=checksum)
                self.assert_rejected_preserves_old()

    def test_archive_requires_regular_tempo_and_rejects_unknown_members(self):
        for members in [[('tempo','symlink')],[('tempo','hardlink')],[('tempo','dir')],
                        [('tempo','file'),('extra','file')],[('tempo','file'),('tempo','file')],
                        [('../outside','file')],[(str(self.root/'absolute-escape'),'file')],
                        [('folder/tempo','file')]]:
            with self.subTest(members=members):
                self.make_assets(members=members)
                self.assert_rejected_preserves_old()
                self.assertFalse((self.root/'outside').exists())
                self.assertFalse((self.root/'absolute-escape').exists())

    def test_invalid_version_or_options_rejected_without_download(self):
        for args in [[],['--version'],['--version','1.2.3'],['--version','v1.2'],
                     ['--version','v1.2.3;touch INJECTION'],['--version','v1.2.3/../evil'],
                     ['--version','v1.2.3\nevil'],['--version','evil\nv1.2.3'],
                     ['--version','v1.2.3-rc1'],['--version','v01.2.3'],
                     ['--version',VERSION,'--unexpected'],['--version',VERSION,'--bin-dir']]:
            with self.subTest(args=args):
                self.log.unlink(missing_ok=True)
                p=self.run_installer(args)
                self.assertNotEqual(p.returncode,0,p.stdout+p.stderr)
                self.assertFalse(self.log.exists(),'invalid arguments started download')

    def test_unsupported_platform_rejected_without_download(self):
        for system,arch in [('FreeBSD','arm64'),('Darwin','i386'),('Linux','riscv64')]:
            with self.subTest(system=system,arch=arch):
                self.env.update(QA_OS=system,QA_ARCH=arch)
                self.log.unlink(missing_ok=True)
                p=self.run_installer()
                self.assertNotEqual(p.returncode,0,p.stdout+p.stderr)
                self.assertFalse(self.log.exists(),'unsupported platform started download')

    def test_release_archive_with_notices_installs_only_binary_after_validation(self):
        import shutil
        members=[('tempo','file'),('README.md','file'),
                 ('THIRD_PARTY_NOTICES.md','file'),('docs/commands.md','file')]
        self.make_assets(members=members)
        archive=self.assets/'tempo_1.2.3_darwin_arm64.tar.gz'
        with tarfile.open(archive) as tar:
            self.assertEqual(tar.getnames(),[name for name,_ in members])
            self.assertTrue(all(member.isfile() for member in tar))
        checksum=(self.assets/'checksums-darwin.txt').read_text().split()[0]
        self.assertEqual(checksum,hashlib.sha256(archive.read_bytes()).hexdigest())
        old=self.bin/'tempo';old.write_bytes(b'old trusted binary');old.chmod(0o755)
        self.env.update(QA_REAL_TAR=shutil.which('tar',path=os.environ['PATH']),
                        QA_OLD_BINARY=str(old),QA_TAR_LOG=str(self.root/'tar.jsonl'))
        self.command('tar', '''#!/usr/bin/env python3
import json,os,pathlib,sys
a=sys.argv[1:]
if pathlib.Path(os.environ['QA_OLD_BINARY']).read_bytes()!=b'old trusted binary':
    sys.stderr.write('QA: replaced old binary before archive validation completed\\n');sys.exit(97)
with open(os.environ['QA_TAR_LOG'],'a') as f:f.write(json.dumps(a)+'\\n')
if any('x' in arg for arg in a if arg.startswith('-')) and (len(a)!=3 or a[0]!='-xOzf' or a[2]!='tempo'):
    sys.stderr.write('QA: extraction must stream only tempo\\n');sys.exit(98)
os.execv(os.environ['QA_REAL_TAR'],[os.environ['QA_REAL_TAR'],*a])
''')
        p=self.run_installer()
        self.assert_no_execution()
        if p.returncode:
            self.assertEqual(old.read_bytes(),b'old trusted binary','rejected release replaced existing binary')
        self.assertEqual(p.returncode,0,p.stdout+p.stderr)
        self.assertEqual(old.read_bytes(),self.payload)
        self.assertTrue(os.access(old,os.X_OK))
        self.assertEqual(sorted(path.name for path in self.bin.iterdir()),['tempo'])
        self.assertEqual(list((self.root/'tmp').iterdir()),[],'installer left downloaded or extracted files')
        self.assertEqual(sorted(path.name for path in self.home.iterdir()),[])
        tar_calls=[json.loads(line) for line in (self.root/'tar.jsonl').read_text().splitlines()]
        self.assertEqual([args[0] for args in tar_calls],['-tzf','-tvzf','-xOzf'])
        self.assertEqual([args[-1] for args in tar_calls[1:]],['tempo','tempo'])
        self.assert_no_execution()

    def test_notice_lookalikes_and_nested_paths_are_rejected(self):
        for notice in ['THIRD_PARTY_NOTICESXmd','third_party_notices.md',
                       'THIRD_PARTY_NOTICES.md.bak','THIRD_PARTY_NOTICES.md/child',
                       './THIRD_PARTY_NOTICES.md','docs/THIRD_PARTY_NOTICES.md',
                       'folder/THIRD_PARTY_NOTICES.md']:
            with self.subTest(notice=notice):
                self.make_assets(members=[('tempo','file'),('README.md','file'),
                                          ('docs/commands.md','file'),(notice,'file')])
                self.assert_rejected_preserves_old()
                self.assertEqual(sorted(path.name for path in self.bin.iterdir()),['tempo'])

    def test_release_archive_with_notices_preserves_checksum_guards(self):
        for checksum in ['corrupt','missing','duplicate','conflicting']:
            with self.subTest(checksum=checksum):
                self.make_assets(members=[('tempo','file'),('README.md','file'),
                                          ('THIRD_PARTY_NOTICES.md','file'),('docs/commands.md','file')],
                                 checksum=checksum)
                self.assert_rejected_preserves_old()

    def test_release_archive_with_notices_requires_one_regular_tempo(self):
        for executable in [[('tempo','symlink')],[('tempo','hardlink')],[('tempo','dir')],
                           [('tempo','file'),('tempo','file')],[],[('folder/tempo','file')]]:
            with self.subTest(executable=executable):
                self.make_assets(members=executable+[('README.md','file'),
                                                     ('THIRD_PARTY_NOTICES.md','file'),('docs/commands.md','file')])
                self.assert_rejected_preserves_old()
                self.assertFalse((self.root/'outside').exists())

if __name__=='__main__':unittest.main(verbosity=2)
PY
