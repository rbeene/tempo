#!/usr/bin/env python3
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('assets', Path(__file__).with_name('check-release-assets.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class ReleaseAssets(unittest.TestCase):
    def fixture(self):
        return {'assets': [{'name': name, 'size': 42} for name in (
            'tempo_0.1.0_darwin_amd64.tar.gz', 'tempo_0.1.0_darwin_arm64.tar.gz',
            'tempo_0.1.0_linux_amd64.tar.gz', 'tempo_0.1.0_linux_arm64.tar.gz',
            'checksums-darwin.txt', 'checksums-linux.txt')]}

    def test_complete_release(self):
        module.validate('v0.1.0', self.fixture())

    def test_incomplete_duplicate_extra_wrong_version_and_empty_assets(self):
        for scenario in ('missing', 'duplicate', 'extra', 'version', 'empty', 'bad_size'):
            with self.subTest(scenario=scenario):
                data = self.fixture()
                if scenario == 'missing':
                    data['assets'].pop()
                elif scenario == 'duplicate':
                    data['assets'][-1] = data['assets'][0]
                elif scenario == 'extra':
                    data['assets'].append({'name': 'unexpected', 'size': 1})
                elif scenario == 'version':
                    data['assets'][0]['name'] = 'tempo_0.2.0_darwin_amd64.tar.gz'
                elif scenario == 'empty':
                    data['assets'][0]['size'] = 0
                else:
                    data['assets'][0]['size'] = '42'
                with self.assertRaises(ValueError):
                    module.validate('v0.1.0', data)

    def test_invalid_version(self):
        for tag in ('v01.1.0', 'v0.1.0\nv0.2.0', '0.1.0', 'v0.1.0-rc1'):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                module.validate(tag, self.fixture())

if __name__ == '__main__':
    unittest.main(verbosity=2)
