#!/usr/bin/env python3
# Copyright 2026 KafScale team.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("site_sync", ROOT / "scripts/sync_quickstart_site.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SiteSyncTest(unittest.TestCase):
    def test_onboarding_and_idempotence(self):
        with tempfile.TemporaryDirectory() as directory:
            site = pathlib.Path(directory)
            (site / "_docs").mkdir()
            (site / "index.md").write_text('<a class="button" href="/quickstart/">Get started</a>\n<a href="/quickstart/">Helm</a>')
            front = '---\nlayout: doc\ntitle: "Existing Helm title"\npermalink: /quickstart/\n---\n'
            (site / "_docs/quickstart.md").write_text(front + "old")
            MODULE.sync(ROOT, site)
            self.assertIn('href="/local-quickstart/">Get started', (site / "index.md").read_text())
            self.assertIn('href="/quickstart/">Helm', (site / "index.md").read_text())
            self.assertTrue((site / "_docs/quickstart.md").read_text().startswith(front))
            local = (site / "_docs/local-quickstart.md").read_text()
            self.assertIn('permalink: /local-quickstart/', local)
            self.assertIn('](/quickstart/)', local)
            self.assertIn('](/s3-connection/)', local)
            before = {str(p.relative_to(site)): p.read_bytes() for p in site.rglob('*.md')}
            MODULE.sync(ROOT, site)
            after = {str(p.relative_to(site)): p.read_bytes() for p in site.rglob('*.md')}
            self.assertEqual(before, after)

    def test_code_fences_and_relative_links(self):
        text = '[S3](s3-connection.md#native-cli)\n```md\n[S3](s3-connection.md)\n```\n[API](../api/README.md)\n'
        actual = MODULE.rewrite_links('docs/local-quickstart.md', text)
        self.assertIn('[S3](/s3-connection/#native-cli)', actual)
        self.assertIn('```md\n[S3](s3-connection.md)\n```', actual)
        self.assertIn('[API](https://github.com/KafScale/platform/blob/main/api/README.md)', actual)

    def test_symlink_destination_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            site = root / 'site'
            site.mkdir()
            outside = root / 'outside.md'
            outside.write_text('keep')
            (site / 'index.md').symlink_to(outside)
            with self.assertRaises(ValueError):
                MODULE.sync(ROOT, site)
            self.assertEqual('keep', outside.read_text())


if __name__ == '__main__':
    unittest.main()
