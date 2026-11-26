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

"""Exercise real Maven HTTP resolution against a local repository, without Central."""
import hashlib
import http.server
import os
import pathlib
import shutil
import subprocess
import tempfile
import threading
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
BOM = b'''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>fixture</groupId>
<artifactId>bom</artifactId><version>1</version><packaging>pom</packaging></project>'''


class MavenTransportTest(unittest.TestCase):
    def run_resolution(self, status, failures, retry_after=None, restore=False):
        with tempfile.TemporaryDirectory() as directory:
            work = pathlib.Path(directory)
            requests = []

            class Repository(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    if self.path.endswith('/bom-1.pom'):
                        requests.append(self.path)
                        if len(requests) <= failures:
                            self.send_response(status)
                            if retry_after is not None:
                                self.send_header('Retry-After', retry_after)
                            self.end_headers()
                            return
                        data = BOM
                    elif self.path.endswith('/bom-1.pom.sha1'):
                        data = hashlib.sha1(BOM).hexdigest().encode()
                    elif self.path.endswith('/bom-1.pom.md5'):
                        data = hashlib.md5(BOM).hexdigest().encode()
                    else:
                        self.send_error(404)
                        return
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                def log_message(self, *_args):
                    pass

            server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Repository)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = f'http://127.0.0.1:{server.server_port}/'
                settings = (ROOT / '.github/maven-settings.xml').read_text()
                self.assertIn('<mirrorOf>central</mirrorOf>', settings)
                (work / 'settings.xml').write_text(settings.replace(
                    'https://maven-central.storage-download.googleapis.com/maven2/', url))
                (work / 'pom.xml').write_text('''<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>fixture</groupId>
<artifactId>client</artifactId><version>1</version><packaging>pom</packaging>
<dependencyManagement><dependencies><dependency><groupId>fixture</groupId>
<artifactId>bom</artifactId><version>1</version><type>pom</type><scope>import</scope>
</dependency></dependencies></dependencyManagement></project>''')

                def invoke(repository):
                    # Accelerate the backoff in this fixture, retaining the production
                    # retry count and maximum Retry-After interval.
                    return subprocess.run([
                        'bash', str(ROOT / 'scripts/maven_ci.sh'),
                        f'-Dmaven.repo.local={repository}',
                        '-Daether.connector.http.retryHandler.interval=1',
                        '-Daether.transport.http.retryHandler.interval=1',
                        'validate',
                    ], cwd=work, env={**os.environ, 'MAVEN_SETTINGS': str(work / 'settings.xml')},
                        text=True, stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, timeout=30)

                result = invoke(work / 'repository')
                if restore:
                    self.assertEqual(result.returncode, 0, result.stdout)
                    before = len(requests)
                    shutil.copytree(work / 'repository', work / 'restored')
                    cached = invoke(work / 'restored')
                    self.assertEqual(cached.returncode, 0, cached.stdout)
                    self.assertEqual(len(requests), before, 'restored cache redownloaded the BOM')
                return result, len(requests)
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

    def test_rate_limit_recovers_and_restored_cache_avoids_downloads(self):
        result, count = self.run_resolution(429, 2, restore=True)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(count, 3)

    def test_transient_failures_stop_after_three_retries(self):
        for status in (429, 502, 503, 504):
            with self.subTest(status=status):
                result, count = self.run_resolution(status, 100)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertEqual(count, 4, result.stdout)

    def test_missing_artifacts_are_not_retried(self):
        result, count = self.run_resolution(404, 100)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(count, 1, result.stdout)

    def test_excessive_retry_after_fails_without_waiting(self):
        result, count = self.run_resolution(429, 100, retry_after='120')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(count, 1, result.stdout)


if __name__ == '__main__':
    unittest.main()
