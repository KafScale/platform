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

import argparse
import json
import pathlib
import posixpath
import re
import urllib.parse

PAGES = {
    "docs/local-quickstart.md": ("local-quickstart", "Try KafScale locally", 1),
    "docs/quickstart.md": ("quickstart", "Kubernetes and Helm quick start", 2),
    "docs/s3-connection.md": ("s3-connection", "Connect your S3", 3),
    "docs/cli-install.md": ("cli-install", "Install the native CLI", 4),
    "docs/operations.md": ("operations", "Operations", 5),
    "docs/schema-registry.md": ("schema-registry", "Schema registry", 6),
}
LINK = re.compile(r"(\]\()([^\s)]+)(\))")


def rewrite_links(source, text):
    def replace(match):
        target = urllib.parse.urlsplit(match[2])
        if target.scheme or target.netloc or not target.path or target.path.startswith("/"):
            return match[0]
        path = posixpath.normpath(posixpath.join(posixpath.dirname(source), target.path))
        if path in PAGES:
            url = "/" + PAGES[path][0] + "/"
        else:
            url = "https://github.com/KafScale/platform/blob/main/" + path
        if target.query:
            url += "?" + target.query
        if target.fragment:
            url += "#" + target.fragment
        return match[1] + url + match[3]

    result, fence = [], None
    for line in text.splitlines(keepends=True):
        marker = re.match(r"^\s*(`{3,}|~{3,})", line)
        if marker:
            if fence is None:
                fence = marker[1][0]
            elif marker[1][0] == fence:
                fence = None
            result.append(line)
        else:
            result.append(line if fence else LINK.sub(replace, line))
    return "".join(result)


def checked_destination(site, path):
    destination = site / path
    for item in (destination, *destination.parents):
        if item.is_symlink():
            raise ValueError(f"site path is a symlink: {item}")
        if item == site:
            break
    return destination


def sync(source, site):
    source, site = pathlib.Path(source).absolute(), pathlib.Path(site).absolute()
    index = checked_destination(site, "index.md")
    old = '<a class="button" href="/quickstart/">Get started</a>'
    new = '<a class="button" href="/local-quickstart/">Get started</a>'
    homepage = index.read_text()
    if old not in homepage and new not in homepage:
        raise ValueError("homepage Get started link was not found")
    destinations = {rel: checked_destination(site, "_docs/" + info[0] + ".md")
                    for rel, info in PAGES.items()}
    for rel, (slug, title, order) in PAGES.items():
        destination = destinations[rel]
        front = ("---\nlayout: doc\ntitle: " + json.dumps(title) + "\npermalink: /" + slug
                 + "/\nnav_title: " + json.dumps(title) + "\nnav_order: " + str(order) + "\n---\n")
        if destination.exists():
            previous = destination.read_text()
            if previous.startswith("---\n"):
                end = previous.find("\n---\n", 4)
                if end == -1:
                    raise ValueError(f"invalid front matter: {destination}")
                front = previous[:end + 5]
        destination.parent.mkdir(parents=True, exist_ok=True)
        body = rewrite_links(rel, (source / rel).read_text())
        destination.write_text(front + "\n" + body.rstrip("\n") + "\n")
    index.write_text(homepage.replace(old, new))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish the local, S3, CLI and Helm onboarding pages.")
    parser.add_argument("--source", required=True)
    parser.add_argument("--site", required=True)
    args = parser.parse_args()
    sync(args.source, args.site)
