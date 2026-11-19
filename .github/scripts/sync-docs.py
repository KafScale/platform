#!/usr/bin/env python3
# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
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
import pathlib
import posixpath
import re
import subprocess
import sys

REPO_URL = "https://github.com/KafScale/platform"

PAGES = {
    "architecture.md": ("architecture.md", "/architecture/"),
    "supported.md": ("supported.md", "/supported/"),
    "development.md": ("_docs/development.md", "/development/"),
    "kafka-authentication.md": ("_docs/kafka-authentication.md", "/kafka-authentication/"),
    "metrics.md": ("_docs/metrics.md", "/metrics/"),
    "operations.md": ("_docs/operations.md", "/operations/"),
    "ops-api.md": ("_docs/ops-api.md", "/ops-api/"),
    "ops-mcp.md": ("_docs/ops-mcp.md", "/ops-mcp/"),
    "overview.md": ("_docs/overview.md", "/overview/"),
    "protocol.md": ("_docs/protocol.md", "/protocol/"),
    "quickstart.md": ("_docs/quickstart.md", "/quickstart/"),
    "roadmap.md": ("_docs/roadmap.md", "/roadmap/"),
    "schema-registry.md": ("_docs/schema-registry.md", "/schema-registry/"),
    "security.md": ("_docs/security.md", "/security/"),
    "storage.md": ("_docs/storage.md", "/storage/"),
    "upgrade-compatibility.md": ("_docs/upgrade-compatibility.md", "/upgrade-compatibility/"),
    "user-guide.md": ("_docs/user-guide.md", "/user-guide/"),
    "benchmarks/README.md": ("_docs/benchmarks.md", "/benchmarks/"),
    "grafana/README.md": ("_docs/grafana.md", "/grafana/"),
}

ALIASES = {
    "mcp.md": "/ops-mcp/",
    "releases/": "/releases/",
    "releases": "/releases/",
}

SKIP = {
    "mcp.md": "covered by /ops-mcp/, which redirects /mcp/",
    "security-dependencies.md": "internal dependency and CI notes",
    "releases/unreleased.md": "unreleased notes are not published",
}

NEW_FRONT = {
    "schema-registry.md": {
        "title": "KafScale Schema Registry: Built-in Registry and Migration",
        "description": "Run the built-in Confluent-compatible schema registry, move schemas into it and configure compatibility.",
        "nav_title": "Schema Registry",
        "nav_order": "6",
    },
    "upgrade-compatibility.md": {
        "title": "KafScale Upgrade and Compatibility Guide",
        "description": "Compatibility boundaries and migration steps for upgrading KafScale.",
        "nav_title": "Upgrade guide",
        "nav_order": "8",
        "nav_group": "Releases",
    },
}

RELEASE_RE = re.compile(r"^releases/v(\d+\.\d+\.\d+)\.md$")
FENCE_RE = re.compile(r"^\s*(```|~~~)")
MD_LINK_RE = re.compile(r"(\]\()([^)\s]+)(\))")
CODE_DOC_RE = re.compile(r"(?<!\[)`(docs/[^`\s]+)`")


def git(repo, *args):
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True).stdout


def release_version(rel):
    m = RELEASE_RE.match(rel)
    return m.group(1) if m else None


def site_target(rel):
    if rel in PAGES:
        return PAGES[rel]
    v = release_version(rel)
    if v:
        return (f"_docs/release-v{v}.md", f"/releases/v{v}/")
    return None


def permalink_for(rel):
    if rel in ALIASES:
        return ALIASES[rel]
    t = site_target(rel)
    if t and rel not in SKIP:
        return t[1]
    return None


def resolve(src_rel, target):
    path, _, anchor = target.partition("#")
    if not path:
        return None
    if re.match(r"^[a-z]+:", path) or path.startswith("/"):
        return None
    base = posixpath.dirname(src_rel)
    full = posixpath.normpath(posixpath.join("docs", base, path))
    if path.endswith("/"):
        full += "/"
    if full.startswith("docs/"):
        rel = full[len("docs/"):]
        link = permalink_for(rel)
        if link:
            return link + ("#" + anchor if anchor else "")
    kind = "tree" if full.endswith("/") or "." not in posixpath.basename(full) else "blob"
    return f"{REPO_URL}/{kind}/main/{full.rstrip('/')}" + ("#" + anchor if anchor else "")


def rewrite(src_rel, text):
    out = []
    in_fence = False
    for line in text.split("\n"):
        if FENCE_RE.match(line):
            in_fence = not in_fence
            out.append(line)
            continue
        if in_fence:
            out.append(line)
            continue

        def md(m):
            new = resolve(src_rel, m.group(2))
            return m.group(1) + (new or m.group(2)) + m.group(3)

        def code(m):
            ref = m.group(1)
            path, _, anchor = ref.partition("#")
            rel = path[len("docs/"):]
            link = permalink_for(rel)
            if not link:
                kind = "tree" if path.endswith("/") else "blob"
                link = f"{REPO_URL}/{kind}/main/{path.rstrip('/')}"
            elif anchor:
                link += "#" + anchor
            return f"[`{ref}`]({link})"

        line = MD_LINK_RE.sub(md, line)
        line = CODE_DOC_RE.sub(code, line)
        out.append(line)
    return "\n".join(out)


def split_front(text):
    if text.startswith("---\n"):
        end = text.index("\n---\n", 4)
        return text[: end + 5]
    return None


def new_front(rel, link):
    if rel in NEW_FRONT:
        meta = dict(NEW_FRONT[rel])
    else:
        v = release_version(rel)
        if not v:
            return None
        meta = {
            "title": f"KafScale Release v{v}",
            "description": f"Release notes for KafScale v{v}.",
            "nav_title": f"v{v}",
            "nav_order": "99",
            "nav_group": "Releases",
        }
    lines = ["---", "layout: doc"]
    lines.append(f'title: "{meta["title"]}"')
    lines.append(f'description: "{meta["description"]}"')
    lines.append(f"permalink: {link}")
    for key in ("nav_title", "nav_order", "nav_group"):
        if key in meta:
            lines.append(f"{key}: {meta[key]}")
    lines.append("---")
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description="Copy KafScale repo docs into the gh-pages site.")
    ap.add_argument("--repo", required=True, help="path to a KafScale/platform clone")
    ap.add_argument("--ref", default="main", help="git ref to read docs/ from")
    ap.add_argument("--site", default=".", help="path to the gh-pages checkout")
    args = ap.parse_args()

    site = pathlib.Path(args.site)
    files = [f for f in git(args.repo, "ls-tree", "-r", "--name-only", args.ref, "docs").split("\n") if f.endswith(".md")]
    rels = sorted(f[len("docs/"):] for f in files)
    warnings = []
    for rel in rels:
        if rel in SKIP:
            print(f"skip {rel}: {SKIP[rel]}")
            continue
        target = site_target(rel)
        if not target:
            warnings.append(f"no site page mapped for docs/{rel}")
            continue
        dest = site / target[0]
        body = git(args.repo, "show", f"{args.ref}:docs/{rel}")
        front = split_front(dest.read_text()) if dest.exists() else None
        if front is None:
            front = new_front(rel, target[1])
            if release_version(rel):
                warnings.append(f"new release page {target[0]}: set nav_order and add it to _docs/releases.md")
        if front is None:
            warnings.append(f"no front matter for {target[0]}")
            continue
        text = rewrite(rel, body).rstrip("\n") + "\n"
        extra = pathlib.Path(__file__).parent / "site-extras" / pathlib.Path(target[0]).name
        if extra.exists():
            text += "\n" + extra.read_text().rstrip("\n") + "\n"
        dest.write_text(front + "\n" + text)
        print(f"wrote {target[0]} from docs/{rel}")
    for w in warnings:
        print(f"WARNING: {w}", file=sys.stderr)


if __name__ == "__main__":
    main()
