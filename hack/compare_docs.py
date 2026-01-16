#!/usr/bin/env python3
import argparse
import difflib
import json
import os
import re
import subprocess
import sys


def run_git(args):
    try:
        return subprocess.check_output(["git"] + args, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as exc:
        return None


def git_ls_tree(ref, path=None, recursive=False):
    cmd = ["ls-tree"]
    if recursive:
        cmd.append("-r")
    cmd.extend(["--name-only", ref])
    if path:
        cmd.append(path)
    out = run_git(cmd)
    if out is None:
        return []
    return [line for line in out.splitlines() if line.strip()]


def git_show(ref, path):
    out = run_git(["show", f"{ref}:{path}"])
    return out


def strip_front_matter(text):
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end != -1:
            return text[end + 5 :]
    return text


def strip_html_comment_header(text):
    return re.sub(r"\A\s*<!--.*?-->\s*", "", text, flags=re.DOTALL)


def normalize(text):
    text = text.replace("\r\n", "\n")
    text = strip_html_comment_header(text)
    text = strip_front_matter(text)
    return text.strip() + "\n"


def load_mapping(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict) and "mappings" in data:
        return data["mappings"] or []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [{"main": k, "gh": v} for k, v in data.items()]
    return []


def mapping_lookup(mappings, main_doc):
    basename = os.path.basename(main_doc)
    for item in mappings:
        if item.get("main") == main_doc or item.get("main") == basename:
            return item
    return None


def diff_stats(a_lines, b_lines):
    diff = list(difflib.unified_diff(a_lines, b_lines, lineterm=""))
    change_lines = [
        line
        for line in diff
        if (line.startswith("+") or line.startswith("-"))
        and not (line.startswith("+++") or line.startswith("---"))
    ]
    return diff, len(change_lines)


def extract_title(text):
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return ""


def extract_headings(text):
    headings = []
    in_code = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        match = re.match(r"^(#{1,6})\s+(.*)$", line)
        if match:
            headings.append(match.group(2).strip())
    return headings


def similarity(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()


def suggest_matches(main_title, gh_titles, limit=3):
    scored = []
    for gh_doc, gh_title in gh_titles.items():
        score = similarity(main_title.lower(), gh_title.lower())
        scored.append((score, gh_doc, gh_title))
    scored.sort(reverse=True)
    return scored[:limit]


def main():
    parser = argparse.ArgumentParser(
        description="Compare docs in main with gh-pages."
    )
    parser.add_argument(
        "--show-diff",
        action="store_true",
        help="Print unified diffs for changed files.",
    )
    parser.add_argument(
        "--max-diff-lines",
        type=int,
        default=120,
        help="Max diff lines to print per file (default: 120).",
    )
    parser.add_argument(
        "--mapping",
        default="hack/docs_sync_map.json",
        help="JSON mapping file (default: hack/docs_sync_map.json).",
    )
    parser.add_argument(
        "--no-normalize",
        action="store_true",
        help="Do not strip front matter or license headers.",
    )
    args = parser.parse_args()

    main_docs = [
        f
        for f in git_ls_tree("main", "docs", recursive=True)
        if f.endswith(".md") and f.count("/") == 1
    ]
    gh_docs = [
        f
        for f in git_ls_tree("gh-pages", recursive=True)
        if f.endswith(".md") and f.count("/") == 0
    ]

    mapping = load_mapping(args.mapping)
    pairs = []
    used_gh = set()
    gh_set = set(gh_docs)
    missing = []
    gh_titles = {}
    for gh_doc in gh_docs:
        gh_text = git_show("gh-pages", gh_doc) or ""
        gh_text = normalize(gh_text)
        title = extract_title(gh_text)
        if not title:
            title = gh_doc
        gh_titles[gh_doc] = title

    gh_by_title = {v: k for k, v in gh_titles.items()}

    for main_doc in sorted(main_docs):
        basename = os.path.basename(main_doc)
        map_item = mapping_lookup(mapping, main_doc)
        target = None
        label = None
        explicit_gh = False
        if map_item:
            target = map_item.get("gh") or map_item.get("target")
            label = map_item.get("label")
            explicit_gh = "gh" in map_item
            if explicit_gh and not map_item.get("gh"):
                target = None
        if not target:
            if explicit_gh:
                target = None
            else:
                target = basename
        pairs.append((main_doc, target, label))
        if target:
            used_gh.add(target)
            if target not in gh_set:
                missing.append(f"{target} (from {main_doc})")
        else:
            missing.append(f"(no page yet) (from {main_doc})")

    gh_only = sorted([f for f in gh_docs if f not in used_gh])

    print(f"Main docs: {len(main_docs)}")
    print(f"gh-pages docs: {len(gh_docs)}")
    if missing:
        print("\nMissing in gh-pages:")
        for item in missing:
            print(f"  - {item}")

    if gh_only:
        print("\nOnly in gh-pages (no main match):")
        for item in gh_only:
            print(f"  - {item}")

    print("\nMapping:")
    for main_doc, gh_doc, label in pairs:
        label_text = f"{label}: " if label else ""
        target_text = gh_doc if gh_doc else "(no page yet)"
        print(f"  - {label_text}{main_doc} -> {target_text}")

    print("\nComparisons:")
    changed = 0
    summary = []
    for main_doc, gh_doc, label in pairs:
        main_text = git_show("main", main_doc)
        gh_text = git_show("gh-pages", gh_doc) if gh_doc else None
        if gh_text is None:
            main_text_fallback = main_text or ""
            main_text_fallback = normalize(main_text_fallback)
            main_title = extract_title(main_text_fallback)
            label_text = f"{label}: " if label else ""
            target_text = gh_doc if gh_doc else "(no page yet)"
            print(f"  MISSING: {label_text}{main_doc} -> {target_text}")
            if main_title:
                suggestions = suggest_matches(main_title, gh_titles, limit=3)
                if suggestions:
                    hint = ", ".join(
                        f"{doc} ('{title}')" for _, doc, title in suggestions
                    )
                    print(f"    Suggested matches: {hint}")
            summary.append(
                {
                    "name": label or os.path.splitext(os.path.basename(main_doc))[0],
                    "status": "missing",
                    "details": "missing in gh-pages" if gh_doc else "no gh page yet",
                }
            )
            continue
        if main_text is None:
            print(f"  MISSING: {main_doc} (main)")
            summary.append(
                {
                    "name": label or os.path.splitext(os.path.basename(main_doc))[0],
                    "status": "missing",
                    "details": "missing in main",
                }
            )
            continue

        if not args.no_normalize:
            main_text = normalize(main_text)
            gh_text = normalize(gh_text)
        else:
            main_text = main_text.replace("\r\n", "\n")
            gh_text = gh_text.replace("\r\n", "\n")

        main_lines = main_text.splitlines(keepends=True)
        gh_lines = gh_text.splitlines(keepends=True)
        diff, diff_count = diff_stats(main_lines, gh_lines)
        main_title = extract_title(main_text)
        gh_title = extract_title(gh_text)
        label_text = f"{label}: " if label else ""

        if diff_count == 0:
            print(f"  OK: {label_text}{main_doc} -> {gh_doc}")
            summary.append(
                {
                    "name": label or os.path.splitext(os.path.basename(main_doc))[0],
                    "status": "ok",
                    "details": "content matches",
                }
            )
            continue

        changed += 1
        main_headings = set(extract_headings(main_text))
        gh_headings = set(extract_headings(gh_text))
        missing_headings = sorted(main_headings - gh_headings)
        extra_headings = sorted(gh_headings - main_headings)
        missing_slice = ", ".join(missing_headings[:6])
        extra_slice = ", ".join(extra_headings[:6])

        print(
            f"  DIFF: {label_text}{main_doc} -> {gh_doc} "
            f"({diff_count} changed lines, main {len(main_lines)} lines, gh {len(gh_lines)} lines)"
        )
        if missing_headings:
            print(f"    Missing headings in gh-pages ({len(missing_headings)}): {missing_slice}")
        if extra_headings:
            print(f"    Extra headings in gh-pages ({len(extra_headings)}): {extra_slice}")
        if main_title or gh_title:
            print(f"    Titles: main='{main_title}' gh='{gh_title}'")

        summary.append(
            {
                "name": label or os.path.splitext(os.path.basename(main_doc))[0],
                "status": "differs",
                "details": f"{diff_count} line changes",
            }
        )

        if args.show_diff:
            for line in diff[: args.max_diff_lines]:
                sys.stdout.write(line + ("\n" if not line.endswith("\n") else ""))
            if len(diff) > args.max_diff_lines:
                print(f"... diff truncated ({len(diff)} total lines)")

    print(f"\nChanged pairs: {changed}")

    print("\nSummary (main -> gh-pages):")
    for item in summary:
        print(f"  - {item['name']} => {item['status']} ({item['details']})")


if __name__ == "__main__":
    main()
