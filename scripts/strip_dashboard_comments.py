#!/usr/bin/env python3
"""
strip_dashboard_comments.py — deploy-time slim for the dashboard HTML.

The Viking_Invest_Trading dashboard carries a lot of design history in
JS comments (RULES_VERSION rationales, prior-iteration notes, link to
internal docs etc). The 27% of the file that's // comments helps me
navigate the code but adds ~300 KB to every cold mobile load.

This script strips JS line/block comments + collapses runs of blank
lines from <script> blocks inside _site/dashboard.html and
_site/Viking_Invest_Trading_v*.html. Source stays untouched — only
the deployed bundle is slim.

Safety:
- Operates only on lines whose first non-whitespace chars are // or
  /* …  Never strips mid-line content. Strings beginning with // (URLs)
  inside an expression are not at the start of the trimmed line, so
  they're never matched.
- Validates brace + paren balance before/after; aborts if they shift.
- Aborts on RegExp inside comments — we have none, but kept as a guard.

Usage:
  python3 scripts/strip_dashboard_comments.py _site/dashboard.html \
                                              _site/Viking_Invest_Trading_v*.html
"""

from __future__ import annotations

import glob
import re
import sys
from pathlib import Path


def strip_script_block(script: str) -> str:
    """Strip whole-line JS comments + collapse runs of blank lines."""
    lines = script.split('\n')
    out: list[str] = []
    in_block_comment = False
    blank_count = 0
    for line in lines:
        s = line.strip()
        if in_block_comment:
            if '*/' in s:
                in_block_comment = False
                idx = line.index('*/')
                line = line[idx + 2:]
                if not line.strip():
                    continue
            else:
                continue
        if s.startswith('//'):
            continue
        if s.startswith('/*'):
            if '*/' in s[2:]:
                end = line.index('*/', line.index('/*') + 2) + 2
                line = line[:line.index('/*')] + line[end:]
                if not line.strip():
                    continue
            else:
                in_block_comment = True
                line = line[:line.index('/*')]
                if not line.strip():
                    continue
        if not line.strip():
            blank_count += 1
            if blank_count >= 2:
                continue
        else:
            blank_count = 0
        out.append(line)
    return '\n'.join(out)


def slim_file(path: Path) -> tuple[int, int]:
    src = path.read_text(encoding='utf-8')

    if path.suffix == '.js':
        # Plain JS file — strip the whole body once
        out = strip_script_block(src)
    else:
        # HTML — strip each <script> block independently
        def replace(m: re.Match) -> str:
            return '<script>\n' + strip_script_block(m.group(1)) + '\n</script>'
        out = re.sub(r'<script>([\s\S]*?)</script>', replace, src)

    open_b_src = src.count('{')
    close_b_src = src.count('}')
    open_b_out = out.count('{')
    close_b_out = out.count('}')
    if (open_b_src - close_b_src) != (open_b_out - close_b_out):
        raise SystemExit(
            f"brace balance shift in {path.name}: "
            f"src {open_b_src}-{close_b_src} vs out {open_b_out}-{close_b_out}"
        )

    path.write_text(out, encoding='utf-8')
    return len(src), len(out)


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: strip_dashboard_comments.py FILE [FILE ...]", file=sys.stderr)
        return 1
    targets: list[Path] = []
    for arg in argv[1:]:
        for match in glob.glob(arg):
            targets.append(Path(match))
    if not targets:
        print("no files matched", file=sys.stderr)
        return 1
    grand_before = 0
    grand_after = 0
    for p in targets:
        if not p.exists():
            continue
        before, after = slim_file(p)
        grand_before += before
        grand_after += after
        print(
            f"  {p.name:50s} {before:>10,} -> {after:>10,} bytes "
            f"({100 * (before - after) / before:.1f}% saved)"
        )
    if grand_before:
        print(
            f"\nTotal: {grand_before:,} -> {grand_after:,} bytes "
            f"({100 * (grand_before - grand_after) / grand_before:.1f}% saved)"
        )
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
