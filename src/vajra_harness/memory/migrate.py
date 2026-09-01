"""Idempotent migration of legacy sync_scope to spec scope.

Deliberately does NOT round-trip through YAML. Two real failures forced this:

1. Real memories carry UNQUOTED descriptions containing a bare "colon space"
   (`... genuinely executing: 25 turn ...`), which yaml.safe_load rejects
   outright, aborting the batch part-way and stranding a partial migration.
2. yaml.safe_dump reformatted every description it did parse, rewrapping long
   single-line strings into folded multi-line form. Semantically equal, but it
   churns files it was never asked to touch and buries the real change.

So this edits the one line it must and leaves every other byte alone.
"""
import re
from pathlib import Path

LEGACY_MAP = {"shared": "estate"}

DELIM = "---"
RE_SCOPE = re.compile(r"^\s+scope\s*:")
RE_SYNC_SCOPE = re.compile(r"^(\s+)sync_scope\s*:\s*(\S+)\s*$")
RE_META = re.compile(r"^metadata\s*:\s*$")
RE_INDENTED = re.compile(r"^\s+\S")


def _frontmatter_bounds(lines):
    """Return (start, close) indices of the frontmatter body, or None."""
    if not lines or lines[0].rstrip("\n") != DELIM:
        return None
    for i in range(1, len(lines)):
        if lines[i].rstrip("\n") == DELIM:
            return 1, i
    return None


def migrate_text(text, default_scope):
    """Return (new_text, changed). Never raises on malformed frontmatter."""
    lines = text.splitlines(keepends=True)
    bounds = _frontmatter_bounds(lines)
    if not bounds:
        return text, False
    start, close = bounds
    fm = list(lines[start:close])

    # Already migrated: an indented scope: key anywhere in the frontmatter.
    if any(RE_SCOPE.match(ln) for ln in fm):
        return text, False

    # Case 1: swap a legacy sync_scope line in place. Line count unchanged.
    for i, ln in enumerate(fm):
        m = RE_SYNC_SCOPE.match(ln)
        if m:
            indent, val = m.group(1), m.group(2)
            fm[i] = "{}scope: {}\n".format(indent, LEGACY_MAP.get(val, default_scope))
            return "".join(lines[:start] + fm + lines[close:]), True

    # Case 2: a metadata block exists; append scope as its last key.
    for i, ln in enumerate(fm):
        if RE_META.match(ln.rstrip("\n")):
            j = i + 1
            indent = "  "
            while j < len(fm) and RE_INDENTED.match(fm[j]):
                indent = re.match(r"^(\s+)", fm[j]).group(1)
                j += 1
            fm.insert(j, "{}scope: {}\n".format(indent, default_scope))
            return "".join(lines[:start] + fm + lines[close:]), True

    # Case 3: no metadata block at all; create one.
    fm.append("metadata:\n")
    fm.append("  scope: {}\n".format(default_scope))
    return "".join(lines[:start] + fm + lines[close:]), True


def migrate_dir(path, default_scope):
    """Migrate every .md except the generated index.

    One unreadable file must never strand the rest: a partial migration is
    worse than none, because half the estate silently disagrees with the other.
    """
    root = Path(path)
    if not root.is_dir():
        # A silent zero here is indistinguishable from "nothing to do".
        raise FileNotFoundError("migrate root does not exist: {}".format(root))
    res = {"changed": 0, "unchanged": 0, "skipped_index": 0, "errors": []}
    for f in sorted(root.glob("*.md")):
        if f.name == "MEMORY.md":
            res["skipped_index"] += 1
            continue
        try:
            text = f.read_text(encoding="utf-8")
            out, changed = migrate_text(text, default_scope)
            if changed:
                f.write_text(out, encoding="utf-8", newline="")
                res["changed"] += 1
            else:
                res["unchanged"] += 1
        except Exception as e:  # noqa: BLE001 - report, never abort the batch
            res["errors"].append("{}: {}".format(f.name, e))
    return res
