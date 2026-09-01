"""Verdicts, the completeness gate, and status persistence.

Governing rule, learned the hard way: a probe that cannot measure reports
UNKNOWN, never PASS. Success-by-absence is how an instrument reports healthy
while failing.
"""
import json
from pathlib import Path
from .frontmatter import parse, scope_of


def completeness(root, entries, machine):
    """Count loadable files on disk against indexed entries.

    An unindexed memory is invisible, and invisibility is silent, so this
    compares against an independent denominator rather than trusting the index.
    """
    denom = 0
    for d, default in (("_shared", "estate"), ("_local", machine)):
        base = Path(root) / d
        if not base.is_dir():
            continue
        for f in base.glob("*.md"):
            if f.name == "MEMORY.md":
                continue
            meta, _ = parse(f.read_text(encoding="utf-8"))
            if scope_of(meta, default) in ("estate", machine):
                denom += 1
    got = len(entries)
    if got == denom:
        return {"state": "PASS", "detail": "{}/{} indexed".format(got, denom)}
    return {"state": "FAIL",
            "detail": "{}/{} indexed, {} missing".format(got, denom, denom - got)}


def verdict(checks):
    """INCOMPLETE > BAD > DEGRADED > OK. UNKNOWN can never yield OK."""
    states = [c.get("state") for c in checks.values()]
    if "INCOMPLETE" in states:
        return "INCOMPLETE"
    if "FAIL" in states:
        return "BAD"
    if "UNKNOWN" in states:
        return "DEGRADED"
    return "OK"


def write_status(path, payload):
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
