"""Parse and emit the YAML frontmatter Claude Code memories use.

Real memories are not guaranteed to be valid YAML. Many carry an UNQUOTED
description containing a bare "colon space" (`... genuinely executing: 25 turn
...`), which yaml.safe_load rejects outright. A strict parser therefore takes
the whole memory system down over one malformed file, so this degrades to a
tolerant line reader instead of raising.
"""
import re
import yaml

DELIM = "---"
RE_TOP = re.compile(r"^([A-Za-z_][\w-]*)\s*:\s*(.*)$")
RE_NESTED = re.compile(r"^\s+([A-Za-z_][\w-]*)\s*:\s*(.*)$")


def _split(text):
    """Return (frontmatter_text, body) or None if there is no frontmatter."""
    if not text.startswith(DELIM):
        return None
    parts = text.split(DELIM, 2)
    if len(parts) < 3:
        return None
    return parts[1], parts[2].lstrip("\n")


def _unquote(v):
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        return v[1:-1]
    return v


def _tolerant(fm_text):
    """Line-based fallback: top-level keys plus one nested block level.

    Good enough for what the index needs (name, description, metadata.scope)
    and it cannot fail on content it does not understand.
    """
    meta = {}
    current_block = None
    for line in fm_text.splitlines():
        if not line.strip():
            continue
        m = RE_NESTED.match(line)
        if m and current_block is not None:
            meta.setdefault(current_block, {})[m.group(1)] = _unquote(m.group(2))
            continue
        m = RE_TOP.match(line)
        if m:
            key, val = m.group(1), m.group(2).strip()
            if val == "":
                current_block = key
                meta.setdefault(key, {})
            else:
                current_block = None
                meta[key] = _unquote(val)
    return meta


def parse(text):
    """Return (frontmatter_dict, body). Missing frontmatter yields ({}, text)."""
    split = _split(text)
    if split is None:
        return {}, text
    fm_text, body = split
    try:
        meta = yaml.safe_load(fm_text) or {}
        if not isinstance(meta, dict):
            raise ValueError("frontmatter is not a mapping")
    except Exception:  # noqa: BLE001 - malformed YAML must degrade, not crash
        meta = _tolerant(fm_text)
    return meta, body


def scope_of(meta, default):
    """Resolve loading scope. ONLY metadata.scope is authoritative (spec D7/O3).

    Deliberately ignores legacy sync_scope: migrating that is migrate.py's job,
    and silently reinterpreting it here would hide un-migrated files.
    """
    md = meta.get("metadata") or {}
    if not isinstance(md, dict):
        return default
    scope = md.get("scope")
    return scope if scope else default


def dump(meta, body):
    """Serialise back to a memory file.

    Only used for synthetic/new content. The migrator deliberately does NOT use
    this, because safe_dump rewraps long descriptions and churns files.
    """
    head = yaml.safe_dump(meta, sort_keys=False, allow_unicode=True).rstrip("\n")
    return "{d}\n{h}\n{d}\n\n{b}".format(d=DELIM, h=head, b=body.lstrip("\n"))
