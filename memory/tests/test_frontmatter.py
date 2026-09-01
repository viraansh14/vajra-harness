import pytest
from memory_sync.frontmatter import parse, scope_of, dump

RAW = """---
name: capability-os
description: "A thing: with a colon, and \\"quotes\\""
metadata:
  sync_scope: shared
  type: project
---

Body line one.
"""


def test_parse_splits_meta_and_body():
    meta, body = parse(RAW)
    assert meta["name"] == "capability-os"
    assert meta["metadata"]["sync_scope"] == "shared"
    assert body.strip() == "Body line one."


def test_parse_handles_no_frontmatter():
    meta, body = parse("just a body\n")
    assert meta == {}
    assert body.strip() == "just a body"


def test_scope_of_prefers_explicit_scope():
    assert scope_of({"metadata": {"scope": "estate"}}, "winpc") == "estate"


def test_scope_of_falls_back_to_default_when_absent():
    assert scope_of({"metadata": {}}, "winpc") == "winpc"


def test_scope_of_never_guesses_from_sync_scope():
    # migration is Task 2's job; the parser must not silently reinterpret
    assert scope_of({"metadata": {"sync_scope": "shared"}}, "winpc") == "winpc"


def test_dump_roundtrips():
    meta, body = parse(RAW)
    meta["metadata"]["scope"] = "estate"
    out = dump(meta, body)
    meta2, body2 = parse(out)
    assert meta2["metadata"]["scope"] == "estate"
    assert body2.strip() == body.strip()


# Real memories carry UNQUOTED descriptions with bare "colon space" sequences,
# which yaml.safe_load rejects. The parser must degrade, never crash: an index
# that dies on one malformed file takes the whole memory system down with it.
NASTY = (
    "---\n"
    "name: capos-v13-actuators\n"
    "description: 868 (100/100 genuinely executing: 25 turn / 65 rotation / 9 auto)\n"
    "metadata: \n"
    "  scope: estate\n"
    "  type: project\n"
    "---\n"
    "\n"
    "body\n"
)


def test_parse_survives_unquoted_colon_in_description():
    meta, body = parse(NASTY)
    assert meta["name"] == "capos-v13-actuators"
    assert meta["metadata"]["scope"] == "estate"
    assert body.strip() == "body"


def test_parse_recovers_the_full_description_text():
    meta, _ = parse(NASTY)
    assert "genuinely executing: 25 turn" in meta["description"]


def test_scope_resolves_from_a_yaml_hostile_file():
    meta, _ = parse(NASTY)
    assert scope_of(meta, "winpc") == "estate"


def test_parse_strips_wrapping_quotes_in_fallback():
    raw = "---\nname: x\ndescription: \"has: colon\"\nmetadata:\n  scope: estate\n---\n\nb\n"
    meta, _ = parse(raw)
    assert meta["description"] == "has: colon"
