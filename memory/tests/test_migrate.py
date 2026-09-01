from pathlib import Path
from memory_sync.migrate import migrate_text, migrate_dir

SHARED = "---\nname: a\nmetadata:\n  sync_scope: shared\n---\n\nbody\n"
PLAIN = "---\nname: b\nmetadata:\n  type: project\n---\n\nbody\n"
DONE = "---\nname: c\nmetadata:\n  scope: estate\n---\n\nbody\n"
NO_META = "---\nname: d\ndescription: hello\n---\n\nbody\n"

# The shape that actually broke the first implementation: an UNQUOTED
# description containing a bare "colon space", which yaml.safe_load rejects.
NASTY = (
    "---\n"
    "name: e\n"
    "description: 868 (100/100 genuinely executing: 25 turn / 65 rotation)\n"
    "metadata: \n"
    "  sync_scope: shared\n"
    "  originSessionId: abc-123\n"
    "---\n"
    "\n"
    "body with --- dashes inside\n"
)


def test_sync_scope_shared_becomes_estate():
    out, changed = migrate_text(SHARED, "macmini")
    assert changed is True
    assert "scope: estate" in out
    assert "sync_scope" not in out


def test_missing_scope_gets_machine_default():
    out, changed = migrate_text(PLAIN, "macmini")
    assert changed is True
    assert "scope: macmini" in out


def test_already_migrated_is_untouched():
    out, changed = migrate_text(DONE, "macmini")
    assert changed is False
    assert out == DONE


def test_migration_is_idempotent():
    once, _ = migrate_text(SHARED, "macmini")
    twice, changed = migrate_text(once, "macmini")
    assert changed is False
    assert once == twice


def test_handles_unparseable_yaml_description():
    """Must not crash on a bare colon in an unquoted description."""
    out, changed = migrate_text(NASTY, "macmini")
    assert changed is True
    assert "scope: estate" in out
    assert "sync_scope" not in out


def test_description_is_preserved_byte_for_byte():
    """The migrator must NOT reformat content it was not asked to change."""
    out, _ = migrate_text(NASTY, "macmini")
    assert "description: 868 (100/100 genuinely executing: 25 turn / 65 rotation)\n" in out
    assert "body with --- dashes inside" in out
    assert "originSessionId: abc-123" in out


def test_only_one_line_changes():
    before = NASTY.splitlines()
    after, _ = migrate_text(NASTY, "macmini")
    after = after.splitlines()
    assert len(before) == len(after), "line count must not change for a swap"
    diff = [(b, a) for b, a in zip(before, after) if b != a]
    assert len(diff) == 1, "exactly one line should differ, got {}".format(diff)


def test_file_without_metadata_block_gets_one():
    out, changed = migrate_text(NO_META, "winpc")
    assert changed is True
    assert "metadata:" in out
    assert "scope: winpc" in out
    assert "description: hello" in out


def test_migrate_dir_counts_and_skips_memory_md(tmp_path):
    (tmp_path / "a.md").write_text(SHARED, encoding="utf-8")
    (tmp_path / "MEMORY.md").write_text(SHARED, encoding="utf-8")
    res = migrate_dir(tmp_path, "macmini")
    assert res["changed"] == 1
    assert res["skipped_index"] == 1
    assert "scope: estate" in (tmp_path / "a.md").read_text(encoding="utf-8")
    assert "sync_scope" in (tmp_path / "MEMORY.md").read_text(encoding="utf-8")


def test_migrate_dir_never_aborts_the_batch_on_one_bad_file(tmp_path):
    """A partial migration is worse than none: one bad file must not strand the rest."""
    (tmp_path / "good1.md").write_text(SHARED, encoding="utf-8")
    (tmp_path / "weird.md").write_text("no frontmatter at all\n", encoding="utf-8")
    (tmp_path / "good2.md").write_text(NASTY, encoding="utf-8")
    res = migrate_dir(tmp_path, "macmini")
    assert res["changed"] == 2
    assert "scope: estate" in (tmp_path / "good2.md").read_text(encoding="utf-8")


def test_migrate_dir_raises_on_a_missing_root():
    """A nonexistent root previously returned a clean {changed:0} - a zero that
    is indistinguishable from 'nothing needed migrating'. Absence of work must
    not read as success."""
    import pytest
    with pytest.raises(FileNotFoundError):
        migrate_dir("/definitely/not/a/real/path", "macmini")
