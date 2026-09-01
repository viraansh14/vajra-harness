import json
from pathlib import Path
from memory_sync.status import completeness, verdict, write_status
from memory_sync.index import collect


def _mem(name, scope):
    return "---\nname: {n}\nmetadata:\n  scope: {s}\n---\n\nb\n".format(n=name, s=scope)


def test_completeness_ok_when_every_loadable_file_is_indexed(tmp_path):
    (tmp_path / "_shared").mkdir()
    (tmp_path / "_shared" / "a.md").write_text(_mem("a", "estate"), encoding="utf-8")
    res = completeness(tmp_path, collect(tmp_path, "winpc"), "winpc")
    assert res["state"] == "PASS"


def test_completeness_fails_when_a_file_is_unindexed(tmp_path):
    (tmp_path / "_shared").mkdir()
    (tmp_path / "_shared" / "a.md").write_text(_mem("a", "estate"), encoding="utf-8")
    (tmp_path / "_shared" / "b.md").write_text(_mem("b", "estate"), encoding="utf-8")
    entries = collect(tmp_path, "winpc")[:1]  # simulate a dropped entry
    res = completeness(tmp_path, entries, "winpc")
    assert res["state"] == "FAIL"
    assert "1" in res["detail"]


def test_verdict_unknown_never_becomes_ok():
    assert verdict({"a": {"state": "UNKNOWN"}}) == "DEGRADED"


def test_verdict_fail_dominates_unknown():
    assert verdict({"a": {"state": "UNKNOWN"}, "b": {"state": "FAIL"}}) == "BAD"


def test_verdict_all_pass_is_ok():
    assert verdict({"a": {"state": "PASS"}}) == "OK"


def test_write_status_is_valid_json(tmp_path):
    p = tmp_path / "s.json"
    write_status(p, {"verdict": "OK"})
    assert json.loads(p.read_text(encoding="utf-8"))["verdict"] == "OK"
