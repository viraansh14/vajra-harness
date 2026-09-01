"""Behavioural tests for the coordination bus.

Every test runs against a throwaway CLAUDEBUS_HOME. That isolation is not
politeness: the bus auto-discovers a live database under ~/.claude/claudebus,
so a test that forgot to set it would silently read and write the operator's
real message history and pass while corrupting it.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

BUS = Path(__file__).resolve().parents[2] / "src" / "vajra_harness" / "bus" / "claudebus.py"


@pytest.fixture()
def home(tmp_path):
    d = tmp_path / "bushome"
    d.mkdir()
    return d


def run(home, *args, timeout=60):
    env = dict(os.environ, CLAUDEBUS_HOME=str(home))
    p = subprocess.run([sys.executable, str(BUS), *args],
                       capture_output=True, text=True, env=env, timeout=timeout)
    return p.returncode, p.stdout + p.stderr


# ---------------------------------------------------------------- messaging

def test_directed_message_reaches_only_the_addressee(home):
    run(home, "--as", "alice", "send", "for-bob-only", "--to", "bob")
    _, bob = run(home, "--as", "bob", "recv")
    assert "for-bob-only" in bob
    _, carol = run(home, "--as", "carol", "recv")
    assert "for-bob-only" not in carol


def test_a_message_is_delivered_once(home):
    run(home, "--as", "alice", "send", "once-only", "--to", "bob")
    _, first = run(home, "--as", "bob", "recv")
    _, second = run(home, "--as", "bob", "recv")
    assert "once-only" in first
    # redelivery would make every hook that drains the bus reprocess history
    assert "once-only" not in second


# -------------------------------------------------------------------- lanes

def test_lane_is_exclusive(home):
    code, out = run(home, "--as", "alice", "claim", "migrations")
    assert code == 0 and "claimed" in out
    _, denied = run(home, "--as", "bob", "claim", "migrations")
    assert "alice" in denied, "a second claimant must be told who holds it"


def test_released_lane_can_be_reclaimed(home):
    run(home, "--as", "alice", "claim", "build")
    run(home, "--as", "alice", "release", "build")
    code, out = run(home, "--as", "bob", "claim", "build")
    assert code == 0 and "claimed" in out


def test_lane_survives_the_process_that_claimed_it(home):
    """Lanes are held by identity, not PID.

    Each run() is a separate short-lived process that has already exited. A
    lock keyed on liveness would free itself the instant the claiming process
    returned, which is exactly when the work it guards is still in flight.
    """
    run(home, "--as", "alice", "claim", "deploy")
    _, out = run(home, "lanes")
    assert "deploy" in out and "alice" in out


# --------------------------------------------------------------- blackboard

def test_blackboard_round_trip(home):
    run(home, "--as", "alice", "bb", "set", "schema", "7")
    _, out = run(home, "--as", "bob", "bb", "get", "schema")
    assert "7" in out


def test_compare_and_set_rejects_a_stale_version(home):
    run(home, "--as", "alice", "bb", "set", "k", "v1")          # version 1
    run(home, "--as", "alice", "bb", "set", "k", "v2")          # version 2
    # bob still believes it is at version 1
    code, _ = run(home, "--as", "bob", "bb", "set", "k", "v3", "--if-ver", "1")
    assert code != 0, "a stale CAS must fail, not silently clobber"
    _, out = run(home, "--as", "carol", "bb", "get", "k")
    assert "v2" in out


# --------------------------------------------------------------- work queue

def test_task_is_taken_by_exactly_one_worker(home):
    run(home, "--as", "boss", "submit", "compile")
    _, first = run(home, "--as", "w1", "take")
    _, second = run(home, "--as", "w2", "take")
    assert "compile" in first
    assert "compile" not in second, "two workers must not take the same task"


def test_failed_task_returns_to_the_queue(home):
    run(home, "--as", "boss", "submit", "flaky")
    run(home, "--as", "w1", "take")
    run(home, "--as", "w1", "fail", "1")
    _, out = run(home, "--as", "boss", "tasks")
    assert "flaky" in out, "a nacked task must not vanish"


def test_priority_orders_the_queue(home):
    run(home, "--as", "boss", "submit", "low-priority")
    run(home, "--as", "boss", "submit", "urgent", "--prio", "9")
    _, out = run(home, "--as", "w1", "take")
    assert "urgent" in out


# ------------------------------------------------------------- presence/ps

def test_ps_lists_a_peer_that_has_spoken(home):
    run(home, "--as", "alice", "send", "hi", "--to", "bob")
    _, out = run(home, "ps")
    assert "alice" in out


# ------------------------------------------------------------- portability

def test_wait_times_out_cleanly_on_every_platform(home):
    """The FIFO push path is POSIX-only.

    Windows has no mkfifo and its select() accepts only sockets, so listeners
    fall back to polling. This test is the one that catches a regression there:
    before the fallback existed, this raised AttributeError on Windows instead
    of timing out.
    """
    code, out = run(home, "--as", "carol", "wait", "--timeout", "2", timeout=60)
    assert "Traceback" not in out
    assert code in (0, 1), f"unexpected exit {code}: {out}"


def test_no_command_crashes_on_an_empty_bus(home):
    for args in (["lanes"], ["ps"], ["tasks"], ["--as", "x", "recv"]):
        code, out = run(home, *args)
        assert "Traceback" not in out, f"{args} crashed: {out}"


def test_state_stays_inside_claudebus_home(home, tmp_path):
    """Guards the isolation the rest of this file depends on."""
    run(home, "--as", "alice", "send", "hello", "--to", "bob")
    assert (home / "bus.db").exists(), "bus did not honour CLAUDEBUS_HOME"
