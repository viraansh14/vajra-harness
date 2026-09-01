"""Git transport. Every call is bounded; failure is reported, never swallowed.

Two corrections from review are encoded here:

O2  `receive.denyCurrentBranch=updateInstead` inspects the RECEIVING worktree,
    not the sender's. The receiver is dirty exactly when it is mid-turn writing
    a memory, so pushes fail intermittently and precisely when the peer is most
    active. The commit-on-write hook keeps the receiver clean; this module makes
    the residual failure loud rather than silent.

O2-minor  A non-fast-forward push must pull-rebase and retry IMMEDIATELY.
    Deferring to the 30-minute reconciler leaves a known divergence standing.
"""
import subprocess
from pathlib import Path

DEFAULT_TIMEOUT = 45


def run(args, cwd, timeout=DEFAULT_TIMEOUT):
    """Run git bounded by a timeout. Returns (returncode, combined_output)."""
    try:
        p = subprocess.run(["git"] + list(args), cwd=str(cwd), timeout=timeout,
                           capture_output=True, text=True)
        return p.returncode, (p.stdout + p.stderr).strip()
    except subprocess.TimeoutExpired:
        return 124, "timeout after {}s".format(timeout)
    except OSError as e:
        return 127, "git not runnable: {}".format(e)


def commit_all(repo, message):
    """Commit everything. Returns True if a commit was created."""
    run(["add", "-A"], repo)
    code, _ = run(["diff", "--cached", "--quiet"], repo)
    if code == 0:
        return False
    run(["commit", "-qm", message], repo)
    return True


def pull_rebase(repo, remote, branch):
    code, out = run(["pull", "--rebase", remote, branch], repo)
    if code == 0:
        return {"state": "PASS", "detail": "pulled"}
    unreachable = (
        "does not appear to be a git repository" in out
        or "Could not read from remote" in out
        or "No such remote" in out
        or "not a git repository" in out
        or "unable to access" in out
        or "timeout after" in out
    )
    if unreachable:
        return {"state": "UNKNOWN", "detail": "peer unreachable: {}".format(out[:200])}
    return {"state": "FAIL", "detail": out[:300]}


def push_with_retry(repo, remote, branch, attempts=3):
    """Push; on non-fast-forward, rebase and retry immediately."""
    last = ""
    for _ in range(attempts):
        code, out = run(["push", remote, branch], repo)
        if code == 0:
            return {"state": "PASS", "detail": "pushed"}
        last = out
        if "non-fast-forward" in out or "fetch first" in out or "rejected" in out:
            r = pull_rebase(repo, remote, branch)
            if r["state"] == "PASS":
                continue
            return {"state": r["state"],
                    "detail": "rebase before retry: " + r["detail"]}
        return {"state": "FAIL", "detail": out[:300]}
    return {"state": "FAIL", "detail": "exhausted retries: {}".format(last[:300])}


def remotes_of(repo):
    """Configured remote names, in config order."""
    code, out = run(["remote"], repo)
    if code != 0:
        return []
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def sync_repo(repo, candidates, branch, message="memory: sync"):
    """Commit, then pull+push via the first candidate remote that works.

    The LAN peer is reached by mDNS, which resolves intermittently on this
    network (observed 2026-09-01: the same host alternately resolving and
    returning 'Name or service not known' minutes apart). A single-remote
    transport therefore reports FAIL for reasons unrelated to the memory. Try
    candidates in order - mDNS, then raw IP, then the relay - and say which one
    actually carried the sync.
    """
    guard = repo_state(repo)
    if guard["state"] != "PASS":
        return dict(guard, remote=None, tried=[])
    commit_all(repo, message)
    have = set(remotes_of(repo))
    tried = []
    last = {"state": "UNKNOWN", "detail": "no candidate remote configured"}
    for name in candidates:
        if name not in have:
            tried.append("{}=absent".format(name))
            continue
        pulled = pull_rebase(repo, name, branch)
        if pulled["state"] != "PASS":
            tried.append("{}={}".format(name, pulled["state"]))
            last = pulled
            continue
        pushed = push_with_retry(repo, name, branch)
        pushed["remote"] = name
        pushed["tried"] = tried
        if pushed["state"] == "PASS":
            return pushed
        tried.append("{}=push:{}".format(name, pushed["state"]))
        last = pushed
    last = dict(last)
    last["remote"] = None
    last["tried"] = tried
    last["detail"] = "all remotes failed [{}]: {}".format(
        ", ".join(tried) or "none", last.get("detail", ""))[:400]
    return last


def repo_state(repo):
    """Refuse to sync a repo that is mid-rebase or on a detached HEAD.

    A conflicted pull leaves exactly this state, and every subsequent sync then
    fails with a message that looks like a transport fault ("non-fast-forward")
    while the real cause is local and unrelated. Observed live 2026-09-01: the
    branch ref and HEAD had silently diverged onto two equivalent histories.
    """
    g = Path(repo) / ".git"
    for marker, label in (("rebase-merge", "rebase"), ("rebase-apply", "rebase"),
                          ("MERGE_HEAD", "merge"), ("CHERRY_PICK_HEAD", "cherry-pick")):
        if (g / marker).exists():
            return {"state": "FAIL",
                    "detail": "repo is mid-{}: resolve or abort it before syncing "
                              "(.git/{} present)".format(label, marker)}
    code, out = run(["symbolic-ref", "-q", "HEAD"], repo)
    if code != 0:
        return {"state": "FAIL",
                "detail": "HEAD is DETACHED: the branch ref and HEAD have diverged; "
                          "checkout the branch before syncing"}
    return {"state": "PASS", "detail": "on {}".format(out.replace("refs/heads/", ""))}
