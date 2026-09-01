# vajra-harness

Infrastructure for running several AI coding agents at once, across more than one machine,
without them silently trampling each other.

Two components, both dependency-free Python:

- **`bus/`** is a local coordination bus. Agents on one machine send messages, take exclusive
  locks, negotiate work by auction, share state, and run a work queue through it.
- **`memory/`** is a cross-machine memory sync. Durable facts written by an agent on one
  machine converge onto the other, with a generated index and a status file that is allowed to
  say `DEGRADED` out loud.

This is extracted from a working two-machine setup (a macOS laptop and a Windows workstation)
that has been running it in production. It is not a demo.

## The problem

One agent in one repo needs no coordination. The trouble starts at two.

Two agents editing the same file produce a merge conflict at best and a silent overwrite at
worst. Two agents each deciding to run the migration produce one success and one confusing
failure. An agent that finishes work nobody asked it to redo has burned an hour. And the moment
the agents live on different machines, they each accumulate knowledge the other cannot see, so
they diverge without either one noticing.

Every one of those is a *coordination* problem, not an intelligence problem, and they are all
solved problems in distributed systems. This is that literature, applied.

## The bus

SQLite in WAL mode is the durable, concurrency-safe source of truth. A per-waiter FIFO gives
instant push: a `send` pokes every active waiter's pipe, waking a blocked `wait` in
microseconds. There is no daemon to babysit and nothing to start at boot.

```bash
claudebus send "refactoring the auth module" --to worker-2
claudebus claim migrations --note "running 0007"    # exclusive lane, TTL'd
claudebus bb set schema-version 7 --if-ver 6        # compare-and-set
claudebus submit "run integration suite" --prio 5
claudebus cfp "port the parser" && claudebus bids   # auction, then award
```

Each subsystem is a named pattern rather than something improvised:

| Feature | Pattern | Lineage |
|---|---|---|
| `cfp` / `bid` / `award` | Contract net: negotiate work by auction instead of blind assignment | Smith 1980, FIPA Contract Net |
| `bb set/get/del` | Blackboard: shared versioned KV with compare-and-set and TTL | Linda tuple space, Hearsay-II |
| `submit` / `take` / `fail` / `retry` | Work queue with priority, exponential backoff, dead-letter queue, and `--after` dependency gating | SQS reliability semantics, Airflow-style DAGs |
| `caps` / `discover` | Capability discovery: route work by what a peer can actually do | FIPA directory facilitator, A2A agent cards |
| `elect` / `leader` | Leader election for singleton work | standard |
| `claim` / `release` | TTL'd advisory lanes, held by identity rather than PID | standard |

The lane design is worth calling out. A lane is held by an agent *identity*, which outlives any
single short-lived process, so freedom is TTL-based rather than liveness-based. A lock keyed on
a PID is wrong the moment the process that took it exits normally.

## Memory sync

Each fact declares a scope, and the scope decides where it lives and whether it travels:

| Directory | Meaning | Syncs |
|---|---|---|
| `_shared/` | True regardless of machine | Yes |
| `_local/` | About *this* machine only | No |
| `_peer-local/` | Read-only mirror of the peer's `_local/` | Pull |

`_peer-local` is the part that is easy to get wrong. Each machine needs to *know* the peer's
local facts without *owning* them, or you get two writers on one file and a conflict every sync.

Gates run on every index and sync, and the verdict is the worst of them, never rounded up:
`completeness`, `lint`, `placement`, `transport`. A run where the memory is intact but the
network leg is down reports `DEGRADED`. An operator needs to tell "your memory is broken" apart
from "your memory is fine and the Wi-Fi is not".

## Three silent failures it exists to catch

**A completeness check that only sees what the collector found.** If a fact is filed under the
wrong scope, the collector never walks that directory, so the index looks complete and nothing
errors. `audit()` therefore walks every directory independently and reports files the collector
never saw.

**A transport that fails for reasons unrelated to the data.** mDNS on a real LAN resolves
intermittently, returning `Name or service not known` minutes apart for a host that is up. A
single-remote sync reads that as data loss. Sync instead walks an ordered candidate list (mDNS
name, then raw IP, then a relay) and reports which one carried the run.

**A stale status file that reads as a fresh pass.** A verdict with no timestamp cannot be told
apart from one left behind by a run that has since started failing. Every status write carries
the time it was written.

## Refusals

The sync refuses to run against a repo left mid-rebase or on a detached HEAD. Syncing from a
detached HEAD silently commits to no branch and the work vanishes on the next checkout.
Refusing is louder than succeeding into a void.

## Install

```bash
python bus/install_bus.py            # installs the bus into ~/.claude/claudebus
cd memory && pip install -e .        # installs the memory-sync CLI
python -m pytest -q                  # 55 tests
```

Python 3.12+. The bus needs nothing but the standard library. The sync needs `pyyaml` and
invokes `git` as a subprocess rather than through a binding.

`$CLAUDEBUS_HOME` relocates the bus state directory, which is what the test suite uses to stay
away from a live bus.

## Platform notes

Both components run on macOS, Linux and Windows.

The instant-poke path is POSIX-only, because Windows has no `mkfifo` and its `select()` accepts
only sockets. On Windows, listeners fall back to polling: identical delivery semantics, with
latency bounded by a one second interval instead of microseconds. Everything else is unchanged.

## Layout

```
bus/       coordination bus, session hooks, installer, cross-harness worker
memory/    memory sync package, tests, and the scheduled reconciler scripts
docs/      design notes
```

## License

MIT.
