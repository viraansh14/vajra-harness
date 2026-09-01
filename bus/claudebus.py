#!/usr/bin/env python3
"""
claudebus V5 - a fast, local message + coordination bus for full Claude,
Codex and bridge-agent sessions on one machine.

What it is:
  * SQLite (WAL) is the durable, concurrent-safe source of truth.
  * A per-waiter FIFO gives instant push: `send` pokes every active waiter's
    pipe, waking a blocking `wait` in ~microseconds. No daemon to babysit.

V5 over V4 (all V4 commands remain compatible):
  * Non-stealing, atomic session identity ownership with losing-side recovery.
  * Provider endpoint registry for Claude/Codex native session and thread IDs.
  * Append-only, idempotent lifecycle/evidence journal and CapOS projection.
  * Codex lifecycle hooks plus an app-server worker transport.

V4 over V3 (all V3 commands behave identically) - each subsystem composes a
proven coordination pattern onto the same SQLite+FIFO core:
  * Contract net (`cfp`/`bid`/`award`/`bids`): task negotiation by auction
    instead of blind assignment (Smith 1980; FIPA Contract Net). Rides the
    messages table's existing kind/corr columns.
  * Blackboard (`bb set/get/del/list`): shared versioned KV with optimistic
    compare-and-set (`--if-ver`) and TTL - the Linda tuple-space /
    Hearsay-II coordination-medium model. State peers poll, not messages.
  * Work-queue v2: `submit --prio/--max-attempts/--after`, `fail` (nack with
    exponential backoff), `retry`, and a dead-letter queue (`tasks --dlq`) -
    SQS/Forq reliability semantics; `--after` gives Airflow-style DAG
    dependency gating so a pipeline can be submitted whole.
  * Capability discovery (`caps`, `discover`, `hello --caps`): agent-card-lite
    (Google A2A / FIPA directory facilitator) - route work by what a peer can
    do, not by name.
  * Leader election (`elect`/`leader`): lease-based, Chubby-style, reusing the
    lanes TTL machinery; `leave` releases leadership with the session.
  * Wildcard channels: MQTT/NATS-style `--sub 'build.*'` topic subscriptions.

V3 over V2 (all V2 commands behave identically):
  * Session-id identity: `hello` binds Claude's session_id to a bus name
    (identities table); whoami resolves --as > binding > $CLAUDEBUS_ID >
    tab-<ppid>. No shell wrapper needed; env inheritance can't lie.
  * Always-on hooks via hook.py (one dispatcher for SessionStart /
    UserPromptSubmit / PostToolUse / Stop / SessionEnd): every interactive
    session auto-joins, gets messages at turn start, MID-TURN (throttled
    non-destructive `pulse`), and at turn end (Stop delivers + blocks to make
    the session respond), and `leave`s cleanly. Headless `claude -p` runs are
    excluded by parent-argv inspection, NOT by the dead
    CLAUDE_CODE_CHILD_SESSION env gate (CC 2.1.x sets it for every subprocess).
  * Idle reachability: sessions park `claudebus wait --json` as a background
    task; traffic wakes the harness, which re-invokes the model. `armed`
    reports listener state; the Stop hook enforces re-arming.
  * `deliver` no longer echoes a sender's own broadcasts back to it.

V2 over V1 (all V1 commands behave identically):
  * Bounded catch-up: a brand-new consumer starts at HEAD, never inherits days
    of backlog. `recv --since all|N` opts into history. (Fixes the hook flood.)
  * Atomic delivery: cursor advance runs in BEGIN IMMEDIATE, so concurrent
    `recv` for the same consumer can't double-deliver or drop.
  * Lanes/locks: `claim`/`release`/`lanes` - first-class mutual exclusion over a
    named resource (replaces manual "shout if you're mid-edit" broadcasts).
  * Request/reply: `ask`/`reply` - correlated RPC between tabs.
  * Work-queue: `submit`/`take`/`done`/`tasks` - at-least-once task hand-off
    across N tabs (visibility-timeout lease + claimer ack).
  * Real presence: `ps` checks pid liveness; dead tabs show `dead`, get GC'd.
  * Auto-prune: old messages / dead peers / finished tasks expire on their own.

No third-party deps. Stdlib only. macOS/Linux.

Identity:  --as NAME  >  $CLAUDEBUS_ID  >  tab-<ppid>
Home (state dir):  $CLAUDEBUS_HOME  >  ~/.claude/claudebus
Addressing:
  send "hi"                 -> broadcast on channel 'all'
  send "hi" --to bob        -> direct message (channel '@bob')
  send "hi" --channel build -> topic channel 'build'
recv delivers, for ME: channel 'all' + '@me' + any channel I subscribe to
  (via --sub a,b or $CLAUDEBUS_CHANNELS).
"""
import argparse
import errno
import fnmatch
import hashlib
import json
import os
import random
import re
import select
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
import uuid

HOME = os.path.expanduser("~")
ROOT = os.environ.get("CLAUDEBUS_HOME") or os.path.join(HOME, ".claude", "claudebus")
DB = os.path.join(ROOT, "bus.db")
WAITERS = os.path.join(ROOT, "waiters")

MSG_TTL = float(os.environ.get("CLAUDEBUS_MSG_TTL", 3 * 86400))   # prune msgs older than
PEER_TTL = float(os.environ.get("CLAUDEBUS_PEER_TTL", 3600))      # GC dead peers older than
TASK_TTL = float(os.environ.get("CLAUDEBUS_TASK_TTL", 3600))      # purge done tasks older than
TAB_TTL = float(os.environ.get("CLAUDEBUS_TAB_TTL", 900))         # GC anonymous tab-* peers faster
SPAWN_TTL = float(os.environ.get("CLAUDEBUS_SPAWN_TTL", 7 * 86400))  # GC dead spawn rows older than
PRUNE_EVERY = float(os.environ.get("CLAUDEBUS_PRUNE_EVERY", 300))    # min secs between auto-GC sweeps
DEAD_LINGER = float(os.environ.get("CLAUDEBUS_DEAD_LINGER", 3600))   # sessions/tree hide dead older than
LIVE_WINDOW = 120         # seconds since heartbeat to count a peer "active"
SELECT_CEIL = 60          # cap a blocking select() so a missed poke self-heals

# a stray poke to a just-dead waiter must never kill `send`
try:
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except (ValueError, AttributeError, OSError):
    pass

C = {  # ansi colors, suppressed when not a tty
    "dim": "\033[2m", "b": "\033[1m", "cy": "\033[36m", "gn": "\033[32m",
    "yl": "\033[33m", "mg": "\033[35m", "rd": "\033[31m", "rs": "\033[0m",
}


def color(s, k):
    if not sys.stdout.isatty():
        return s
    return f"{C[k]}{s}{C['rs']}"


def ensure():
    os.makedirs(WAITERS, exist_ok=True)


def _cols(con, table):
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}


SCHEMA_VERSION = 10


def _migrate(con):
    """Build/upgrade schema. Runs only when user_version < SCHEMA_VERSION, so
    steady-state opens skip all DDL (and its locks) entirely."""
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            sender TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'all',
            body TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_msg_chan ON messages(channel, id);
        CREATE TABLE IF NOT EXISTS cursors(
            consumer TEXT PRIMARY KEY,
            last_id INTEGER NOT NULL DEFAULT 0,
            dm_id INTEGER NOT NULL DEFAULT 0,
            updated REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS peers(
            name TEXT PRIMARY KEY,
            pid INTEGER, tty TEXT, last_seen REAL
        );
        CREATE TABLE IF NOT EXISTS lanes(
            lane TEXT PRIMARY KEY,
            holder TEXT NOT NULL,
            pid INTEGER,
            ts REAL NOT NULL,
            ttl REAL NOT NULL DEFAULT 0,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS tasks(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            queue TEXT NOT NULL DEFAULT 'default',
            body TEXT NOT NULL,
            submitter TEXT,
            claimed_by TEXT,
            lease_until REAL NOT NULL DEFAULT 0,
            gen INTEGER NOT NULL DEFAULT 0,
            done INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_task_q ON tasks(queue, done, lease_until, id);
        CREATE TABLE IF NOT EXISTS spawns(
            name TEXT PRIMARY KEY,
            tmux TEXT NOT NULL,
            parent TEXT,
            task TEXT,
            ts REAL NOT NULL,
            agent TEXT NOT NULL DEFAULT 'claude',
            transport TEXT NOT NULL DEFAULT 'tmux'
        );
        CREATE TABLE IF NOT EXISTS identities(
            session_id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            created REAL NOT NULL,
            owner_pid INTEGER,
            provider TEXT NOT NULL DEFAULT 'claude',
            updated REAL NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS endpoints(
            name TEXT PRIMARY KEY,
            provider TEXT NOT NULL,
            native_session_id TEXT,
            native_thread_id TEXT,
            parent TEXT,
            transport TEXT,
            cwd TEXT,
            model TEXT,
            permission_profile TEXT,
            state TEXT NOT NULL DEFAULT 'joined',
            last_seen REAL NOT NULL,
            metadata TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_endpoint_native_thread
            ON endpoints(provider, native_thread_id)
            WHERE provider='codex' AND native_thread_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_endpoint_native_session
            ON endpoints(provider, native_session_id)
            WHERE provider<>'codex' AND native_session_id IS NOT NULL;
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            type TEXT NOT NULL,
            actor TEXT,
            subject TEXT,
            provider TEXT,
            corr TEXT,
            causal_id INTEGER,
            idempotency_key TEXT UNIQUE,
            status TEXT NOT NULL DEFAULT 'observed',
            payload TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_type_id ON events(type, id);
        CREATE INDEX IF NOT EXISTS idx_events_corr_id ON events(corr, id);
        CREATE TABLE IF NOT EXISTS prefs(
            name TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            updated REAL NOT NULL,
            PRIMARY KEY(name, key)
        );
        CREATE TABLE IF NOT EXISTS blackboard(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            author TEXT,
            ver INTEGER NOT NULL DEFAULT 1,
            ts REAL NOT NULL,
            ttl REAL NOT NULL DEFAULT 0
        );
        """
    )
    # additive migration of the V1 messages table -> kind / corr / reply_to
    mc = _cols(con, "messages")
    if "kind" not in mc:
        con.execute("ALTER TABLE messages ADD COLUMN kind TEXT NOT NULL DEFAULT 'msg'")
    if "corr" not in mc:
        con.execute("ALTER TABLE messages ADD COLUMN corr TEXT")
    if "reply_to" not in mc:
        con.execute("ALTER TABLE messages ADD COLUMN reply_to TEXT")
    if "dm_id" not in _cols(con, "cursors"):
        con.execute("ALTER TABLE cursors ADD COLUMN dm_id INTEGER NOT NULL DEFAULT 0")
        # backfill: existing V1 consumers already saw everything up to last_id
        # (incl. DMs), so seed dm_id there to avoid a one-time DM re-delivery.
        con.execute("UPDATE cursors SET dm_id=last_id WHERE dm_id<last_id")
    if "agent" not in _cols(con, "spawns"):
        con.execute("ALTER TABLE spawns ADD COLUMN agent TEXT NOT NULL DEFAULT 'claude'")
    if "transport" not in _cols(con, "spawns"):
        con.execute("ALTER TABLE spawns ADD COLUMN transport TEXT NOT NULL DEFAULT 'tmux'")
    # V4 additive migrations: queue reliability + DAG deps + agent capabilities
    tc = _cols(con, "tasks")
    for cname, ddl in (("prio", "INTEGER NOT NULL DEFAULT 0"),
                       ("max_attempts", "INTEGER NOT NULL DEFAULT 3"),
                       ("not_before", "REAL NOT NULL DEFAULT 0"),
                       ("dead", "INTEGER NOT NULL DEFAULT 0"),
                       ("after_ids", "TEXT")):
        if cname not in tc:
            con.execute(f"ALTER TABLE tasks ADD COLUMN {cname} {ddl}")
    if "caps" not in _cols(con, "peers"):
        con.execute("ALTER TABLE peers ADD COLUMN caps TEXT")
    ic = _cols(con, "identities")
    for cname, ddl in (("owner_pid", "INTEGER"),
                       ("provider", "TEXT NOT NULL DEFAULT 'claude'"),
                       ("updated", "REAL NOT NULL DEFAULT 0")):
        if cname not in ic:
            con.execute(f"ALTER TABLE identities ADD COLUMN {cname} {ddl}")
    con.execute("UPDATE identities SET updated=created WHERE updated=0")
    # Repair the short-lived pre-v10 staging index, which incorrectly treated
    # Codex session-tree IDs as per-thread unique identities.
    con.execute("DROP INDEX IF EXISTS idx_endpoint_native")
    con.execute("DROP INDEX IF EXISTS idx_endpoint_native_thread")
    con.execute("DROP INDEX IF EXISTS idx_endpoint_native_session")
    con.execute(
        "CREATE UNIQUE INDEX idx_endpoint_native_thread ON endpoints("
        "provider,native_thread_id) WHERE provider='codex' "
        "AND native_thread_id IS NOT NULL"
    )
    con.execute(
        "CREATE UNIQUE INDEX idx_endpoint_native_session ON endpoints("
        "provider,native_session_id) WHERE provider<>'codex' "
        "AND native_session_id IS NOT NULL"
    )
    # Existing v4 participants must be visible immediately after migration.
    # Provider-native joins will replace these conservative legacy projections
    # only through the ownership checks in upsert_endpoint.
    con.execute(
        "INSERT OR IGNORE INTO endpoints(name,provider,native_session_id,"
        "native_thread_id,state,last_seen,metadata) SELECT name,provider,"
        "CASE WHEN provider='codex' THEN NULL ELSE session_id END,"
        "CASE WHEN provider='codex' THEN session_id ELSE NULL END,'legacy-bound',"
        "MAX(created,updated),'{\"migrated_from\":\"identities\"}' FROM identities"
    )
    con.execute(
        "INSERT OR IGNORE INTO endpoints(name,provider,parent,transport,state,"
        "last_seen,metadata) SELECT name,agent,parent,transport,'legacy-spawn',ts,"
        "'{\"migrated_from\":\"spawns\"}' FROM spawns"
    )
    con.execute(
        "UPDATE endpoints SET "
        "parent=COALESCE(parent,(SELECT parent FROM spawns WHERE spawns.name=endpoints.name)),"
        "transport=COALESCE(transport,(SELECT transport FROM spawns "
        "WHERE spawns.name=endpoints.name)),"
        "state='legacy-spawn',"
        "last_seen=MAX(last_seen,(SELECT ts FROM spawns WHERE spawns.name=endpoints.name)) "
        "WHERE EXISTS(SELECT 1 FROM spawns WHERE spawns.name=endpoints.name)"
    )
    con.execute(
        "INSERT OR IGNORE INTO endpoints(name,provider,state,last_seen,metadata) "
        "SELECT name,'legacy','legacy-peer',last_seen,"
        "'{\"migrated_from\":\"peers\"}' FROM peers"
    )
    con.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


# Transient-lock retry budget. sqlite's busy_timeout only waits while it is
# willing to block; a write lock that outlives it (WAL checkpoint stall, boot
# thrash at load 388) surfaces as "database is locked" and must be retried at
# the app level. Exponential backoff with a cap + jitter rides multi-second
# stalls while staying BOUNDED, so a genuinely permanent lock still fails
# closed. The original 8-attempt linear schedule (~1.08s total) was too shallow
# and killed parked `claudebus wait` listeners 4+ times in one day (2026-07-23).
# Module-level so tests can shrink the budget to exercise the fail-closed path
# quickly. Full budget ~= sum(min(_CAP, _BASE*2**n) for n in range(_ATTEMPTS)).
_LOCK_RETRY_ATTEMPTS = 12
_LOCK_RETRY_BASE = 0.05
_LOCK_RETRY_CAP = 1.0


def _is_lock_error(exc):
    s = str(exc).lower()
    return "locked" in s or "busy" in s


def _lock_backoff_sleep(attempt):
    """Sleep before the next transient-lock retry: exponential with a cap, plus
    up to 25% jitter to decorrelate many sessions retrying the same lock at
    once (the multi-session thundering herd that caused the stalls)."""
    delay = min(_LOCK_RETRY_CAP, _LOCK_RETRY_BASE * (2 ** attempt))
    time.sleep(delay + random.uniform(0.0, delay * 0.25))


def db():
    """Open a connection, retrying on SQLITE_PROTOCOL/locked. busy_timeout only
    covers BUSY/LOCKED, NOT PROTOCOL, so we retry at the app level (per SQLite's
    own guidance). journal_mode/DDL are only touched when actually needed, to
    avoid taking locks on the hot path under many concurrent tabs."""
    ensure()
    last = None
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        con = None
        try:
            con = sqlite3.connect(DB, timeout=10, isolation_level=None)  # autocommit; we BEGIN explicitly
            con.execute("PRAGMA busy_timeout=5000")
            if (con.execute("PRAGMA journal_mode").fetchone()[0] or "").lower() != "wal":
                con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA temp_store=MEMORY")
            con.execute("PRAGMA wal_autocheckpoint=4000")
            if con.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
                _migrate(con)
            return con
        except sqlite3.OperationalError as e:
            last = e
            if con is not None:
                try:
                    con.close()
                except sqlite3.Error:
                    pass
            _lock_backoff_sleep(attempt)
    raise last


# ---------------- provider endpoints / evidence journal ----------------

def record_event(con, event_type, actor=None, subject=None, provider=None,
                 corr=None, causal_id=None, idempotency_key=None,
                 status="observed", payload=None):
    """Append a typed, replayable bus event.

    Delivery across hooks, tmux and app-server is deliberately at-least-once;
    callers that can retry should supply an idempotency key. A duplicate key
    returns the original row instead of inventing a second lifecycle event.
    """
    body = None if payload is None else json.dumps(payload, sort_keys=True)
    try:
        cur = con.execute(
            "INSERT INTO events(ts,type,actor,subject,provider,corr,causal_id,"
            "idempotency_key,status,payload) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (time.time(), event_type, actor, subject, provider, corr, causal_id,
             idempotency_key, status, body),
        )
        return cur.lastrowid
    except sqlite3.IntegrityError:
        if not idempotency_key:
            raise
        row = con.execute(
            "SELECT id FROM events WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        return row[0] if row else None


def upsert_endpoint(con, name, provider, native_session_id=None,
                    native_thread_id=None, parent=None, transport=None,
                    cwd=None, model=None, permission_profile=None,
                    state="joined", metadata=None):
    now = time.time()
    body = None if metadata is None else json.dumps(metadata, sort_keys=True)
    own_tx = not con.in_transaction

    native_key = native_thread_id if provider == "codex" else native_session_id
    native_column = "native_thread_id" if provider == "codex" else "native_session_id"

    def identity_authorizes():
        if not native_key:
            return False
        row = con.execute(
            "SELECT name,provider FROM identities WHERE session_id=?",
            (native_key,),
        ).fetchone()
        return bool(row and row[0] == name and row[1] == provider)

    if own_tx:
        con.execute("BEGIN IMMEDIATE")
    try:
        existing = con.execute(
            "SELECT provider,native_session_id,native_thread_id FROM endpoints "
            "WHERE name=?",
            (name,),
        ).fetchone()
        if existing:
            old_provider, old_session_id, old_thread_id = existing
            old_key = old_thread_id if old_provider == "codex" else old_session_id
            provider_change = old_provider != provider
            session_change = bool(old_key and native_key and old_key != native_key)
            identity_downgrade = bool(old_key and not native_key)
            if (provider_change or session_change or identity_downgrade) \
                    and not identity_authorizes():
                raise ValueError(
                    f"endpoint '{name}' is owned by {old_provider}/{old_key or '-'}"
                )

        if native_key:
            other = con.execute(
                f"SELECT name FROM endpoints WHERE provider=? AND {native_column}=? "
                "AND name<>?",
                (provider, native_key, name),
            ).fetchone()
            if other:
                # A provider-native identity can move only when the allocator's
                # authoritative binding already points at the new name.
                if not identity_authorizes():
                    raise ValueError(
                        f"native endpoint {provider}/{native_key} is owned by "
                        f"'{other[0]}'"
                    )
                con.execute(
                    f"DELETE FROM endpoints WHERE provider=? AND {native_column}=? "
                    "AND name=?",
                    (provider, native_key, other[0]),
                )

        con.execute(
            "INSERT INTO endpoints(name,provider,native_session_id,native_thread_id,"
            "parent,transport,cwd,model,permission_profile,state,last_seen,metadata) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(name) DO UPDATE SET provider=excluded.provider, "
            "native_session_id=COALESCE(excluded.native_session_id,endpoints.native_session_id), "
            "native_thread_id=COALESCE(excluded.native_thread_id,endpoints.native_thread_id), "
            "parent=COALESCE(excluded.parent,endpoints.parent), "
            "transport=COALESCE(excluded.transport,endpoints.transport), "
            "cwd=COALESCE(excluded.cwd,endpoints.cwd), "
            "model=COALESCE(excluded.model,endpoints.model), "
            "permission_profile=COALESCE(excluded.permission_profile,endpoints.permission_profile), "
            "state=excluded.state,last_seen=excluded.last_seen, "
            "metadata=COALESCE(excluded.metadata,endpoints.metadata)",
            (name, provider, native_session_id, native_thread_id, parent, transport,
             cwd, model, permission_profile, state, now, body),
        )
        if own_tx:
            con.execute("COMMIT")
    except Exception:
        if own_tx and con.in_transaction:
            con.execute("ROLLBACK")
        raise


def endpoint_state(con, name, state, **metadata):
    row = con.execute(
        "SELECT provider,native_session_id,native_thread_id,parent,transport,cwd,"
        "model,permission_profile FROM endpoints WHERE name=?", (name,),
    ).fetchone()
    if not row:
        return
    upsert_endpoint(con, name, row[0], row[1], row[2], row[3], row[4],
                    row[5], row[6], row[7], state, metadata or None)


# ---------------- identity / liveness ----------------

def _provider_session_id(args=None):
    """Return the current provider-native session/thread id, if one exists."""
    explicit = getattr(args, "session", None) if args is not None else None
    if explicit:
        return explicit
    provider = os.environ.get("CLAUDEBUS_PROVIDER")
    if provider == "codex":
        return os.environ.get("CODEX_THREAD_ID") or os.environ.get(
            "CLAUDE_CODE_SESSION_ID")
    claude_sid = os.environ.get("CLAUDE_CODE_SESSION_ID")
    if claude_sid:
        return claude_sid
    # A manually launched Codex shell may not carry CLAUDEBUS_PROVIDER. Only
    # infer Codex when no explicit bus identity is present; otherwise child
    # processes must not collapse onto an inherited parent CODEX_THREAD_ID.
    if provider is None and not os.environ.get("CLAUDEBUS_ID"):
        return os.environ.get("CODEX_THREAD_ID")
    return None


_AUTOGEN_ID = False   # whoami fell through to the tab-<ppid> default
_OBSERVER_CMD = False  # main() marked this invocation as read-only


def whoami(args):
    """--as > provider-native binding > $CLAUDEBUS_ID > tab-<ppid>.
    The binding (made by `hello`) outranks the env var because inherited env
    lies: tmux/wrapper layers hand sessions stale CLAUDEBUS_IDs, while
    session_id is unique per Claude session by construction."""
    global _AUTOGEN_ID
    if getattr(args, "as_", None):
        return args.as_
    sid = _provider_session_id(args)
    if sid:
        bound = _bound_name(sid)
        if bound:
            return bound
        # Losing-side recovery: if an explicitly dead owner was reclaimed, the
        # old process must not keep emitting under its cached/env identity. It
        # atomically rejoins under the next free suffix on its very next call.
        con = db()
        try:
            owner_pid = os.environ.get("CLAUDEBUS_OWNER_PID")
            return _alloc_name(
                con, sid, os.environ.get("CLAUDEBUS_ID"),
                owner_pid=int(owner_pid) if owner_pid else None,
                provider=os.environ.get("CLAUDEBUS_PROVIDER", "claude"),
            )
        finally:
            con.close()
    envid = os.environ.get("CLAUDEBUS_ID")
    if envid:
        return envid
    _AUTOGEN_ID = True
    return f"tab-{os.getppid()}"


_BOUND = {}  # session_id -> name cache for this process


def _bound_name(sid):
    # Validate against SQLite every time. The old permanent process cache let a
    # displaced session continue speaking as a name that now belonged to a
    # different session, extending one allocation race into a long collision.
    name = None
    try:
        con = db()
        row = con.execute("SELECT name FROM identities WHERE session_id=?", (sid,)).fetchone()
        con.close()
        name = row[0] if row else None
    except sqlite3.Error:
        name = None
    if name:
        _BOUND[sid] = name
    else:
        _BOUND.pop(sid, None)
    return name


# auto-identity pool for sessions that arrive with no name at all (desktop app,
# IDE extension, direct binary launch - anything outside the zshrc wrapper)
NAME_POOL = ("agni", "vayu", "soma", "mitra", "varuna", "usha", "tara", "ravi",
             "chandra", "bhumi", "akash", "marut", "ashvin", "yami", "ila", "vach")


def _alloc_name(con, sid, want, owner_pid=None, provider="claude"):
    """Bind session_id -> bus name, idempotently. `want` (usually the env
    CLAUDEBUS_ID) is honored unless another binding owns it. A binding is
    reclaimed only when it records an owner PID and that PID is provably dead;
    heartbeat gaps alone never transfer ownership. Allocation is serialized and
    uses plain INSERT so a uniqueness conflict advances to a suffix instead of
    REPLACE silently evicting the losing session."""
    now = time.time()
    con.execute("BEGIN IMMEDIATE")
    try:
        row = con.execute(
            "SELECT name FROM identities WHERE session_id=?", (sid,),
        ).fetchone()
        if row:
            con.execute(
                "UPDATE identities SET owner_pid=COALESCE(?,owner_pid), "
                "provider=?,updated=? WHERE session_id=?",
                (owner_pid, provider, now, sid),
            )
            con.execute("COMMIT")
            _BOUND[sid] = row[0]
            return row[0]

        base = want or NAME_POOL[sum(sid.encode()) % len(NAME_POOL)]
        candidates = [base] + [f"{base}-{i}" for i in range(2, 100)]
        candidates.append(f"tab-{uuid.uuid4().hex[:8]}")
        name = None
        for cand in candidates:
            bound = con.execute(
                "SELECT session_id,owner_pid FROM identities WHERE name=?",
                (cand,),
            ).fetchone()
            if bound:
                other_sid, other_pid = bound
                if other_pid and not pid_alive(other_pid):
                    con.execute(
                        "DELETE FROM identities WHERE name=? AND session_id=? "
                        "AND owner_pid=?",
                        (cand, other_sid, other_pid),
                    )
                else:
                    continue
            else:
                endpoint = con.execute(
                    "SELECT provider,native_session_id,native_thread_id,metadata "
                    "FROM endpoints "
                    "WHERE name=?", (cand,),
                ).fetchone()
                provisional_endpoint = False
                if endpoint:
                    try:
                        endpoint_meta = json.loads(endpoint[3]) if endpoint[3] else {}
                    except (TypeError, json.JSONDecodeError):
                        endpoint_meta = {}
                    endpoint_key = endpoint[2] if provider == "codex" else endpoint[1]
                    provisional_endpoint = bool(
                        owner_pid and endpoint[0] == provider and not endpoint_key
                        and endpoint_meta.get("pid") == owner_pid
                    )
                    if not provisional_endpoint:
                        continue
                # Preserve a live pre-v3/env-only identity even though it has
                # no session binding to protect it with the UNIQUE constraint.
                if not provisional_endpoint:
                    peer = con.execute(
                        "SELECT pid,last_seen FROM peers WHERE name=?", (cand,),
                    ).fetchone()
                    if peer and peer_active(peer[0], peer[1], now):
                        continue
            try:
                con.execute(
                    "INSERT INTO identities(session_id,name,created,owner_pid,"
                    "provider,updated) VALUES(?,?,?,?,?,?)",
                    (sid, cand, now, owner_pid, provider, now),
                )
                name = cand
                break
            except sqlite3.IntegrityError:
                continue  # a concurrent allocator won; advance, never replace
        if name is None:
            raise RuntimeError("unable to allocate a unique bus identity")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    _BOUND[sid] = name
    return name


def my_channels(me, args):
    chans = {"all", f"@{me}"}
    env = os.environ.get("CLAUDEBUS_CHANNELS", "")
    for c in env.split(","):
        if c.strip():
            chans.add(c.strip())
    for c in (getattr(args, "sub", None) or "").split(","):
        if c.strip():
            chans.add(c.strip())
    return chans


def chan_match(ch, chans):
    """MQTT/NATS-style topic subscriptions: a subscribed name containing a
    glob metachar (* ? [) matches channels via fnmatch; plain names stay
    exact-match, so pre-V4 subscriptions behave identically."""
    if ch in chans:
        return True
    for pat in chans:
        if ("*" in pat or "?" in pat or "[" in pat) and fnmatch.fnmatchcase(ch, pat):
            return True
    return False


def pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False


def peer_active(pid, seen, now):
    """A tab runs only short-lived claudebus calls, so its stored pid is usually
    already gone - recent heartbeat is the real liveness signal; a still-live pid
    (e.g. a parked `watch`) is a positive bonus, never used as a death signal."""
    return pid_alive(pid) or (now - (seen or 0)) < LIVE_WINDOW


def heartbeat(con, me):
    # Observer discipline (2026-07-24): a read-only command running under an
    # auto-minted tab-<ppid> identity must not register presence — dashboards
    # polling `peek` every 15s were minting ~5,700 ghost peers/day. Probes
    # still drive the time-gated GC sweep so a quiet bus self-cleans.
    if _OBSERVER_CMD and _AUTOGEN_ID:
        maybe_prune(con)
        return
    tty = ""
    try:
        tty = os.ttyname(2)
    except (OSError, AttributeError):
        # AttributeError: os.ttyname does not exist on Windows. Presence
        # tracking works fine without a tty name; it is display-only.
        pass
    # busy_timeout only helps while sqlite is willing to wait; a write lock
    # outliving it (WAL checkpoint stall, boot thrash) raised straight through
    # here and killed parked listeners at arm time. The upsert is idempotent
    # and every caller runs it in autocommit, so retry at the app level. The
    # budget must ride out a multi-second checkpoint stall (load-388 boot
    # thrash held locks >1s four times on 2026-07-23), hence the shared capped
    # exponential backoff (_lock_backoff_sleep), same schedule db() uses.
    last = None
    for attempt in range(_LOCK_RETRY_ATTEMPTS):
        try:
            con.execute(
                "INSERT INTO peers(name,pid,tty,last_seen) VALUES(?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET pid=excluded.pid, tty=excluded.tty, "
                "last_seen=excluded.last_seen",
                (me, os.getpid(), tty, time.time()),
            )
            maybe_prune(con)
            return
        except sqlite3.OperationalError as e:
            if not _is_lock_error(e):
                raise
            last = e
            _lock_backoff_sleep(attempt)
    raise last


# ---------------- pub/sub cursors (atomic) ----------------

def head_id(con):
    row = con.execute("SELECT COALESCE(MAX(id),0) FROM messages").fetchone()
    return row[0]


def ensure_cursor(con, me, since):
    """Register a consumer's read cursors on first contact. Two watermarks:
    last_id (broadcast/topic) starts at HEAD so a new tab is never flooded with
    backlog; dm_id (directed @me) starts at 0 so messages addressed to me are
    never missed, even if sent just before I joined. `since`='all' -> replay all;
    int N -> last N."""
    row = con.execute("SELECT last_id,dm_id FROM cursors WHERE consumer=?", (me,)).fetchone()
    if row is not None and since is None:
        return
    h = head_id(con)
    if since == "all":
        b = dm = 0
    elif since is not None:
        try:
            b = dm = max(0, h - int(since))
        except ValueError:
            b = dm = h
    elif row is None:
        b, dm = h, 0                  # new: skip broadcast backlog, keep pending DMs
    else:
        return
    con.execute(
        "INSERT INTO cursors(consumer,last_id,dm_id,updated) VALUES(?,?,?,?) "
        "ON CONFLICT(consumer) DO UPDATE SET last_id=excluded.last_id, "
        "dm_id=excluded.dm_id, updated=excluded.updated",
        (me, b, dm, time.time()),
    )


PRESENCE_BODIES = frozenset({"joined the bus", "left the bus"})


def is_presence(row):
    """Presence chatter (default join/leave announces) is bus plumbing: it stays
    visible in peek/ps/stats but is never DELIVERED - delivering it woke every
    idle listener (one wasted model turn per session start/end) and spammed
    every turn-start/mid-turn injection. Rows carry kind='presence'; the body
    match also catches legacy rows written before that kind existed."""
    kind = row[5] if len(row) > 5 else "msg"
    if kind == "presence":
        return True
    return row[3] == "all" and row[4] in PRESENCE_BODIES


def deliver(con, me, chans):
    """Atomically return my new messages and advance both watermarks past
    everything scanned. BEGIN IMMEDIATE serialises concurrent same-consumer
    reads, so no message is double-delivered or dropped."""
    me_chan = f"@{me}"
    con.execute("BEGIN IMMEDIATE")
    try:
        row = con.execute("SELECT last_id,dm_id FROM cursors WHERE consumer=?", (me,)).fetchone()
        cur, dmcur = row if row else (0, 0)
        lo = min(cur, dmcur)
        rows = con.execute(
            "SELECT id,ts,sender,channel,body,kind,corr,reply_to FROM messages "
            "WHERE id>? ORDER BY id", (lo,),
        ).fetchall()
        if not rows:
            con.execute("COMMIT")
            return []
        maxid = rows[-1][0]
        mine = []
        for r in rows:
            ch = r[3]
            if r[2] == me and ch != me_chan:
                continue          # never echo my own broadcasts back to me
            if is_presence(r):
                continue          # plumbing: never deliver, never wake
            if ch == me_chan:
                if r[0] > dmcur:
                    mine.append(r)
            elif r[0] > cur and chan_match(ch, chans):
                mine.append(r)
        con.execute(
            "INSERT INTO cursors(consumer,last_id,dm_id,updated) VALUES(?,?,?,?) "
            "ON CONFLICT(consumer) DO UPDATE SET last_id=excluded.last_id, "
            "dm_id=excluded.dm_id, updated=excluded.updated",
            (me, maxid, maxid, time.time()),
        )
        ep = con.execute(
            "SELECT provider FROM endpoints WHERE name=?", (me,),
        ).fetchone()
        for delivered in mine:
            record_event(
                con, "message_delivered", actor=me, subject=delivered[3],
                provider=ep[0] if ep else None, corr=delivered[6],
                idempotency_key=f"delivered:{me}:{delivered[0]}",
                payload={"message_id": delivered[0], "sender": delivered[2],
                         "kind": delivered[5]},
            )
        con.execute("COMMIT")
        return mine
    except Exception:
        con.execute("ROLLBACK")
        raise


def stage_delivery(con, me, chans):
    """Peek the same bounded delivery set as :func:`deliver` without moving
    cursors. Command hooks use this two-phase path and acknowledge only after
    their JSON has been flushed to Codex, so a killed/broken hook repeats data
    instead of losing it. Returns ``(rows, max_scanned_id)``."""
    me_chan = f"@{me}"
    con.execute("BEGIN IMMEDIATE")
    try:
        row = con.execute(
            "SELECT last_id,dm_id FROM cursors WHERE consumer=?", (me,),
        ).fetchone()
        cur, dmcur = row if row else (0, 0)
        lo = min(cur, dmcur)
        rows = con.execute(
            "SELECT id,ts,sender,channel,body,kind,corr,reply_to FROM messages "
            "WHERE id>? ORDER BY id", (lo,),
        ).fetchall()
        maxid = rows[-1][0] if rows else lo
        mine = []
        for item in rows:
            channel = item[3]
            if item[2] == me and channel != me_chan:
                continue
            if is_presence(item):
                continue
            if channel == me_chan:
                if item[0] > dmcur:
                    mine.append(item)
            elif item[0] > cur and chan_match(channel, chans):
                mine.append(item)
        con.execute("COMMIT")
        return mine, maxid
    except Exception:
        con.execute("ROLLBACK")
        raise


def acknowledge_delivery(con, me, maxid, rows=(), transport="staged"):
    """Advance a staged hook delivery after its output reached Codex stdout."""
    con.execute("BEGIN IMMEDIATE")
    try:
        current = con.execute(
            "SELECT last_id,dm_id FROM cursors WHERE consumer=?", (me,),
        ).fetchone() or (0, 0)
        through = max(int(maxid), current[0], current[1])
        con.execute(
            "INSERT INTO cursors(consumer,last_id,dm_id,updated) VALUES(?,?,?,?) "
            "ON CONFLICT(consumer) DO UPDATE SET last_id=MAX(last_id,excluded.last_id), "
            "dm_id=MAX(dm_id,excluded.dm_id),updated=excluded.updated",
            (me, through, through, time.time()),
        )
        endpoint = con.execute(
            "SELECT provider FROM endpoints WHERE name=?", (me,),
        ).fetchone()
        for item in rows:
            record_event(
                con, "message_delivered", actor=me, subject=item[3],
                provider=endpoint[0] if endpoint else None, corr=item[6],
                idempotency_key=f"delivered:{me}:{item[0]}",
                payload={"message_id": item[0], "sender": item[2],
                         "kind": item[5], "transport": transport},
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def fmt_digest(rows, max_rows=10, max_body=240):
    """Token-diet variant of fmt(as_json=True) for HOOK deliveries only: cap the
    row count (newest kept, older collapsed to a count) and truncate long bodies.
    Full text is never lost - `claudebus peek -n N` retrieves any message verbatim.
    The interactive CLI (recv/peek) keeps full fmt()."""
    out = []
    shown = rows
    if len(rows) > max_rows:
        shown = rows[-max_rows:]
        out.append(json.dumps({"digest": f"{len(rows) - max_rows} earlier messages "
                               f"omitted - `claudebus peek -n {len(rows)}` for full history"}))
    for r in shown:
        body = r[4]
        if len(body) > max_body:
            body = body[:max_body] + f"...[+{len(r[4]) - max_body} chars; claudebus peek]"
        d = {"id": r[0], "ts": r[1], "from": r[2], "channel": r[3], "body": body}
        kind = r[5] if len(r) > 5 else "msg"
        if kind and kind != "msg":
            d["kind"] = kind
        if len(r) > 6 and r[6]:
            d["corr"] = r[6]
        out.append(json.dumps(d))
    return "\n".join(out)


def fmt(rows, as_json=False):
    if as_json:
        out = []
        for r in rows:
            d = {"id": r[0], "ts": r[1], "from": r[2], "channel": r[3], "body": r[4]}
            kind = r[5] if len(r) > 5 else "msg"
            if kind and kind != "msg":
                d["kind"] = kind
            if len(r) > 6 and r[6]:
                d["corr"] = r[6]
            if len(r) > 7 and r[7]:
                d["reply_to"] = r[7]
            out.append(json.dumps(d))
        return "\n".join(out)
    out = []
    for r in rows:
        _id, ts, sender, chan, body = r[0], r[1], r[2], r[3], r[4]
        kind = r[5] if len(r) > 5 else "msg"
        t = time.strftime("%H:%M:%S", time.localtime(ts))
        tag = "" if chan == "all" else f" {color('['+chan+']','mg')}"
        if kind == "req":
            corr = r[6] if len(r) > 6 else ""
            tag += f" {color('[ask '+(corr or '')[:8]+']','yl')}"
        elif kind == "reply":
            tag += f" {color('[reply]','gn')}"
        elif kind in ("cfp", "bid", "award"):
            corr = r[6] if len(r) > 6 else ""
            tag += f" {color('['+kind+' '+(corr or '')[:12]+']','yl')}"
        out.append(f"{color(t,'dim')} {color(sender,'cy')}{tag}: {body}")
    return "\n".join(out)


# ---------------- FIFO push (hardened) ----------------

def poke_waiters():
    try:
        files = os.listdir(WAITERS)
    except FileNotFoundError:
        return
    for f in files:
        if not f.endswith(".fifo"):
            continue
        p = os.path.join(WAITERS, f)
        try:
            fd = os.open(p, os.O_WRONLY | os.O_NONBLOCK)
            try:
                os.write(fd, b"\x01")
            finally:
                os.close(fd)
        except OSError as e:
            if e.errno in (errno.ENXIO, errno.ENOENT):
                try:
                    os.unlink(p)  # no live reader -> stale, reap it
                except OSError:
                    pass
            # EAGAIN (pipe full) / EPIPE (reader vanished mid-write): the waiter
            # re-checks the DB on its next wake anyway, so dropping a poke is safe.


# ---------------- prune / GC ----------------

def prune(con):
    now = time.time()
    con.execute("DELETE FROM messages WHERE ts < ?", (now - MSG_TTL,))
    con.execute("DELETE FROM tasks WHERE done=1 AND ts < ?", (now - TASK_TTL,))
    con.execute("DELETE FROM blackboard WHERE ttl>0 AND ts+ttl < ?", (now,))
    # dead-lettered tasks linger longer than done ones (they need a human look)
    con.execute("DELETE FROM tasks WHERE dead=1 AND ts < ?", (now - MSG_TTL,))
    # per-class peer TTL: anonymous tab-* identities are single-shot CLI calls,
    # not sessions — reap them fast; named peers get the full window
    for name, pid, seen in con.execute("SELECT name,pid,last_seen FROM peers").fetchall():
        ttl = TAB_TTL if name.startswith("tab-") else PEER_TTL
        if (now - (seen or 0)) > ttl and not pid_alive(pid):
            con.execute("DELETE FROM peers WHERE name=?", (name,))
    # spawn corpses: registry rows whose tmux session is gone and that are past
    # SPAWN_TTL (they stay respawnable/inspectable until then)
    try:
        old = con.execute(
            "SELECT name,tmux FROM spawns WHERE ts < ?", (now - SPAWN_TTL,)
        ).fetchall()
        if old:
            live = _tmux_live_sessions()
            for name, tmux in old:
                if tmux not in live:
                    con.execute("DELETE FROM spawns WHERE name=?", (name,))
    except Exception:
        pass  # a tmux probe failure must never break a bus command
    # cursors idle past MSG_TTL: every message their watermark could still
    # deliver has already been pruned, so dropping the row loses nothing —
    # a returning consumer re-registers and dm-delivery from 0 covers exactly
    # the still-retained (unread) window
    con.execute("DELETE FROM cursors WHERE updated < ?", (now - MSG_TTL,))


def maybe_prune(con):
    """Time-gated sweep: at most one prune per PRUNE_EVERY across all callers.
    Replaces the old probabilistic 1-in-25-of-sends trigger, which tied GC
    frequency to chat volume while junk creation scaled with probe volume."""
    now = time.time()
    try:
        row = con.execute(
            "SELECT value FROM prefs WHERE name='__bus__' AND key='last_prune'"
        ).fetchone()
        if row and now - float(row[0]) < PRUNE_EVERY:
            return
        con.execute(
            "INSERT INTO prefs(name,key,value,updated) VALUES('__bus__','last_prune',?,?) "
            "ON CONFLICT(name,key) DO UPDATE SET value=excluded.value, updated=excluded.updated",
            (str(now), now),
        )
        prune(con)
    except (sqlite3.Error, ValueError):
        pass


# ---------------- commands: messaging ----------------

def cmd_send(args):
    me = whoami(args)
    channel = "all"
    if args.to:
        channel = f"@{args.to}"
    elif args.channel:
        channel = args.channel
    body = args.message
    if body == "-" or body is None:
        body = sys.stdin.read().rstrip("\n")
    if len(body) > 64 * 1024:
        print(color("send: message too large (>64KB)", "rd"), file=sys.stderr)
        return 2
    con = db()
    heartbeat(con, me)
    cur = con.execute(
        "INSERT INTO messages(ts,sender,channel,body,kind) VALUES(?,?,?,?,'msg')",
        (time.time(), me, channel, body),
    )
    record_event(con, "message_sent", actor=me, subject=channel,
                 idempotency_key=f"message:{cur.lastrowid}",
                 payload={"message_id": cur.lastrowid, "kind": "msg"})
    maybe_prune(con)
    con.close()
    poke_waiters()
    print(color(f"sent -> {channel}", "gn"))


def cmd_recv(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    ensure_cursor(con, me, getattr(args, "since", None))
    rows = deliver(con, me, my_channels(me, args))
    con.close()
    if rows:
        print(fmt(rows, args.json))
    elif not args.json:
        print(color("(no new messages)", "dim"))


def cmd_peek(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    chans = my_channels(me, args)
    rows = con.execute(
        "SELECT id,ts,sender,channel,body,kind,corr,reply_to FROM messages ORDER BY id DESC LIMIT ?",
        (args.n,),
    ).fetchall()
    con.close()
    rows = [r for r in reversed(rows) if r[3] in chans]
    print(fmt(rows, args.json) if rows else color("(empty)", "dim"))


# ---------------- blocking listen (wait / watch) ----------------

# Windows has no mkfifo and its select() accepts only sockets, so the
# instant-poke path is POSIX-only. Listeners there degrade to polling: same
# delivery semantics, latency bounded by POLL_FALLBACK instead of microseconds.
POLL_FALLBACK = 1.0


def _open_fifo(me):
    ensure()
    if not hasattr(os, "mkfifo"):
        return None, None
    fifo = os.path.join(WAITERS, f"{me}.{os.getpid()}.fifo")
    if not os.path.exists(fifo):
        os.mkfifo(fifo, 0o600)
    fd = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)  # self-open so it's always openable
    return fifo, fd


def _block(fd, deadline):
    timeout = SELECT_CEIL
    if deadline is not None:
        timeout = min(SELECT_CEIL, max(0.0, deadline - time.time()))
    if fd is None:               # no FIFO on this platform: poll instead
        time.sleep(min(POLL_FALLBACK, timeout))
        return
    r, _, _ = select.select([fd], [], [], timeout)
    if r:
        try:
            os.read(fd, 4096)
        except OSError:
            pass


def _listen(args, once):
    me = whoami(args)
    chans = my_channels(me, args)
    deadline = time.time() + args.timeout if args.timeout else None
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
    con = db()
    ensure_cursor(con, me, getattr(args, "since", None))
    con.close()
    try:
        fifo, fd = _open_fifo(me)
    except OSError as e:
        print(f"listen: {e}", file=sys.stderr)
        return 2
    got = False
    if not once and not args.json:
        print(color(f"watching bus as {me}  (channels: {', '.join(sorted(chans))})", "dim"))
    try:
        while True:
            con = db()
            heartbeat(con, me)
            rows = deliver(con, me, chans)
            con.close()
            if rows:
                print(fmt(rows, args.json)); sys.stdout.flush()
                got = True
                if once:
                    return 0
            if deadline is not None and time.time() >= deadline:
                if once and not got and not args.json:
                    print(color("(timeout)", "dim"))
                return 0 if got else 1
            _block(fd, deadline)
    except KeyboardInterrupt:
        return 0
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if fifo is not None:
            try:
                os.unlink(fifo)
            except OSError:
                pass


def cmd_wait(args):
    return _listen(args, once=True)


def cmd_watch(args):
    return _listen(args, once=False)


# ---------------- lanes / locks ----------------

def _lane_free(row, now):
    """row=(holder,pid,ts,ttl). A lane is held by a tab *identity* (which outlives
    any single short-lived claudebus process), so freedom is TTL-based: a lane
    with a ttl auto-expires; ttl=0 is held until released/stolen. Use --steal or
    `release --force` to reclaim a stale no-ttl lane (its age shows in `lanes`)."""
    if row is None:
        return True
    holder, pid, ts, ttl = row
    if ttl and now > ts + ttl:
        return True
    return False


def cmd_claim(args):
    me = whoami(args)
    lane = args.lane
    now = time.time()
    con = db()
    heartbeat(con, me)
    con.execute("BEGIN IMMEDIATE")
    try:
        row = con.execute(
            "SELECT holder,pid,ts,ttl FROM lanes WHERE lane=?", (lane,)).fetchone()
        if row and row[0] != me and not args.steal and not _lane_free(row, now):
            con.execute("COMMIT"); con.close()
            print(color(f"lane '{lane}' held by {row[0]}", "rd"), file=sys.stderr)
            return 1
        con.execute(
            "INSERT INTO lanes(lane,holder,pid,ts,ttl,note) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(lane) DO UPDATE SET holder=excluded.holder, pid=excluded.pid, "
            "ts=excluded.ts, ttl=excluded.ttl, note=excluded.note",
            (lane, me, os.getpid(), now, args.ttl or 0, args.note),
        )
        record_event(con, "claim_acquired", actor=me, subject=lane,
                     payload={"ttl": args.ttl or 0, "note": args.note})
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK"); con.close(); raise
    con.close()
    poke_waiters()
    print(color(f"claimed '{lane}'" + (f" ttl={int(args.ttl)}s" if args.ttl else ""), "gn"))
    return 0


def cmd_release(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    if args.force:
        con.execute("DELETE FROM lanes WHERE lane=?", (args.lane,))
    else:
        con.execute("DELETE FROM lanes WHERE lane=? AND holder=?", (args.lane, me))
    record_event(con, "claim_released", actor=me, subject=args.lane,
                 payload={"forced": bool(args.force)})
    con.close()
    print(color(f"released '{args.lane}'", "gn"))
    return 0


def cmd_lanes(args):
    con = db()
    now = time.time()
    rows = con.execute(
        "SELECT lane,holder,pid,ts,ttl,note FROM lanes ORDER BY ts DESC").fetchall()
    con.close()
    live = []
    for lane, holder, pid, ts, ttl, note in rows:
        if _lane_free((holder, pid, ts, ttl), now):
            continue
        live.append((lane, holder, pid, ts, ttl, note))
    if args.json:
        print("\n".join(json.dumps({
            "lane": l, "holder": h, "pid": p, "age": round(now - ts, 1),
            "ttl": ttl, "expires_in": (round(ts + ttl - now, 1) if ttl else None),
            "note": n,
        }) for l, h, p, ts, ttl, n in live))
        return 0
    if not live:
        print(color("(no lanes held)", "dim")); return 0
    print(color(f"{'LANE':18} {'HOLDER':14} {'AGE':>6} {'TTL':>6}  NOTE", "b"))
    for l, h, p, ts, ttl, n in live:
        age = int(now - ts)
        age_s = f"{age}s" if age < 90 else f"{age//60}m"
        ttl_s = f"{int(ttl)}s" if ttl else "-"
        print(f"{l:18} {h:14} {age_s:>6} {ttl_s:>6}  {n or ''}")
    return 0


# ---------------- work-queue ----------------

def cmd_submit(args):
    me = whoami(args)
    body = args.task
    if body == "-" or body is None:
        body = sys.stdin.read().rstrip("\n")
    after = None
    if getattr(args, "after", None):
        try:
            after = ",".join(str(int(x)) for x in args.after.replace(" ", "").split(",") if x)
        except ValueError:
            print(color("submit: --after takes task ids, e.g. --after 3,4", "rd"),
                  file=sys.stderr)
            return 2
    con = db()
    heartbeat(con, me)
    cur = con.execute(
        "INSERT INTO tasks(ts,queue,body,submitter,prio,max_attempts,after_ids) "
        "VALUES(?,?,?,?,?,?,?)",
        (time.time(), args.queue, body, me, args.prio, args.max_attempts, after),
    )
    tid = cur.lastrowid
    record_event(con, "task_submitted", actor=me, subject=str(tid),
                 idempotency_key=f"task_submitted:{tid}",
                 payload={"queue": args.queue, "priority": args.prio,
                          "after": after})
    con.close()
    poke_waiters()
    if args.json:
        print(json.dumps({"id": tid, "queue": args.queue}))
    else:
        print(color(f"submitted #{tid} -> queue '{args.queue}'", "gn"))
    return 0


def _deps_met(con, after_ids):
    """Airflow-style gating: a task with `--after a,b` is only takeable once
    every listed task is done. A dead-lettered dependency keeps it blocked
    (visible in `tasks`) rather than silently running out of order."""
    ids = [i for i in (after_ids or "").split(",") if i]
    if not ids:
        return True
    q = ",".join("?" * len(ids))
    n = con.execute(f"SELECT COUNT(*) FROM tasks WHERE id IN ({q}) AND done=1",
                    ids).fetchone()[0]
    return n == len(ids)


def _dead_letter(con, tid, gen, submitter, body):
    """SQS DLQ semantics: park the task out of the take path and tell the
    submitter, so failures surface instead of looping forever."""
    con.execute("UPDATE tasks SET dead=1, lease_until=0 WHERE id=?", (tid,))
    if submitter:
        con.execute(
            "INSERT INTO messages(ts,sender,channel,body,kind) VALUES(?,?,?,?,'msg')",
            (time.time(), "claudebus", f"@{submitter}",
             f"task #{tid} dead-lettered after {gen} attempts: {body[:120]} "
             f"(`claudebus retry {tid}` to requeue)"))


def _take_one(con, me, queue, lease):
    """Claim the highest-priority ready task. Readiness now excludes: dead
    (DLQ), backoff-delayed (not_before), and dependency-blocked (--after)
    tasks; a lease-expired task past max_attempts dead-letters here instead
    of being handed out again (poison-message protection)."""
    now = time.time()
    con.execute("BEGIN IMMEDIATE")
    try:
        cands = con.execute(
            "SELECT id, gen, max_attempts, submitter, body, after_ids FROM tasks "
            "WHERE queue=? AND done=0 AND dead=0 AND lease_until<=? AND not_before<=? "
            "ORDER BY prio DESC, id", (queue, now, now)).fetchall()
        got = None
        for tid, gen, maxa, submitter, body, after in cands:
            if gen >= (maxa or 3):
                _dead_letter(con, tid, gen, submitter, body)
                continue
            if not _deps_met(con, after):
                continue
            got = con.execute(
                "UPDATE tasks SET claimed_by=?, lease_until=?, gen=gen+1 "
                "WHERE id=? RETURNING id, ts, body, gen",
                (me, now + lease, tid)).fetchone()
            break
        con.execute("COMMIT")
        return got
    except Exception:
        con.execute("ROLLBACK")
        raise


def cmd_fail(args):
    """Explicit nack (the missing half of take/done): requeue with exponential
    backoff, or dead-letter once attempts are exhausted (Forq/SQS pattern)."""
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    row = con.execute(
        "SELECT gen, max_attempts, submitter, body FROM tasks "
        "WHERE id=? AND claimed_by=? AND done=0 AND dead=0",
        (args.id, me)).fetchone()
    if not row:
        con.close()
        print(color(f"#{args.id}: not yours / done / already dead", "rd"), file=sys.stderr)
        return 1
    gen, maxa, submitter, body = row
    if gen >= (maxa or 3):
        _dead_letter(con, args.id, gen, submitter, body)
        con.close()
        poke_waiters()
        print(color(f"#{args.id} dead-lettered after {gen} attempts", "rd"))
        return 0
    delay = args.delay if args.delay is not None else min(600.0, 5.0 * (2 ** gen))
    con.execute("UPDATE tasks SET lease_until=0, not_before=? WHERE id=?",
                (time.time() + delay, args.id))
    con.close()
    poke_waiters()
    print(color(f"#{args.id} requeued (attempt {gen}/{maxa}, retry in {int(delay)}s)", "yl"))
    return 0


def cmd_retry(args):
    """Resurrect a dead-lettered (or stuck) task: reset attempts and backoff."""
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    cur = con.execute(
        "UPDATE tasks SET dead=0, gen=0, lease_until=0, not_before=0, claimed_by=NULL "
        "WHERE id=? AND done=0", (args.id,))
    n = cur.rowcount
    con.close()
    if n:
        poke_waiters()
        print(color(f"#{args.id} requeued fresh", "gn"))
        return 0
    print(color(f"#{args.id}: no such undone task", "rd"), file=sys.stderr)
    return 1


def cmd_take(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    row = _take_one(con, me, args.queue, args.lease)
    con.close()
    if not row:
        if not args.json:
            print(color("(queue empty)", "dim"))
        return 0
    tid, ts, body, gen = row
    con = db()
    record_event(con, "task_claimed", actor=me, subject=str(tid),
                 idempotency_key=f"task_claimed:{tid}:{gen}",
                 payload={"queue": args.queue, "attempt": gen})
    con.close()
    if args.json:
        print(json.dumps({"id": tid, "ts": ts, "body": body, "gen": gen, "queue": args.queue}))
    else:
        print(f"{color('#'+str(tid),'cy')} {body}")
    return 0


def cmd_done(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    cur = con.execute(
        "UPDATE tasks SET done=1 WHERE id=? AND claimed_by=? AND done=0",
        (args.id, me),
    )
    n = cur.rowcount
    if n:
        record_event(con, "task_completed", actor=me, subject=str(args.id),
                     idempotency_key=f"task_completed:{args.id}",
                     status="completed")
    con.close()
    if n:
        print(color(f"done #{args.id}", "gn")); return 0
    print(color(f"#{args.id}: not yours / already done / expired", "rd"), file=sys.stderr)
    return 1


def cmd_pipeline(args):
    """Celery-canvas-style chain: submit N steps in ONE atomic call, each
    gated on the previous via after_ids, so a sequential workflow enters the
    queue whole and workers drain it in order."""
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    now = time.time()
    ids = []
    con.execute("BEGIN IMMEDIATE")
    try:
        prev = None
        for step in args.steps:
            cur = con.execute(
                "INSERT INTO tasks(ts,queue,body,submitter,prio,max_attempts,after_ids) "
                "VALUES(?,?,?,?,?,?,?)",
                (now, args.queue, step, me, args.prio, args.max_attempts,
                 str(prev) if prev else None))
            prev = cur.lastrowid
            ids.append(prev)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        con.close()
        raise
    con.close()
    poke_waiters()
    if args.json:
        print(json.dumps({"queue": args.queue, "ids": ids}))
    else:
        print(color(f"pipeline of {len(ids)} -> queue '{args.queue}': "
                    + " -> ".join(f"#{i}" for i in ids), "gn"))
    return 0


def _task_state(con, now, row):
    i, ts, qn, b, s, cb, lu, d, prio, gen, maxa, nb, dead, after = row
    if d:
        return "done"
    if dead:
        return "dead"
    if lu > now:
        return "leased"
    if nb > now:
        return "delayed"
    if not _deps_met(con, after):
        return "blocked"
    return "ready"


def cmd_tasks(args):
    con = db()
    now = time.time()
    q = ("SELECT id,ts,queue,body,submitter,claimed_by,lease_until,done,"
         "prio,gen,max_attempts,not_before,dead,after_ids FROM tasks")
    where, params = [], []
    if args.queue:
        where.append("queue=?"); params.append(args.queue)
    if getattr(args, "dlq", False):
        where.append("dead=1 AND done=0")
    if where:
        q += " WHERE " + " AND ".join(where)
    q += " ORDER BY id"
    rows = con.execute(q, params).fetchall()
    if args.json:
        out = []
        for row in rows:
            i, ts, qn, b, s, cb, lu, d, prio, gen, maxa, nb, dead, after = row
            state = _task_state(con, now, row)
            out.append(json.dumps({
                "id": i, "queue": qn, "body": b, "submitter": s, "state": state,
                "claimed_by": cb if state == "leased" else None,
                "prio": prio, "attempts": gen, "max_attempts": maxa,
                "after": after or None,
            }))
        con.close()
        print("\n".join(out))
        return 0
    if not rows:
        con.close()
        print(color("(no tasks)", "dim")); return 0
    print(color(f"{'#':>4} {'QUEUE':10} {'STATE':8} {'WORKER':12} {'PRIO':>4} {'TRY':>5} BODY", "b"))
    for row in rows:
        i, ts, qn, b, s, cb, lu, d, prio, gen, maxa, nb, dead, after = row
        state = _task_state(con, now, row)
        worker = cb if state == "leased" else "-"
        print(f"{i:>4} {qn:10} {state:8} {(worker or '-'):12} {prio:>4} "
              f"{str(gen)+'/'+str(maxa):>5} {b[:44]}")
    con.close()
    return 0


# ---------------- request / reply (RPC) ----------------

def _await_reply(me, corr, start, timeout, as_json, whom):
    """Park on my FIFO until a reply correlated to `corr` lands (or timeout).
    Shared by `ask` and `spawn --wait`."""
    deadline = time.time() + timeout
    try:
        fifo, fd = _open_fifo(me)
    except OSError as e:
        print(f"await: {e}", file=sys.stderr); return 2
    try:
        while True:
            con = db()
            row = con.execute(
                "SELECT id,ts,sender,channel,body,kind,corr,reply_to FROM messages "
                "WHERE kind='reply' AND corr=? AND id>? ORDER BY id LIMIT 1",
                (corr, start),
            ).fetchone()
            con.close()
            if row:
                print(fmt([row], as_json))
                return 0
            if time.time() >= deadline:
                if not as_json:
                    print(color(f"(no reply from {whom} in {int(timeout)}s)", "rd"),
                          file=sys.stderr)
                return 1
            _block(fd, deadline)
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if fifo is not None:
            try:
                os.unlink(fifo)
            except OSError:
                pass


def cmd_ask(args):
    me = whoami(args)
    corr = uuid.uuid4().hex
    reply_to = f"@{me}"
    con = db()
    heartbeat(con, me)
    start = head_id(con)
    cur = con.execute(
        "INSERT INTO messages(ts,sender,channel,body,kind,corr,reply_to) "
        "VALUES(?,?,?,?,'req',?,?)",
        (time.time(), me, f"@{args.peer}", args.question, corr, reply_to),
    )
    record_event(con, "ask_sent", actor=me, subject=args.peer, corr=corr,
                 idempotency_key=f"message:{cur.lastrowid}",
                 payload={"message_id": cur.lastrowid})
    con.close()
    poke_waiters()
    return _await_reply(me, corr, start, args.timeout or 30, args.json, args.peer)


def cmd_reply(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    req = con.execute(
        "SELECT reply_to,sender FROM messages WHERE corr=? AND kind='req' ORDER BY id DESC LIMIT 1",
        (args.corr,),
    ).fetchone()
    if not req:
        con.close()
        print(color(f"no pending request with id {args.corr}", "rd"), file=sys.stderr)
        return 1
    reply_to = req[0] or f"@{req[1]}"
    body = args.answer
    if body == "-" or body is None:
        body = sys.stdin.read().rstrip("\n")
    cur = con.execute(
        "INSERT INTO messages(ts,sender,channel,body,kind,corr,reply_to) "
        "VALUES(?,?,?,?,'reply',?,?)",
        (time.time(), me, reply_to, body, args.corr, None),
    )
    record_event(con, "reply_sent", actor=me, subject=reply_to, corr=args.corr,
                 idempotency_key=f"message:{cur.lastrowid}",
                 payload={"message_id": cur.lastrowid})
    con.close()
    poke_waiters()
    print(color(f"replied -> {reply_to}", "gn"))
    return 0


# ---------------- spawn: full delegated Claude sessions ----------------
#
# `spawn` is the bus's lifecycle primitive: it starts a FULL interactive
# `claude` session (hooks, MCP, CLAUDE.md, bus identity - NOT a stripped
# `claude -p` child) in a detached tmux session, hands it a task wrapped with
# report-back instructions, and registers it so `tell`/`sessions`/`kill` can
# manage it. The child env is sanitized: CLAUDECODE / CLAUDE_CODE_* markers are
# stripped (else hook-join.sh refuses it as a headless child) and CLAUDEBUS_ID
# is set, so the SessionStart hook auto-joins it to the bus like any real tab.

CLAUDE_BIN = os.environ.get("CLAUDEBUS_CLAUDE_BIN", "claude")

# Agent profiles: the bus is agent-agnostic. Any CLI that can run shell
# commands can be a worker, because report-back is just `claudebus send` with
# CLAUDEBUS_ID set in its env. claude workers auto-join via hooks; codex/gemini
# have no hooks but register on their first bus call (heartbeat-on-send).
#
# SAFETY: claude delegates run INSIDE CapOS (agentshield + action gate), so the
# operator's bypassPermissions default applies to them. codex/gemini run OUTSIDE
# CapOS with no safety layer, so their profiles are SANDBOXED by default and the
# dangerous auto-approve flag (`--dangerously-bypass-approvals-and-sandbox` /
# `--yolo`) is armed ONLY when the operator passes `--perms bypassPermissions`
# explicitly. Each profile: env-overridable bin, argv builder (perms -> flags),
# and whether the CapOS model tiers (haiku/sonnet/opus) apply.
AGENTS = {
    "claude": {
        "bin_env": "CLAUDEBUS_CLAUDE_BIN", "bin": "claude", "tiered": True,
        "argv": lambda b, model, perms, prompt:
            [b, "--permission-mode", perms]
            + (["--model", model] if model != "default" else []) + [prompt],
    },
    "codex": {  # OpenAI Codex CLI (the ChatGPT-subscription GPT lane)
        "bin_env": "CLAUDEBUS_CODEX_BIN", "bin": "codex", "tiered": False,
        # bypass only on explicit opt-in; normal unattended workers use the
        # current explicit workspace sandbox + never-escalate policy. The old
        # --full-auto alias was removed by Codex CLI 0.144.1.
        "argv": lambda b, model, perms, prompt:
            [b] + (["--dangerously-bypass-approvals-and-sandbox"]
                   if perms == "bypassPermissions"
                   else ["--sandbox", "workspace-write",
                         "--ask-for-approval", "never"])
            + (["-m", model] if model != "default" else []) + [prompt],
    },
    "gemini": {  # Google Gemini CLI
        # --yolo only on explicit opt-in; default approval mode auto-edits files
        # but still gates shell/network actions.
        "bin_env": "CLAUDEBUS_GEMINI_BIN", "bin": "gemini", "tiered": False,
        "argv": lambda b, model, perms, prompt:
            [b] + (["--yolo"] if perms == "bypassPermissions"
                   else ["--approval-mode", "auto_edit"])
            + (["-m", model] if model != "default" else []) + ["-i", prompt],
    },
    "atlas": {  # ChatGPT Atlas agent mode, driven via AppleScript+JS (bridge).
        # No CLI/prompt: cmd_spawn runs atlas_bridge.py with the RAW task and the
        # bridge does the report-back itself. See atlas_bridge.py for why this is
        # the transport ceiling (no API/CDP/extension/native-messaging in Atlas).
        "bridge": True, "tiered": False, "bin_env": None, "bin": None,
    },
}
BUNDLE_DIR = os.path.dirname(os.path.realpath(__file__))
ATLAS_BRIDGE = os.path.join(BUNDLE_DIR, "atlas_bridge.py")
CODEX_WORKER = os.path.join(BUNDLE_DIR, "codex_worker.py")
# non-claude agents run outside CapOS: their default perms is SANDBOX, never the
# global claude bypass default. Explicit --perms bypassPermissions still arms it.
AGENT_SAFE_PERMS = "sandbox"
# spawned sessions run unattended with full permission bypass - the operator's
# explicit 2026-07-03 decision (chosen over an allowlist / manual approval when
# asked directly): a delegate that stalls on its first report-back is useless.
# Override per spawn with --perms, or globally via CLAUDEBUS_SPAWN_PERMS.
SPAWN_PERMS = os.environ.get("CLAUDEBUS_SPAWN_PERMS", "bypassPermissions")
SPAWN_MODEL = os.environ.get("CLAUDEBUS_SPAWN_MODEL", "auto")
# env the child must NOT inherit: session markers that would make the join hook
# refuse it, and bus identity/subs that belong to the parent tab.
ENV_STRIP = ("CLAUDECODE", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_ENTRYPOINT",
             "CLAUDE_CODE_SSE_PORT", "CODEX_THREAD_ID", "CLAUDEBUS_ID",
             "CLAUDEBUS_CHANNELS")


# ---------------- contract net (Smith 1980 / FIPA CNP) ----------------

def _cfp_row(con, corr):
    return con.execute(
        "SELECT sender, body, reply_to FROM messages WHERE kind='cfp' AND corr=? "
        "ORDER BY id DESC LIMIT 1", (corr,)).fetchone()


def _bids_for(con, corr):
    out = []
    for mid, ts, sender, body in con.execute(
            "SELECT id, ts, sender, body FROM messages WHERE kind='bid' AND corr=? "
            "ORDER BY id", (corr,)).fetchall():
        try:
            d = json.loads(body)
            score = float(d.get("score", 0))
            note = d.get("note")
        except (ValueError, TypeError):
            score, note = 0.0, body
        out.append({"id": mid, "ts": ts, "bidder": sender, "score": score, "note": note})
    return out


def _do_award(con, me, corr, to=None):
    """Pick the winner (highest score, earliest bid on ties - or --to override)
    and DM them the award. One award per cfp; only the issuer can award."""
    cfp = _cfp_row(con, corr)
    if not cfp:
        return None, f"no cfp with corr '{corr}'"
    issuer, task, _ = cfp
    if issuer != me:
        return None, f"cfp '{corr}' was issued by {issuer}, not you"
    if con.execute("SELECT 1 FROM messages WHERE kind='award' AND corr=? LIMIT 1",
                   (corr,)).fetchone():
        return None, f"cfp '{corr}' already awarded"
    bids = _bids_for(con, corr)
    if to:
        winner = to
    elif bids:
        winner = max(bids, key=lambda b: (b["score"], -b["id"]))["bidder"]
    else:
        return None, f"no bids on cfp '{corr}' (and no --to override)"
    con.execute(
        "INSERT INTO messages(ts,sender,channel,body,kind,corr,reply_to) "
        "VALUES(?,?,?,?,'award',?,?)",
        (time.time(), me, f"@{winner}", f"AWARD {corr}: {task}", corr, f"@{me}"))
    return winner, None


def cmd_cfp(args):
    """Announce a task for bids instead of assigning it blind. Peers see the
    cfp as a broadcast, respond with `bid <corr> <score>`; award manually or
    let --window auto-award the best bid after S seconds."""
    me = whoami(args)
    corr = uuid.uuid4().hex[:12]
    body = args.task
    if getattr(args, "cap", None):
        body += f" [needs cap: {args.cap}]"
    con = db()
    heartbeat(con, me)
    con.execute(
        "INSERT INTO messages(ts,sender,channel,body,kind,corr,reply_to) "
        "VALUES(?,?,?,?,'cfp',?,?)",
        (time.time(), me, "all", body, corr, f"@{me}"))
    con.close()
    poke_waiters()
    if not args.window:
        if args.json:
            print(json.dumps({"corr": corr, "task": args.task}))
        else:
            print(color(f"cfp {corr} broadcast - collect with `bids {corr}`, "
                        f"then `award {corr}`", "gn"))
        return 0
    time.sleep(args.window)   # CNP has a fixed bid deadline by design
    con = db()
    winner, err = _do_award(con, me, corr, None)
    con.close()
    if err:
        print(color(f"cfp {corr}: {err}", "rd"), file=sys.stderr)
        return 1
    poke_waiters()
    if args.json:
        print(json.dumps({"corr": corr, "winner": winner}))
    else:
        print(color(f"cfp {corr} awarded to {winner}", "gn"))
    return 0


def cmd_bid(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    cfp = _cfp_row(con, args.corr)
    if not cfp:
        con.close()
        print(color(f"bid: no cfp with corr '{args.corr}'", "rd"), file=sys.stderr)
        return 1
    issuer = cfp[0]
    con.execute(
        "INSERT INTO messages(ts,sender,channel,body,kind,corr) VALUES(?,?,?,?,'bid',?)",
        (time.time(), me, f"@{issuer}",
         json.dumps({"score": args.score, "note": args.note}), args.corr))
    con.close()
    poke_waiters()
    print(color(f"bid {args.score} on {args.corr} -> {issuer}", "gn"))
    return 0


def cmd_bids(args):
    con = db()
    bids = _bids_for(con, args.corr)
    con.close()
    if args.json:
        print("\n".join(json.dumps(b) for b in bids))
        return 0
    if not bids:
        print(color("(no bids)", "dim")); return 0
    for b in sorted(bids, key=lambda x: -x["score"]):
        print(f"{color(b['bidder'],'cy')} {b['score']:.2f}  {b['note'] or ''}")
    return 0


def cmd_award(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    winner, err = _do_award(con, me, args.corr, args.to)
    con.close()
    if err:
        print(color(f"award: {err}", "rd"), file=sys.stderr)
        return 1
    poke_waiters()
    if args.json:
        print(json.dumps({"corr": args.corr, "winner": winner}))
    else:
        print(color(f"awarded {args.corr} to {winner}", "gn"))
    return 0


# ---------------- blackboard (Linda tuple space / Hearsay-II) ----------------

def cmd_bb(args):
    """Shared versioned KV the whole bus can read/write: coordination state
    that peers poll (build status, plan step, config) instead of messages that
    wake them. `--if-ver` is optimistic compare-and-set: the write only lands
    if nobody moved the value since you read it."""
    me = whoami(args)
    now = time.time()
    con = db()
    heartbeat(con, me)
    op = args.op
    if op == "set":
        if not args.key or args.value is None:
            con.close()
            print(color("bb set <key> <value> [--ttl S] [--if-ver N]", "rd"), file=sys.stderr)
            return 2
        if args.if_ver is not None:
            cur = con.execute(
                "UPDATE blackboard SET value=?, author=?, ver=ver+1, ts=?, ttl=? "
                "WHERE key=? AND ver=?",
                (args.value, me, now, args.ttl or 0, args.key, args.if_ver))
            if cur.rowcount == 0:
                row = con.execute("SELECT ver FROM blackboard WHERE key=?",
                                  (args.key,)).fetchone()
                con.close()
                print(color(f"bb: version conflict on '{args.key}' "
                            f"(now v{row[0] if row else 'absent'}, you had v{args.if_ver})",
                            "rd"), file=sys.stderr)
                return 1
        else:
            con.execute(
                "INSERT INTO blackboard(key,value,author,ver,ts,ttl) VALUES(?,?,?,1,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
                "author=excluded.author, ver=blackboard.ver+1, ts=excluded.ts, "
                "ttl=excluded.ttl",
                (args.key, args.value, me, now, args.ttl or 0))
        con.close()
        print(color(f"bb set '{args.key}'", "gn"))
        return 0
    if op == "get":
        row = con.execute(
            "SELECT key,value,author,ver,ts,ttl FROM blackboard WHERE key=?",
            (args.key,)).fetchone()
        con.close()
        if not row or (row[5] and now > row[4] + row[5]):
            print(color(f"bb: no '{args.key}'", "rd"), file=sys.stderr)
            return 1
        if args.json:
            print(json.dumps({"key": row[0], "value": row[1], "author": row[2],
                              "ver": row[3], "age": round(now - row[4], 1),
                              "ttl": row[5] or None}))
        else:
            print(row[1])
        return 0
    if op == "del":
        con.execute("DELETE FROM blackboard WHERE key=?", (args.key,))
        con.close()
        print(color(f"bb del '{args.key}'", "gn"))
        return 0
    # list
    rows = con.execute(
        "SELECT key,value,author,ver,ts,ttl FROM blackboard ORDER BY key").fetchall()
    con.close()
    live = [r for r in rows
            if not (r[5] and now > r[4] + r[5])
            and (not args.prefix or r[0].startswith(args.prefix))]
    if args.json:
        print("\n".join(json.dumps({"key": k, "value": v, "author": a, "ver": ver,
                                    "age": round(now - ts, 1)})
                        for k, v, a, ver, ts, ttl in live))
        return 0
    if not live:
        print(color("(blackboard empty)", "dim")); return 0
    for k, v, a, ver, ts, ttl in live:
        print(f"{color(k,'cy')} = {v[:60]}  {color(f'(v{ver} by {a})','dim')}")
    return 0


# ---------------- capability discovery (A2A agent-card-lite) ----------------

def cmd_caps(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    if args.list is not None:
        val = None if args.list in ("clear", "none", "-") else args.list
        con.execute("UPDATE peers SET caps=? WHERE name=?", (val, me))
        con.close()
        print(color(f"caps[{me}] = {val or '(none)'}", "gn"))
        return 0
    now = time.time()
    rows = con.execute(
        "SELECT name,pid,last_seen,caps FROM peers WHERE caps IS NOT NULL AND caps!=''"
    ).fetchall()
    con.close()
    if args.json:
        print("\n".join(json.dumps({"name": n, "caps": c.split(","),
                                    "live": peer_active(p, s, now)})
                        for n, p, s, c in rows))
        return 0
    if not rows:
        print(color("(no peer has declared caps)", "dim")); return 0
    for n, p, s, c in rows:
        live = "live" if peer_active(p, s, now) else "gone"
        print(f"{color(n,'cy'):24} {c}  {color('('+live+')','dim')}")
    return 0


def cmd_discover(args):
    """Find live peers by capability, not name - route a cfp/ask/spawn to
    whoever can actually do the work."""
    con = db()
    now = time.time()
    rows = con.execute(
        "SELECT name,pid,last_seen,caps FROM peers WHERE caps IS NOT NULL AND caps!=''"
    ).fetchall()
    con.close()
    hits = [(n, c) for n, p, s, c in rows
            if peer_active(p, s, now)
            and args.cap in [x.strip() for x in c.split(",")]]
    if args.json:
        print("\n".join(json.dumps({"name": n, "caps": c.split(",")}) for n, c in hits))
    else:
        for n, c in hits:
            print(f"{color(n,'cy')}  {c}")
        if not hits:
            print(color(f"(no live peer with cap '{args.cap}')", "dim"))
    return 0 if hits else 1


# ---------------- leader election (Chubby-style lease) ----------------

def cmd_elect(args):
    """Lease-based election on the lanes machinery: first claimer of
    leader:<role> wins for --ttl seconds; the holder renews by re-running
    elect; expiry or `leave` opens the seat. No consensus protocol needed on
    one machine - SQLite's write lock IS the arbiter."""
    me = whoami(args)
    lane = f"leader:{args.role}"
    now = time.time()
    ttl = args.ttl or 120
    con = db()
    heartbeat(con, me)
    con.execute("BEGIN IMMEDIATE")
    try:
        row = con.execute("SELECT holder,pid,ts,ttl FROM lanes WHERE lane=?",
                          (lane,)).fetchone()
        if row and row[0] != me and not _lane_free(row, now):
            con.execute("COMMIT")
            con.close()
            if args.json:
                print(json.dumps({"role": args.role, "leader": row[0], "won": False}))
            else:
                print(color(f"'{args.role}' led by {row[0]}", "yl"))
            return 1
        con.execute(
            "INSERT INTO lanes(lane,holder,pid,ts,ttl,note) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(lane) DO UPDATE SET holder=excluded.holder, "
            "pid=excluded.pid, ts=excluded.ts, ttl=excluded.ttl, note=excluded.note",
            (lane, me, os.getpid(), now, ttl, f"leader of {args.role}"))
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        con.close()
        raise
    con.close()
    if args.json:
        print(json.dumps({"role": args.role, "leader": me, "won": True,
                          "lease_s": ttl}))
    else:
        print(color(f"you lead '{args.role}' for {int(ttl)}s (re-elect to renew)", "gn"))
    return 0


def cmd_leader(args):
    con = db()
    now = time.time()
    row = con.execute("SELECT holder,pid,ts,ttl FROM lanes WHERE lane=?",
                      (f"leader:{args.role}",)).fetchone()
    con.close()
    leader = None if (row is None or _lane_free(row, now)) else row[0]
    if args.json:
        print(json.dumps({"role": args.role, "leader": leader}))
    else:
        print(leader or color("(no leader)", "dim"))
    return 0


def _tmux(*args):
    cmd = ["tmux"]
    sock = os.environ.get("CLAUDEBUS_TMUX_SOCKET")
    if sock:
        cmd += ["-L", sock]
    return cmd + list(args)


def _tmux_live_sessions():
    try:
        # ``remain-on-exit`` intentionally retains crashed panes for forensic
        # inspection. Session existence alone therefore cannot prove that a
        # provider worker is alive; inspect pane_dead and accept a session only
        # when at least one of its panes still has a running process.
        p = subprocess.run(_tmux("list-panes", "-a", "-F",
                                 "#{session_name}\t#{pane_dead}"),
                           capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.TimeoutExpired):
        return set()
    if p.returncode != 0:
        return set()
    live = set()
    for line in p.stdout.splitlines():
        session, sep, dead = line.partition("\t")
        if sep and session.strip() and dead.strip() == "0":
            live.add(session.strip())
    return live


def _spawn_unsets():
    """Every CLAUDE-session marker to strip from the child: the known set plus
    anything CLAUDE_CODE_*-shaped in the current env (a parent Claude session
    exports several)."""
    keys = set(ENV_STRIP)
    for k in os.environ:
        if k == "CLAUDECODE" or k.startswith("CLAUDE_CODE_"):
            keys.add(k)
    return sorted(keys)


# CapOS 3-tier alignment (haiku mechanical / sonnet coding / opus hard-review;
# fable is lead-only and NEVER inherited into a fan-out). Opus wins over haiku
# when both match: mis-routing hard work down costs more than routing easy work up.
OPUS_RX = re.compile(
    r"architect|security|audit|threat.?model|red.?team|adjudicat|"
    r"design (?:review|the)|deep(?:ly)? review|hard review|prove|formal(?:ly)? verif",
    re.I)
HAIKU_RX = re.compile(
    r"extract|list(?: all| every)?\b|collect|inventory|count|rename|reformat|"
    r"convert|summari[sz]e|transcribe|grep|find all|status check|poll",
    re.I)


def _route_model(task):
    if OPUS_RX.search(task):
        return "opus"
    if HAIKU_RX.search(task) and len(task) < 500:
        return "haiku"
    return "sonnet"


def _resolve_model(requested, task):
    model = requested or "auto"
    if model == "auto":
        model = _route_model(task)
    if model.startswith("fable") or "fable" in model:
        model = "opus"   # fable = lead-only tier; a delegate never inherits it
    return model


def _live_siblings(con, parent, exclude):
    """Live delegates of the same parent, for introductions."""
    rows = con.execute(
        "SELECT name,tmux,task FROM spawns WHERE parent=? AND name!=? ORDER BY ts",
        (parent, exclude)).fetchall()
    live = _tmux_live_sessions()
    return [(n, task) for n, t, task in rows if t in live][:6]


def _wrap_task(name, parent, task, corr=None, siblings=(), agent="claude",
               automatic_report=False):
    kind = ("full Claude Code session" if agent == "claude"
            else f"full {agent} agent session")
    if automatic_report:
        deliver = ("the app-server transport reports your terminal result automatically; "
                   "do not send a duplicate completion message")
    elif corr:
        deliver = f'claudebus reply {corr} "DONE: <concise result>"'
    else:
        deliver = f'claudebus send "DONE: <concise result>" --to {parent}'
    lines = [
        f"[claudebus spawn] You are '{name}', a {kind} spawned "
        f"by tab '{parent}' as their delegate. Work the task below to completion.",
        "Coordinate over the bus (the `claudebus` CLI is on PATH):",
        f"- when finished, deliver the result: {deliver}",
        f'- progress or blockers: claudebus send "<update>" --to {parent}'
        f'  (a question that needs an answer: claudebus ask {parent} "<q>" --timeout 300)',
    ]
    if siblings:
        intro = ", ".join(f"{n} (\"{(t or '')[:40]}\")" for n, t in siblings)
        lines.append(
            f"- teammates working sibling tasks for the same parent: {intro}. "
            f'Talk to them directly: claudebus send "<msg>" --to <name> / '
            f'claudebus ask <name> "<q>". Use `claudebus claim <lane>` before '
            f"editing files a teammate may also touch.")
    lines.append(
        '- if the task naturally splits, spawn your own delegates: '
        'claudebus spawn "<subtask>" - you become their parent; '
        "`claudebus tree` shows the whole delegation tree.")
    lines += [
        "Stay running after finishing; follow-up instructions may arrive as new prompts.",
        "",
        "TASK:",
        task,
    ]
    return "\n".join(lines)


def _free_name(con):
    used = {r[0] for r in con.execute(
        "SELECT name FROM spawns UNION SELECT name FROM identities "
        "UNION SELECT name FROM endpoints"
    ).fetchall()}
    live = _tmux_live_sessions()
    n = 1
    while f"w{n}" in used or f"cb-w{n}" in live:
        n += 1
    return f"w{n}"


def cmd_spawn(args):
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    name = args.name or _free_name(con)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", name):
        con.close()
        print(color(f"spawn: bad name '{name}'", "rd"), file=sys.stderr)
        return 2
    sess = f"cb-{name}"
    task = args.task
    if task == "-" or task is None:
        task = sys.stdin.read().rstrip("\n")

    profile = AGENTS.get(args.agent)
    if profile is None:
        con.close()
        print(color(f"spawn: unknown agent '{args.agent}' "
                    f"(one of: {', '.join(AGENTS)})", "rd"), file=sys.stderr)
        return 2
    identity_owner = con.execute(
        "SELECT session_id,owner_pid,provider FROM identities WHERE name=?", (name,),
    ).fetchone()
    endpoint_owner = con.execute(
        "SELECT provider,state FROM endpoints WHERE name=?", (name,),
    ).fetchone()
    existing_spawn = con.execute(
        "SELECT parent,agent,tmux FROM spawns WHERE name=?", (name,),
    ).fetchone()
    spawn_owned = bool(
        existing_spawn and existing_spawn[0] == me and existing_spawn[1] == args.agent
    )
    dead_spawn_identity = bool(
        spawn_owned and identity_owner and identity_owner[1]
        and identity_owner[2] == args.agent and not pid_alive(identity_owner[1])
        and existing_spawn[2] not in _tmux_live_sessions()
    )
    if (identity_owner and not dead_spawn_identity) or (endpoint_owner and not existing_spawn):
        con.close()
        print(color(f"spawn: name '{name}' is reserved by an existing endpoint", "rd"),
              file=sys.stderr)
        return 2
    if existing_spawn and not spawn_owned:
        con.close()
        print(color(f"spawn: name '{name}' belongs to another spawn owner/provider", "rd"),
              file=sys.stderr)
        return 2
    corr = uuid.uuid4().hex if args.wait else None
    start = head_id(con)
    transport = getattr(args, "transport", "tmux")
    if transport == "app-server" and args.agent != "codex":
        con.close()
        print(color("spawn: --transport app-server is supported only for codex", "rd"),
              file=sys.stderr)
        return 2
    workdir = os.path.abspath(args.dir or os.getcwd())
    if profile.get("bridge"):
        # a bridge lane (atlas): no LLM prompt - the bridge gets the RAW task and
        # reports back itself. model/perms don't apply.
        model = "-"
        perms = "-"
        argv = [sys.executable, ATLAS_BRIDGE, "--name", name, "--parent", me,
                "--task", task, "--timeout", str(int(args.timeout or 600))]
        if corr:
            argv += ["--corr", corr]
    else:
        if profile["tiered"]:
            model = _resolve_model(args.model, task)   # CapOS tier routing
        else:
            model = args.model if args.model and args.model != "auto" else "default"
        # perms default is PER-AGENT: claude (inside CapOS) gets the operator's
        # SPAWN_PERMS; a non-claude agent (outside CapOS) gets the sandbox default.
        # An explicit --perms overrides either.
        if args.perms is not None:
            perms = args.perms
        elif profile["tiered"]:
            perms = SPAWN_PERMS
        else:
            perms = AGENT_SAFE_PERMS
        prompt = _wrap_task(name, me, task, corr,
                            siblings=_live_siblings(con, me, name),
                            agent=args.agent,
                            automatic_report=(transport == "app-server"))
        bin_ = os.environ.get(profile["bin_env"]) or profile["bin"]
        if transport == "app-server":
            sandbox = ("danger-full-access" if perms == "bypassPermissions"
                       else "read-only" if perms == "plan"
                       else "workspace-write")
            argv = [sys.executable, CODEX_WORKER,
                    "--name", name, "--parent", me,
                    "--task", prompt, "--cwd", workdir,
                    "--model", model, "--sandbox", sandbox,
                    "--approval-policy", "never", "--codex-bin", bin_]
            resume_thread = getattr(args, "resume_thread", None)
            if resume_thread:
                argv += ["--resume-thread", resume_thread]
            if corr:
                argv += ["--corr", corr]
        else:
            argv = profile["argv"](bin_, model, perms, prompt)
    unsets = _spawn_unsets()
    inner = ("env " + " ".join(f"-u {k}" for k in unsets)
             + f" CLAUDEBUS_ID={shlex.quote(name)}"
             + f" CLAUDEBUS_SPAWNED_BY={shlex.quote(me)} "
             + f"CLAUDEBUS_PROVIDER={shlex.quote(args.agent)} "
             + f"CLAUDEBUS_TRANSPORT={shlex.quote(transport)} "
             + " ".join(shlex.quote(a) for a in argv))
    # remain-on-exit is chained into the SAME tmux client command as
    # new-session: both run in one server pass, before the server processes a
    # fast-crashing child's exit. A separate set-option call loses that race
    # and the dead session vanishes before it can be marked.
    tmux_cmd = _tmux("new-session", "-d", "-s", sess, "-c", workdir, inner,
                     ";", "set-option", "-t", sess, "remain-on-exit", "on")

    if args.dry_run:
        con.close()
        print(json.dumps({"name": name, "tmux": sess, "dir": workdir,
                          "agent": args.agent, "model": model,
                          "transport": transport,
                          "cmd": inner, "tmux_cmd": tmux_cmd}))
        return 0

    if args.wait and transport != "app-server":
        # a pending 'req' row gives the child's `claudebus reply <corr>` its
        # reply-to target, exactly like `ask`
        con.execute(
            "INSERT INTO messages(ts,sender,channel,body,kind,corr,reply_to) "
            "VALUES(?,?,?,?,'req',?,?)",
            (time.time(), me, f"@{name}",
             "[spawn --wait] reply here with the result when done", corr, f"@{me}"),
        )

    # launch with the same sanitized env, so a freshly-started tmux SERVER
    # (which inherits our env and passes it to every future session) never
    # carries the parent session's CLAUDE markers either
    env = {k: v for k, v in os.environ.items() if k not in set(unsets)}
    try:
        p = subprocess.run(tmux_cmd, env=env, capture_output=True, text=True, timeout=30)
    except OSError as e:
        con.close()
        print(color(f"spawn: tmux not runnable: {e}", "rd"), file=sys.stderr)
        return 2
    except subprocess.TimeoutExpired:
        con.close()
        print(color("spawn: tmux timed out", "rd"), file=sys.stderr)
        return 2
    if p.returncode != 0:
        con.close()
        print(color(f"spawn: {p.stderr.strip() or 'tmux failed'}", "rd"), file=sys.stderr)
        return 2

    # liveness check: give the agent a moment to start, then verify the pane
    # process is still alive (remain-on-exit above keeps a crashed pane around
    # for inspection). A dead or vanished pane here means the CLI crashed at
    # launch (bad flag, missing binary, env problem) - report its output
    # instead of silently registering a corpse.
    time.sleep(1.5)
    dead = subprocess.run(
        _tmux("display-message", "-p", "-t", sess, "#{pane_dead}"),
        capture_output=True, text=True)
    if dead.returncode != 0 or dead.stdout.strip() == "1":
        # a dead pane is NOT a startup crash if the child already delivered
        # its reply (a fast agent can finish inside the 1.5s grace window) -
        # only an exit with no reply on our corr is a real corpse
        finished = False
        if corr:
            finished = bool(con.execute(
                "SELECT 1 FROM messages WHERE kind='reply' AND corr=? AND id>? LIMIT 1",
                (corr, start)).fetchone())
        if not finished:
            tail = subprocess.run(_tmux("capture-pane", "-p", "-S", "-", "-t", sess),
                                  capture_output=True, text=True).stdout.strip()
            subprocess.run(_tmux("kill-session", "-t", sess), capture_output=True)
            con.close()
            print(color(f"spawn: '{name}' died at startup", "rd"), file=sys.stderr)
            if tail:
                print(color("--- last output ---", "dim"), file=sys.stderr)
                lines = [l for l in tail.splitlines() if l.strip()]
                print("\n".join(lines[-15:]), file=sys.stderr)
            return 2
        subprocess.run(_tmux("kill-session", "-t", sess), capture_output=True)

    con.execute(
        "INSERT INTO spawns(name,tmux,parent,task,ts,agent,transport) "
        "VALUES(?,?,?,?,?,?,?) "
        "ON CONFLICT(name) DO UPDATE SET tmux=excluded.tmux, parent=excluded.parent, "
        "task=excluded.task, ts=excluded.ts, agent=excluded.agent, "
        "transport=excluded.transport",
        (name, sess, me, task, time.time(), args.agent, transport),
    )
    # The child owns endpoint state once it has started. A fast app-server
    # worker may already be active, idle, or failed by this point; never
    # regress that state back to the parent's coarse "started" observation.
    endpoint = con.execute(
        "SELECT state FROM endpoints WHERE name=?", (name,),
    ).fetchone()
    if not endpoint:
        upsert_endpoint(
            con, name, args.agent, parent=me,
            transport="app-server-stdio" if transport == "app-server" else "tmux",
            cwd=workdir,
            model=model, permission_profile=perms, state="started",
            metadata={"tmux": sess},
        )
    record_event(
        con, "spawn_started", actor=me, subject=name, provider=args.agent,
        corr=corr, idempotency_key=f"spawn:{name}:{int(time.time())}",
        status="started", payload={"tmux": sess, "cwd": workdir,
                                    "transport": transport,
                                    "model": model, "permissions": perms,
                                    "brief": task},
    )
    con.close()
    poke_waiters()

    if args.wait:
        return _await_reply(me, corr, start, args.timeout or 600, args.json, name)
    if args.json:
        print(json.dumps({"name": name, "tmux": sess, "dir": workdir,
                          "agent": args.agent, "model": model,
                          "transport": transport,
                          "attach": f"tmux attach -t {sess}"}))
        return 0
    print(color(f"spawned '{name}'", "gn")
          + f" ({args.agent}/{model}, {transport} via tmux {sess}, {workdir})")
    print("  result arrives on the bus: `claudebus wait --timeout N` blocks for it, "
          "or it lands next turn via the hook")
    print(f"  re-task: `claudebus tell {name} \"<msg>\"`   watch live: tmux attach -t {sess}")
    return 0


def cmd_tell(args):
    """Inject a follow-up prompt into a spawned session's TUI (bracketed paste
    + Enter). This is re-delegation to an already-running full session."""
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    row = con.execute("SELECT tmux,transport,agent FROM spawns WHERE name=?",
                      (args.name,)).fetchone()
    sess = row[0] if row else f"cb-{args.name}"
    if subprocess.run(_tmux("has-session", "-t", sess), capture_output=True).returncode != 0:
        con.close()
        print(color(f"tell: no live session '{args.name}' (tmux {sess})", "rd"),
              file=sys.stderr)
        return 1
    body = args.message
    if body == "-" or body is None:
        body = sys.stdin.read().rstrip("\n")
    transport = row[1] if row else "tmux"
    if transport == "app-server":
        cur = con.execute(
            "INSERT INTO messages(ts,sender,channel,body,kind) VALUES(?,?,?,?,?)",
            (time.time(), me, f"@{args.name}", body, "msg"),
        )
        record_event(con, "worker_steer_requested", actor=me, subject=args.name,
                     provider=row[2] if row else None, causal_id=cur.lastrowid,
                     status="queued",
                     payload={"transport": "app-server", "message_id": cur.lastrowid})
        con.close()
        poke_waiters()
        print(color(f"told {args.name}", "gn") + " (durable app-server DM)")
        return 0
    con.close()
    text = f"[claudebus tell, from {me}] {body}"
    loaded = subprocess.run(
        _tmux("load-buffer", "-b", "cbtell", "-"), input=text.encode()
    )
    pasted = subprocess.run(
        _tmux("paste-buffer", "-p", "-d", "-b", "cbtell", "-t", sess)
    )
    time.sleep(0.4)  # let the TUI ingest the paste before submitting
    submitted = subprocess.run(_tmux("send-keys", "-t", sess, "Enter"))
    if any(proc.returncode != 0 for proc in (loaded, pasted, submitted)):
        print(color(f"tell: tmux injection failed for '{args.name}'", "rd"),
              file=sys.stderr)
        return 2
    con = db()
    row = con.execute("SELECT agent FROM spawns WHERE name=?", (args.name,)).fetchone()
    record_event(con, "worker_steer_requested", actor=me, subject=args.name,
                 provider=row[0] if row else None,
                 status="queued", payload={"transport": "tmux"})
    con.close()
    print(color(f"told {args.name}", "gn"))
    return 0


def cmd_sessions(args):
    con = db()
    now = time.time()
    rows = con.execute(
        "SELECT name,tmux,parent,task,ts,agent,transport "
        "FROM spawns ORDER BY ts DESC").fetchall()
    peers = {n: s for n, s in con.execute("SELECT name,last_seen FROM peers").fetchall()}
    con.close()
    live = _tmux_live_sessions()
    if not getattr(args, "all", False):
        # dead spawns linger briefly (visible aftermath), then leave the view;
        # the registry row survives until SPAWN_TTL — `sessions --all` shows it
        rows = [r for r in rows
                if r[1] in live or (now - r[4]) <= DEAD_LINGER]
    if args.json:
        print("\n".join(json.dumps({
            "name": n, "tmux": t, "parent": p, "task": task, "agent": ag,
            "transport": tr,
            "age": round(now - ts, 1), "alive": t in live,
            "on_bus_ago": (round(now - peers[n], 1) if n in peers else None),
        }) for n, t, p, task, ts, ag, tr in rows))
        return 0
    if not rows:
        print(color("(no spawned sessions)", "dim")); return 0
    print(color(f"{'NAME':10} {'AGENT':7} {'TRANSPORT':10} {'STATE':6} "
                f"{'PARENT':12} {'AGE':>6}  TASK", "b"))
    for n, t, p, task, ts, ag, tr in rows:
        age = int(now - ts)
        age_s = f"{age}s" if age < 90 else f"{age//60}m"
        st = "alive" if t in live else "dead"
        line = (f"{n:10} {(ag or 'claude'):7} {(tr or 'tmux'):10} {st:6} "
                f"{(p or '-'):12} {age_s:>6}  {(task or '')[:44]}")
        print(color(line, "gn" if st == "alive" else "dim"))
    print(color("attach: tmux attach -t cb-<name>   re-task: claudebus tell <name> \"<msg>\"", "dim"))
    return 0


def cmd_tree(args):
    """Render the delegation forest: spawns nest under their parent; roots are
    delegates whose parent is a real tab (not itself a spawn)."""
    con = db()
    now = time.time()
    rows = con.execute(
        "SELECT name,tmux,parent,task,ts FROM spawns ORDER BY ts").fetchall()
    con.close()
    live = _tmux_live_sessions()
    names = {r[0] for r in rows}
    kids = {}
    for n, t, p, task, ts in rows:
        kids.setdefault(p, []).append((n, t, p, task, ts))

    def node(r, seen):
        n, t, p, task, ts = r
        seen = seen | {n}
        children = [node(c, seen) for c in kids.get(n, []) if c[0] not in seen]
        return {"name": n, "parent": p, "alive": t in live, "task": task,
                "age": round(now - ts, 1), "children": children}

    roots = [node(r, set()) for r in rows if r[2] not in names]
    if not getattr(args, "all", False):
        # same linger rule as `sessions`: long-dead nodes drop out of the view,
        # but a dead ancestor with a surviving descendant stays as scaffolding
        def sweep(nd):
            nd["children"] = [c for c in map(sweep, nd["children"]) if c]
            if nd["alive"] or nd["age"] <= DEAD_LINGER or nd["children"]:
                return nd
            return None
        roots = [r for r in map(sweep, roots) if r]
    if args.json:
        print(json.dumps(roots))
        return 0
    if not roots:
        print(color("(no delegation tree)", "dim")); return 0

    def render(nd, depth):
        st = "alive" if nd["alive"] else "dead"
        pad = "  " * depth + ("└ " if depth else "")
        line = f"{pad}{nd['name']} [{st}]  {(nd['task'] or '')[:50]}"
        print(color(line, "gn" if nd["alive"] else "dim"))
        for c in nd["children"]:
            render(c, depth + 1)

    seen_parents = set()
    for r in roots:
        if r["parent"] not in seen_parents:
            seen_parents.add(r["parent"])
            print(color(f"{r['parent']} (lead)", "b"))
        render(r, 1)
    return 0


def cmd_kill(args):
    me = whoami(args)
    con = db()
    row = con.execute("SELECT tmux FROM spawns WHERE name=?", (args.name,)).fetchone()
    sess = row[0] if row else f"cb-{args.name}"
    killed = subprocess.run(_tmux("kill-session", "-t", sess),
                            capture_output=True).returncode == 0
    row = con.execute("SELECT agent FROM spawns WHERE name=?", (args.name,)).fetchone()
    n = con.execute("DELETE FROM spawns WHERE name=?", (args.name,)).rowcount
    if killed or n:
        endpoint_state(con, args.name, "stopped")
        record_event(con, "worker_stopped", actor=me,
                     subject=args.name, provider=row[0] if row else None,
                     status="stopped")
    con.close()
    if killed or n:
        print(color(f"killed '{args.name}'", "gn"))
        return 0
    print(color(f"kill: no session '{args.name}'", "rd"), file=sys.stderr)
    return 1


def cmd_respawn(args):
    """OTP-supervisor-style restart: relaunch a DEAD spawned session with its
    originally recorded task - the spawns row is the child spec (Erlang
    one_for_one, restarted on demand rather than by a daemon, since the bus
    has none). A `kill` removes the record; a crash/finish leaves it, which is
    exactly the restartable case."""
    me = whoami(args)
    con = db()
    heartbeat(con, me)
    row = con.execute(
        "SELECT name,tmux,parent,task,agent,transport FROM spawns WHERE name=?",
        (args.name,)).fetchone()
    endpoint = con.execute(
        "SELECT provider,native_thread_id FROM endpoints WHERE name=?",
        (args.name,),
    ).fetchone()
    con.close()
    if not row:
        print(color(f"respawn: no spawn record for '{args.name}' "
                    f"(killed sessions lose theirs)", "rd"), file=sys.stderr)
        return 1
    name, sess, parent, task, agent, transport = row
    resume_thread = (
        endpoint[1]
        if endpoint and endpoint[0] == "codex" and agent == "codex"
        and transport == "app-server" and endpoint[1]
        else None
    )
    if sess in _tmux_live_sessions():
        print(color(f"respawn: '{name}' is still alive (tmux {sess}) - "
                    f"`kill {name}` first or `tell {name}` instead", "rd"),
              file=sys.stderr)
        return 1
    if args.dry_run:
        print(json.dumps({"name": name, "task": task, "agent": agent,
                          "transport": transport,
                          "parent": parent, "resume_thread": resume_thread,
                          "dry_run": True}))
        return 0
    subprocess.run(_tmux("kill-session", "-t", sess), capture_output=True)  # reap corpse
    ns = argparse.Namespace(
        as_=getattr(args, "as_", None), session=getattr(args, "session", None),
        task=task, name=name, agent=agent, model=SPAWN_MODEL, dir=None,
        perms=None, transport=transport, wait=False, timeout=600,
        dry_run=False, json=args.json, resume_thread=resume_thread)
    return cmd_spawn(ns)


# ---------------- presence / stats / purge ----------------

def cmd_join(args):
    """Announce arrival on the bus and report current coordination state. Run at
    session start so a new tab is a participant from turn 0: registers presence,
    joins the message stream at HEAD (bounded), and learns who's online + what
    lanes are held. With --announce it broadcasts a one-line 'joined' notice."""
    me = whoami(args)
    now = time.time()
    con = db()
    heartbeat(con, me)
    ensure_cursor(con, me, None)   # bounded: start at HEAD, never inherit backlog
    peers = con.execute("SELECT name,pid,last_seen FROM peers").fetchall()
    online = [n for n, p, s in peers if n != me and peer_active(p, s, now)]
    lane_rows = con.execute(
        "SELECT lane,holder,pid,ts,ttl,note FROM lanes ORDER BY ts DESC").fetchall()
    held = [(l, h, ts, ttl, n) for l, h, p, ts, ttl, n in lane_rows
            if not _lane_free((h, p, ts, ttl), now)]
    if args.announce:
        # a custom note is substantive and delivers; the bare default is presence
        con.execute(
            "INSERT INTO messages(ts,sender,channel,body,kind) VALUES(?,?,?,?,?)",
            (time.time(), me, "all", args.note or "joined the bus",
             "msg" if args.note else "presence"),
        )
    con.close()
    if args.announce and args.note:
        poke_waiters()

    if args.json:
        print(json.dumps({
            "me": me,
            "peers_online": len(online),
            "peers": sorted(online),
            "lanes": [{"lane": l, "holder": h, "age": round(now - ts, 1), "note": n}
                      for l, h, ts, ttl, n in held],
            "announced": bool(args.announce),
        }))
        return 0
    others = ", ".join(sorted(online)) if online else "none yet"
    lines = [f"[claudebus] You are tab '{me}' on the local Claude-Code tab bus. "
             f"Other tabs online: {others}."]
    if held:
        lstr = "; ".join(
            f"{l} (held by {h}" + (f" - {n}" if n else "") + ")"
            for l, h, ts, ttl, n in held)
        lines.append(f"Lanes currently CLAIMED (coordinate before editing these): {lstr}.")
    lines.append("To coordinate: `claudebus claim <lane>` before editing shared files, "
                 "`send <msg> [--to NAME]`, `ask <NAME> <q>`, `lanes`, `ps`.")
    lines.append("To DELEGATE work, the default primitive is `claudebus spawn \"<task>\"`: "
                 "it starts a FULL Claude Code session (hooks+MCP+bus identity, in tmux) "
                 "that works the task and reports back to you on the bus. "
                 "`tell <name> \"<msg>\"` re-tasks it, `sessions` lists, `kill <name>` stops.")
    print("\n".join(lines))
    return 0


def hello_core(con, sid, want, announce, note=None, caps=None, owner_pid=None,
               provider="claude", native_thread_id=None, parent=None,
               transport="hooks", cwd=None, model=None,
               permission_profile=None):
    """Session-identity join: bind sid->name, enter the stream at HEAD,
    heartbeat presence. Returns (name, online_peers, held_lanes)."""
    now = time.time()
    if sid:
        name = _alloc_name(con, sid, want, owner_pid=owner_pid,
                           provider=provider)
    else:
        name = want or os.environ.get("CLAUDEBUS_ID") or f"tab-{os.getppid()}"
    heartbeat(con, name)
    upsert_endpoint(
        con, name, provider, native_session_id=sid,
        native_thread_id=native_thread_id, parent=parent,
        transport=transport, cwd=cwd, model=model,
        permission_profile=permission_profile, state="joined",
    )
    record_event(
        con, "session_joined", actor=name, subject=name, provider=provider,
        idempotency_key=f"session_joined:{provider}:{sid}" if sid else None,
        payload={"transport": transport, "cwd": cwd, "parent": parent},
    )
    if caps:
        con.execute("UPDATE peers SET caps=? WHERE name=?", (caps, name))
    ensure_cursor(con, name, None)   # bounded: start at HEAD, never inherit backlog
    peers = con.execute("SELECT name,pid,last_seen FROM peers").fetchall()
    online = [n for n, p, s in peers if n != name and peer_active(p, s, now)]
    lane_rows = con.execute(
        "SELECT lane,holder,pid,ts,ttl,note FROM lanes ORDER BY ts DESC").fetchall()
    held = [(l, h, ts, ttl, n) for l, h, p, ts, ttl, n in lane_rows
            if not _lane_free((h, p, ts, ttl), now)]
    if announce:
        con.execute(
            "INSERT INTO messages(ts,sender,channel,body,kind) VALUES(?,?,?,?,?)",
            (now, name, "all", note or "joined the bus",
             "msg" if note else "presence"))
    return name, online, held


def hello_text(name, online, held):
    others = ", ".join(sorted(online)) if online else "none right now"
    lines = [f"[claudebus] You are '{name}' on this machine's agent session bus. "
             f"Peers online: {others}."]
    if held:
        lstr = "; ".join(f"{l} (held by {h}" + (f" - {n}" if n else "") + ")"
                         for l, h, ts, ttl, n in held)
        lines.append(f"Lanes CLAIMED (coordinate before touching these): {lstr}.")
    lines.append(
        "Bus protocol (use it by default): announce work that could collide "
        "(`claudebus send \"<doing X>\"`), `claudebus claim <lane>` before editing "
        "shared files, answer asks addressed to you, `claudebus ps` lists peers, "
        "`claudebus spawn \"<task>\"` delegates to a full session.")
    lines.append(
        "v4 primitives: `cfp \"<task>\"` auctions work to peers (they `bid`, you "
        "`award`); `bb set/get <key>` is the shared blackboard; `discover <cap>` "
        "finds peers by capability (declare yours: `caps rust,review`); "
        "`elect <role>` takes a leader lease; `submit --prio/--after` + "
        "`fail`/`retry` give the queue priorities, backoff and a DLQ.")
    lines.append(
        "Incoming messages auto-surface at turn start, mid-turn after tool calls, and "
        "at turn end. To stay reachable while idle, arm the listener: run in background "
        "`claudebus wait --json` - it exits when traffic arrives, which re-invokes you; "
        "re-arm after each wake-up (the Stop hook reminds you; opt out with "
        "`claudebus listener off`). Join/leave presence never wakes a listener.")
    return "\n".join(lines)


def cmd_hello(args):
    sid = _provider_session_id(args)
    want = args.name or os.environ.get("CLAUDEBUS_ID")
    owner_pid = os.environ.get("CLAUDEBUS_OWNER_PID")
    provider = os.environ.get("CLAUDEBUS_PROVIDER", "claude")
    con = db()
    name, online, held = hello_core(con, sid, want, args.announce, args.note,
                                    getattr(args, "caps", None),
                                    owner_pid=int(owner_pid) if owner_pid else None,
                                    provider=provider,
                                    native_thread_id=(os.environ.get("CLAUDEBUS_THREAD_ID")
                                                      or (os.environ.get("CODEX_THREAD_ID")
                                                          if provider == "codex" else None)),
                                    parent=os.environ.get("CLAUDEBUS_SPAWNED_BY"),
                                    transport=os.environ.get("CLAUDEBUS_TRANSPORT", "cli"),
                                    cwd=os.getcwd())
    con.close()
    if args.announce and args.note:
        poke_waiters()   # presence-only announces never wake parked listeners
    if args.json:
        print(json.dumps({"me": name, "peers": sorted(online),
                          "lanes": [l for l, h, ts, ttl, n in held],
                          "announced": bool(args.announce)}))
        return 0
    print(hello_text(name, online, held))
    return 0


def leave_core(con, name, announce):
    """Session departure: free lanes, drop presence, reap dead waiter FIFOs.
    The identity binding and cursor survive so a resumed session keeps its
    name and never re-reads delivered messages."""
    con.execute("DELETE FROM lanes WHERE holder=?", (name,))
    con.execute("DELETE FROM peers WHERE name=?", (name,))
    endpoint_state(con, name, "left")
    row = con.execute("SELECT provider FROM endpoints WHERE name=?", (name,)).fetchone()
    record_event(con, "session_left", actor=name, subject=name,
                 provider=row[0] if row else None)
    if announce:
        con.execute(
            "INSERT INTO messages(ts,sender,channel,body,kind) VALUES(?,?,?,?,'presence')",
            (time.time(), name, "all", "left the bus"))
    try:
        for f in os.listdir(WAITERS):
            if f.startswith(f"{name}.") and f.endswith(".fifo"):
                parts = f.rsplit(".", 2)
                if len(parts) == 3 and not pid_alive(parts[1]):
                    try:
                        os.unlink(os.path.join(WAITERS, f))
                    except OSError:
                        pass
    except FileNotFoundError:
        pass


def cmd_leave(args):
    me = whoami(args)
    con = db()
    leave_core(con, me, args.announce)
    con.close()
    return 0   # leave is presence-only: nothing deliverable, so no poke


def pulse_core(con, name, chans, mark):
    """Non-destructive mid-turn peek. Reports only traffic newer than BOTH the
    real recv cursor (recv delivers those at the turn boundary anyway) and this
    mark's own watermark, so one agent context never re-sees a message while
    the authoritative delivery stays with recv."""
    row = con.execute("SELECT last_id,dm_id FROM cursors WHERE consumer=?", (name,)).fetchone()
    if row is None:
        return []
    cur, dmcur = row
    pdir = os.path.join(ROOT, "pulse")
    os.makedirs(pdir, exist_ok=True)
    wm_path = os.path.join(
        pdir, f"{name}.{hashlib.md5((mark or 'default').encode()).hexdigest()[:10]}")
    try:
        with open(wm_path) as f:
            wm = int(f.read().strip() or 0)
    except (OSError, ValueError):
        wm = 0
    lo = min(cur, dmcur)
    rows = con.execute(
        "SELECT id,ts,sender,channel,body,kind,corr,reply_to FROM messages "
        "WHERE id>? ORDER BY id", (lo,)).fetchall()
    me_chan = f"@{name}"
    mine = [r for r in rows
            if r[0] > wm and r[2] != name and not is_presence(r)
            and ((r[3] == me_chan and r[0] > dmcur)
                 or (r[3] != me_chan and r[0] > cur and chan_match(r[3], chans)))]
    if rows:
        try:
            with open(wm_path, "w") as f:
                f.write(str(rows[-1][0]))
        except OSError:
            pass
    return mine


def cmd_pulse(args):
    me = whoami(args)
    con = db()
    rows = pulse_core(con, me, my_channels(me, args), args.mark)
    con.close()
    if rows:
        print(fmt(rows, True))
    return 0


def pref_get(con, name, key, default=None):
    """Per-identity preference with '*' as the global-default row."""
    for n in (name, "*"):
        row = con.execute(
            "SELECT value FROM prefs WHERE name=? AND key=?", (n, key)).fetchone()
        if row:
            return row[0]
    return default


def pref_set(con, name, key, value):
    con.execute(
        "INSERT INTO prefs(name,key,value,updated) VALUES(?,?,?,?) "
        "ON CONFLICT(name,key) DO UPDATE SET value=excluded.value, "
        "updated=excluded.updated",
        (name, key, value, time.time()))


def listener_policy(name):
    """'on' (default) = the Stop hook enforces an armed idle listener;
    'off' = this identity opts out of idle wake-ups (messages still deliver
    at turn boundaries, it just isn't woken while idle)."""
    con = db()
    v = pref_get(con, name, "listener", "on")
    con.close()
    return v


def cmd_listener(args):
    me = whoami(args)
    if args.mode == "status":
        con = db()
        v = pref_get(con, me, "listener", "on")
        con.close()
        print(f"listener policy for {me}: {v}")
        return 0
    target = "*" if args.glob else me
    con = db()
    pref_set(con, target, "listener", args.mode)
    con.close()
    print(color(f"listener {args.mode} for {target}", "gn"))
    return 0


def armed_pids(name):
    """Live parked-waiter pids for this identity (fifo files are self-registering:
    created by wait/watch, unlinked on exit)."""
    out = []
    try:
        for f in os.listdir(WAITERS):
            if f.startswith(f"{name}.") and f.endswith(".fifo"):
                parts = f.rsplit(".", 2)
                if len(parts) == 3 and pid_alive(parts[1]):
                    out.append(parts[1])
    except FileNotFoundError:
        pass
    return out


def cmd_armed(args):
    me = whoami(args)
    pids = armed_pids(me)
    if pids:
        print(f"armed: {me} (listener pid {', '.join(pids)})")
        return 0
    print(color(f"not armed: {me} has no parked listener", "dim"))
    return 1


def cmd_ps(args):
    con = db()
    now = time.time()
    rows = con.execute(
        "SELECT name,pid,tty,last_seen FROM peers ORDER BY last_seen DESC").fetchall()
    waiting = set()
    try:
        for f in os.listdir(WAITERS):
            if f.endswith(".fifo"):
                parts = f.rsplit(".", 2)
                # A SIGKILL leaves the FIFO behind because the listener's
                # cleanup cannot run.  Presence is waiting only while the
                # PID encoded in the filename is actually alive.
                if len(parts) == 3 and pid_alive(parts[1]):
                    waiting.add(parts[0])
    except FileNotFoundError:
        pass
    con.close()

    def state_of(name, pid, seen):
        if name in waiting:
            return "waiting"
        if peer_active(pid, seen, now):
            return "active"
        return "idle"

    if args.json:
        print("\n".join(json.dumps({
            "name": n, "pid": p, "tty": t, "age": round(now - s, 1),
            "state": state_of(n, p, s),
        }) for n, p, t, s in rows))
        return 0
    if not rows:
        print(color("(no peers yet)", "dim")); return 0
    print(color(f"{'NAME':14} {'PID':>7} {'SEEN':>8} {'TTY':12} STATE", "b"))
    for name, pid, tty, seen in rows:
        ago = int(now - seen)
        seen_s = f"{ago}s" if ago < 90 else f"{ago//60}m"
        st = state_of(name, pid, seen)
        line = f"{name:14} {pid:>7} {seen_s:>8} {(tty or '-'):12} {st}"
        col = {"active": "gn", "waiting": "cy"}.get(st, "dim")
        print(color(line, col))
    return 0


def cmd_endpoints(args):
    """Provider-neutral lifecycle registry for Claude, Codex and bridge peers."""
    con = db()
    query = ("SELECT name,provider,native_session_id,native_thread_id,parent,"
             "transport,cwd,model,permission_profile,state,last_seen,metadata "
             "FROM endpoints")
    vals = []
    if args.provider:
        query += " WHERE provider=?"
        vals.append(args.provider)
    query += " ORDER BY last_seen DESC"
    rows = con.execute(query, vals).fetchall()
    con.close()
    out = []
    for row in rows:
        (name, provider, sid, tid, parent, transport, cwd, model, perms,
         state, seen, metadata) = row
        try:
            meta = json.loads(metadata) if metadata else None
        except json.JSONDecodeError:
            meta = {"raw": metadata}
        out.append({"name": name, "provider": provider, "session_id": sid,
                    "thread_id": tid, "parent": parent,
                    "transport": transport, "cwd": cwd, "model": model,
                    "permissions": perms, "state": state,
                    "age": round(time.time() - seen, 1), "metadata": meta})
    if args.json:
        print("\n".join(json.dumps(row) for row in out))
        return 0
    if not out:
        print(color("(no provider endpoints)", "dim")); return 0
    print(color(f"{'NAME':16} {'PROVIDER':8} {'STATE':10} {'TRANSPORT':12} {'AGE':>7}", "b"))
    for row in out:
        print(f"{row['name'][:16]:16} {row['provider'][:8]:8} "
              f"{row['state'][:10]:10} {(row['transport'] or '-')[:12]:12} "
              f"{int(row['age']):>6}s")
    return 0


def cmd_events(args):
    """Read the append-only coordination and provider lifecycle journal."""
    con = db()
    where, vals = [], []
    if args.type:
        where.append("type=?"); vals.append(args.type)
    if args.corr:
        where.append("corr=?"); vals.append(args.corr)
    query = ("SELECT id,ts,type,actor,subject,provider,corr,causal_id,"
             "idempotency_key,status,payload FROM events")
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY id DESC LIMIT ?"
    vals.append(args.n)
    rows = list(reversed(con.execute(query, vals).fetchall()))
    con.close()
    data = []
    for row in rows:
        try:
            payload = json.loads(row[10]) if row[10] else None
        except json.JSONDecodeError:
            payload = {"raw": row[10]}
        data.append({"id": row[0], "ts": row[1], "type": row[2],
                     "actor": row[3], "subject": row[4], "provider": row[5],
                     "corr": row[6], "causal_id": row[7],
                     "idempotency_key": row[8], "status": row[9],
                     "payload": payload})
    if args.json:
        print("\n".join(json.dumps(row) for row in data))
        return 0
    for row in data:
        stamp = time.strftime("%H:%M:%S", time.localtime(row["ts"]))
        print(f"{row['id']:>6} {stamp} {row['type']:<20} "
              f"{row['actor'] or '-'} -> {row['subject'] or '-'} "
              f"[{row['status']}]")
    return 0


def cmd_stats(args):
    con = db()
    now = time.time()
    nmsg = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    oldest = con.execute("SELECT MIN(ts) FROM messages").fetchone()[0]
    perchan = con.execute(
        "SELECT channel,COUNT(*) FROM messages GROUP BY channel ORDER BY 2 DESC LIMIT 8").fetchall()
    peers = con.execute("SELECT name,pid,last_seen FROM peers").fetchall()
    active = sum(1 for _, p, s in peers if peer_active(p, s, now))
    lanes = con.execute("SELECT holder,pid,ts,ttl FROM lanes").fetchall()
    held = sum(1 for h, p, ts, ttl in lanes if not _lane_free((h, p, ts, ttl), now))
    qdepth = con.execute(
        "SELECT COUNT(*) FROM tasks WHERE done=0 AND lease_until<=?", (now,)).fetchone()[0]
    leased = con.execute(
        "SELECT COUNT(*) FROM tasks WHERE done=0 AND lease_until>?", (now,)).fetchone()[0]
    nevents = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    nendpoints = con.execute("SELECT COUNT(*) FROM endpoints").fetchone()[0]
    con.close()
    data = {
        "messages": nmsg,
        "oldest_age_s": round(now - oldest, 1) if oldest else None,
        "peers_total": len(peers), "peers_active": active,
        "lanes_held": held, "queue_depth": qdepth, "queue_leased": leased,
        "events": nevents, "endpoints": nendpoints,
        "per_channel": {c: n for c, n in perchan},
        "db_bytes": os.path.getsize(DB) if os.path.exists(DB) else 0,
    }
    if args.json:
        print(json.dumps(data)); return 0
    print(color("claudebus stats", "b"))
    print(f"  messages     {nmsg}" + (f"  (oldest {int(data['oldest_age_s'])}s ago)" if oldest else ""))
    print(f"  peers        {active} active / {len(peers)} total")
    print(f"  lanes held   {held}")
    print(f"  queue        {qdepth} ready, {leased} leased")
    print(f"  v5 journal   {nevents} events, {nendpoints} endpoints")
    if perchan:
        top = "  ".join(f"{c}:{n}" for c, n in perchan)
        print(f"  channels     {top}")
    print(f"  db size      {data['db_bytes']} bytes")
    return 0


def cmd_purge(args):
    con = db()
    if args.all:
        for t in ("messages", "cursors", "lanes", "tasks", "spawns", "blackboard"):
            con.execute(f"DELETE FROM {t}")
        con.execute("DELETE FROM peers")
    elif args.older is not None:
        cutoff = time.time() - args.older * 86400
        con.execute("DELETE FROM messages WHERE ts<?", (cutoff,))
        con.execute("DELETE FROM tasks WHERE done=1 AND ts<?", (cutoff,))
    else:
        prune(con)
    con.execute("VACUUM")
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    print(color("purged", "gn"))
    return 0


HELP = f"""{color('claudebus', 'b')} v5 - local coordination bus for Claude + Codex sessions

{color('Setup (per terminal tab):', 'b')}
  CLAUDEBUS_ID=alice claude        # give each tab a stable name

{color('Messaging:', 'b')}
  send <msg> [--to NAME|--channel CH]   broadcast / DM / topic
  recv [--since all|N] [--sub 'p.*']    read my new msgs (new tabs start at
                                        HEAD); --sub takes MQTT-style wildcards
  wait [--timeout S]                    block until ONE msg, then exit
  watch [--timeout S]                   stream live (run in background)
  peek [-n N]                           recent msgs, don't consume

{color('Coordination:', 'b')}
  claim <lane> [--ttl S] [--note ..]    take exclusive hold of a named lane
  release <lane> [--force]              free a lane
  lanes                                 who holds what
  ask <peer> <q> [--timeout S]          RPC: ask and block for the reply
  reply <id> <answer>                   answer a pending ask

{color('Work-queue (v4: priority / retries / DLQ / deps):', 'b')}
  submit <task> [--queue Q] [--prio N]  enqueue; higher prio taken first;
         [--max-attempts N]             --after gates on other tasks finishing
         [--after id,id]                (submit a whole pipeline at once)
  take [--queue Q] [--lease S]          claim the best ready task
  done <id>                             ack a claimed task
  fail <id> [--delay S]                 nack: requeue with exponential backoff;
                                        past max-attempts -> dead-letter queue
                                        (submitter is DM'd)
  retry <id>                            resurrect a dead-lettered task
  pipeline <s1> <s2> .. [--queue Q]     submit a sequential chain in one call
                                        (each step gated on the previous)
  tasks [--queue Q] [--dlq]             list (states: ready/leased/delayed/
                                        blocked/dead/done); --dlq = dead only

{color('Contract net (task auction - Smith 1980/FIPA):', 'b')}
  cfp <task> [--cap C] [--window S]     broadcast a call-for-proposals; with
                                        --window S it waits S seconds then
                                        auto-awards the best bid
  bid <corr> <score 0-1> [note]         bid on a cfp you can handle
  bids <corr>                           list bids received
  award <corr> [--to NAME]              award to best bid (or force --to)

{color('Blackboard (shared state - polled, never wakes):', 'b')}
  bb set <key> <val> [--ttl S]          shared versioned KV; --if-ver N is
         [--if-ver N]                   compare-and-set (fails on conflict)
  bb get <key> | bb del <key>           read / remove
  bb list [--prefix P]                  browse

{color('Capabilities & leadership:', 'b')}
  caps [LIST|clear]                     declare what I can do (rust,review,..)
  discover <cap>                        live peers that declare a capability
  hello --caps a,b                      declare at join time
  elect <role> [--ttl S]                lease-based leader election (Chubby
                                        style); re-elect to renew; rc=1 lost
  leader <role>                         who currently leads a role

{color('Delegation (full sessions):', 'b')}
  spawn <task> [--name N] [--agent A]   start a FULL agent session in tmux; it
        [--model M] [--dir D]           joins the bus as <name> and reports back.
        [--perms MODE] [--wait]         --agent claude|codex|gemini|atlas
                                        (codex/gemini = GPT/Gemini CLI lane,
                                        sandboxed by default; atlas = ChatGPT
                                        Atlas agent mode via AppleScript, needs
                                        the JS-from-Apple-Events toggle;
                                        --wait blocks for its DONE reply)
  tell <name> <msg>                     inject a follow-up prompt into its TUI
  sessions                              list spawned sessions (alive/dead)
  tree                                  delegation hierarchy (who spawned whom)
  kill <name>                           stop a spawned session (drops its record)
  respawn <name> [--dry-run]            restart a DEAD spawn with its original
                                        task (crash/finish keeps the record)
  (--model auto routes by task: haiku mechanical / sonnet coding / opus review;
   delegates may spawn their own delegates and message siblings directly)

{color('Ops:', 'b')}
  hello [--name N] [--announce]         V3 join: bind THIS Claude session_id to a bus
                                        name (auto-named if none); idempotent; the
                                        SessionStart hook runs it for every session
  leave [--announce]                    free lanes + drop presence (SessionEnd hook)
  pulse [--mark M]                      non-destructive peek of undelivered traffic
                                        (per-mark watermark; PostToolUse hook)
  armed                                 rc 0 if my idle listener (wait) is parked
  listener on|off|status [--global]     idle-wake policy: off = Stop hook stops
                                        enforcing an armed listener (messages
                                        still deliver at turn boundaries)
  join [--announce]                     register on the bus + report who's online & lanes held
  ps                                    who's on the bus (heartbeat presence)
  stats                                 bus health/throughput
  endpoints [--provider P]              provider-native session/thread registry
  events [-n N] [--type T] [--corr C]  append-only lifecycle/evidence journal
  purge [--all | --older DAYS]          clean up (no arg = prune stale)
  (all commands take --as NAME, --session SID and --json; identity resolves
   --as > session binding > $CLAUDEBUS_ID > tab-<ppid>)
"""


def main(argv=None):
    p = argparse.ArgumentParser(prog="claudebus", add_help=False)
    p.add_argument("--as", dest="as_", default=None)
    p.add_argument("--session", default=None)   # Claude session_id; env fallback
    sub = p.add_subparsers(dest="cmd")

    def add(name, fn, *opts):
        s = sub.add_parser(name, add_help=False)
        s.set_defaults(func=fn)
        return s

    s = add("send", cmd_send)
    s.add_argument("message", nargs="?"); s.add_argument("--to"); s.add_argument("--channel")

    s = add("recv", cmd_recv)
    s.add_argument("--sub"); s.add_argument("--since"); s.add_argument("--json", action="store_true")

    for nm, fn in (("wait", cmd_wait), ("watch", cmd_watch)):
        s = add(nm, fn)
        s.add_argument("--timeout", type=float, default=0); s.add_argument("--sub")
        s.add_argument("--since"); s.add_argument("--json", action="store_true")

    s = add("peek", cmd_peek)
    s.add_argument("-n", type=int, default=15); s.add_argument("--sub")
    s.add_argument("--json", action="store_true")

    s = add("claim", cmd_claim)
    s.add_argument("lane"); s.add_argument("--ttl", type=float, default=0)
    s.add_argument("--note"); s.add_argument("--steal", action="store_true")

    s = add("release", cmd_release)
    s.add_argument("lane"); s.add_argument("--force", action="store_true")

    s = add("lanes", cmd_lanes); s.add_argument("--json", action="store_true")

    s = add("ask", cmd_ask)
    s.add_argument("peer"); s.add_argument("question")
    s.add_argument("--timeout", type=float, default=30); s.add_argument("--json", action="store_true")

    s = add("reply", cmd_reply)
    s.add_argument("corr"); s.add_argument("answer", nargs="?")

    s = add("submit", cmd_submit)
    s.add_argument("task", nargs="?"); s.add_argument("--queue", default="default")
    s.add_argument("--prio", type=int, default=0)
    s.add_argument("--max-attempts", dest="max_attempts", type=int, default=3)
    s.add_argument("--after", default=None)
    s.add_argument("--json", action="store_true")

    s = add("take", cmd_take)
    s.add_argument("--queue", default="default"); s.add_argument("--lease", type=float, default=300)
    s.add_argument("--json", action="store_true")

    s = add("done", cmd_done); s.add_argument("id", type=int)

    s = add("fail", cmd_fail)
    s.add_argument("id", type=int); s.add_argument("--delay", type=float, default=None)

    s = add("retry", cmd_retry); s.add_argument("id", type=int)

    s = add("tasks", cmd_tasks)
    s.add_argument("--queue"); s.add_argument("--dlq", action="store_true")
    s.add_argument("--json", action="store_true")

    s = add("pipeline", cmd_pipeline)
    s.add_argument("steps", nargs="+"); s.add_argument("--queue", default="default")
    s.add_argument("--prio", type=int, default=0)
    s.add_argument("--max-attempts", dest="max_attempts", type=int, default=3)
    s.add_argument("--json", action="store_true")

    s = add("cfp", cmd_cfp)
    s.add_argument("task"); s.add_argument("--cap"); s.add_argument("--window", type=float, default=0)
    s.add_argument("--json", action="store_true")

    s = add("bid", cmd_bid)
    s.add_argument("corr"); s.add_argument("score", type=float)
    s.add_argument("note", nargs="?")

    s = add("bids", cmd_bids)
    s.add_argument("corr"); s.add_argument("--json", action="store_true")

    s = add("award", cmd_award)
    s.add_argument("corr"); s.add_argument("--to")
    s.add_argument("--json", action="store_true")

    s = add("bb", cmd_bb)
    s.add_argument("op", choices=["set", "get", "del", "list"])
    s.add_argument("key", nargs="?"); s.add_argument("value", nargs="?")
    s.add_argument("--ttl", type=float, default=0)
    s.add_argument("--if-ver", dest="if_ver", type=int, default=None)
    s.add_argument("--prefix"); s.add_argument("--json", action="store_true")

    s = add("caps", cmd_caps)
    s.add_argument("list", nargs="?", default=None)
    s.add_argument("--json", action="store_true")

    s = add("discover", cmd_discover)
    s.add_argument("cap"); s.add_argument("--json", action="store_true")

    s = add("elect", cmd_elect)
    s.add_argument("role"); s.add_argument("--ttl", type=float, default=120)
    s.add_argument("--json", action="store_true")

    s = add("leader", cmd_leader)
    s.add_argument("role"); s.add_argument("--json", action="store_true")

    s = add("spawn", cmd_spawn)
    s.add_argument("task", nargs="?"); s.add_argument("--name")
    s.add_argument("--agent", default="claude", choices=list(AGENTS))
    s.add_argument("--model", default=SPAWN_MODEL); s.add_argument("--dir")
    s.add_argument("--transport", default="tmux",
                   choices=["tmux", "app-server"])
    s.add_argument("--perms", default=None,   # None => per-agent safe default
                   choices=["default", "auto", "sandbox", "acceptEdits",
                            "dontAsk", "plan", "bypassPermissions"])
    s.add_argument("--wait", action="store_true")
    s.add_argument("--timeout", type=float, default=600)
    s.add_argument("--dry-run", dest="dry_run", action="store_true")
    s.add_argument("--json", action="store_true")

    s = add("tell", cmd_tell)
    s.add_argument("name"); s.add_argument("message", nargs="?")

    s = add("sessions", cmd_sessions)
    s.add_argument("--json", action="store_true")
    s.add_argument("--all", action="store_true")

    s = add("tree", cmd_tree)
    s.add_argument("--json", action="store_true")
    s.add_argument("--all", action="store_true")

    s = add("kill", cmd_kill); s.add_argument("name")

    s = add("respawn", cmd_respawn)
    s.add_argument("name")
    s.add_argument("--dry-run", dest="dry_run", action="store_true")
    s.add_argument("--json", action="store_true")

    s = add("join", cmd_join)
    s.add_argument("--announce", action="store_true"); s.add_argument("--note")
    s.add_argument("--json", action="store_true")

    s = add("hello", cmd_hello)
    s.add_argument("--name"); s.add_argument("--announce", action="store_true")
    s.add_argument("--note"); s.add_argument("--caps")
    s.add_argument("--json", action="store_true")

    s = add("leave", cmd_leave); s.add_argument("--announce", action="store_true")

    s = add("pulse", cmd_pulse)
    s.add_argument("--mark"); s.add_argument("--sub")

    s = add("armed", cmd_armed)

    s = add("listener", cmd_listener)
    s.add_argument("mode", choices=["on", "off", "status"])
    s.add_argument("--global", dest="glob", action="store_true")

    s = add("ps", cmd_ps); s.add_argument("--json", action="store_true")
    s = add("stats", cmd_stats); s.add_argument("--json", action="store_true")

    s = add("endpoints", cmd_endpoints)
    s.add_argument("--provider"); s.add_argument("--json", action="store_true")

    s = add("events", cmd_events)
    s.add_argument("-n", type=int, default=50); s.add_argument("--type")
    s.add_argument("--corr"); s.add_argument("--json", action="store_true")

    s = add("purge", cmd_purge)
    s.add_argument("--all", action="store_true"); s.add_argument("--older", type=float)

    for h in ("help", "-h", "--help"):
        sub.add_parser(h, add_help=False).set_defaults(func=lambda a: print(HELP))

    args = p.parse_args(argv)
    if not getattr(args, "cmd", None):
        print(HELP); return 0
    # read-only observer commands: with an auto-minted identity these must not
    # register presence (see heartbeat) — polling dashboards were flooding the
    # peers table with one ghost tab-* row per probe
    global _OBSERVER_CMD
    _OBSERVER_CMD = (
        args.cmd in ("peek", "ps", "stats", "lanes", "sessions", "tree",
                     "endpoints", "events", "tasks", "bids", "leader",
                     "armed", "discover")
        or (args.cmd == "bb" and args.op in ("get", "list"))
        or (args.cmd == "caps" and not args.list)
    )
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
