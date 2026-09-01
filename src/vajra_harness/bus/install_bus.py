#!/usr/bin/env python3
"""Transactional installer and rollback tool for the staged Claude Bus v5.

The tool is deliberately inert unless the operator selects ``apply`` or
``rollback``.  It snapshots the live SQLite database through SQLite's backup
API (so WAL state is included), records source hashes, installs each file with
an atomic rename, and leaves an exact rollback manifest.  Source rollback keeps
the current database by default because replacing it would discard messages
created after activation; restoring the database requires an explicit flag.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import uuid


DEFAULT_SOURCE = Path(__file__).resolve().parent
DEFAULT_TARGET = Path.home() / ".claude" / "claudebus"
DEFAULT_BACKUP_ROOT = Path.home() / ".claude" / "claudebus-backups"
# Files copied into the target install. Estate-specific bridges (atlas, capos)
# are deliberately absent: the bus runs standalone without them.
BUNDLE = (
    "claudebus.py",
    "hook.py",
    "hook-join.sh",
    "hook-recv.sh",
    "codex_hook.py",
    "codex_worker.py",
    "install_codex_hooks.py",
    "install_bus.py",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(raw_tmp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        fsync_dir(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_copy(source: Path, target: Path, *, mode: int | None = None) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_tmp = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    os.close(fd)
    tmp = Path(raw_tmp)
    try:
        shutil.copy2(source, tmp)
        if mode is not None:
            os.chmod(tmp, mode)
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, target)
        fsync_dir(target.parent)
    finally:
        tmp.unlink(missing_ok=True)


def validate_bundle(source: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for name in BUNDLE:
        path = source / name
        if not path.is_file():
            raise RuntimeError(f"bundle file missing: {path}")
        if path.suffix == ".py":
            compile(path.read_text(encoding="utf-8"), str(path), "exec")
        hashes[name] = sha256(path)
    return hashes


def database_state(path: Path) -> dict:
    if not path.exists():
        return {"exists": False, "user_version": None, "integrity": None}
    with sqlite3.connect(path) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    return {"exists": True, "user_version": version, "integrity": integrity}


def snapshot_database(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as origin, sqlite3.connect(destination) as copy:
        origin.backup(copy)
        copy.commit()
        if copy.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite backup failed integrity_check")
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())
    os.chmod(destination, 0o600)


def remove_sqlite_sidecars(database: Path) -> None:
    Path(f"{database}-wal").unlink(missing_ok=True)
    Path(f"{database}-shm").unlink(missing_ok=True)


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def live_waiters(target: Path) -> list[dict]:
    waiters = target / "waiters"
    if not waiters.is_dir():
        return []
    result = []
    for path in waiters.iterdir():
        if path.suffix != ".fifo":
            continue
        stem = path.name[:-5]
        identity, separator, raw_pid = stem.rpartition(".")
        if not separator or not raw_pid.isdigit():
            continue
        pid = int(raw_pid)
        if pid_alive(pid):
            result.append({"identity": identity, "pid": pid, "path": str(path)})
    return sorted(result, key=lambda item: (item["identity"], item["pid"]))


def current_hashes(target: Path) -> dict[str, str | None]:
    return {
        name: (sha256(target / name) if (target / name).is_file() else None)
        for name in BUNDLE
    }


def current_modes(target: Path) -> dict[str, int | None]:
    return {
        name: ((target / name).stat().st_mode & 0o777 if (target / name).is_file() else None)
        for name in BUNDLE
    }


def plan(source: Path, target: Path) -> dict:
    return {
        "action": "plan",
        "source": str(source),
        "target": str(target),
        "bundle_sha256": validate_bundle(source),
        "target_sha256": current_hashes(target),
        "database": database_state(target / "bus.db"),
        "live_waiters": live_waiters(target),
        "mutated": False,
    }


@contextlib.contextmanager
def install_lock(target: Path):
    target.mkdir(parents=True, exist_ok=True)
    lock_path = target / ".v5-install.lock"
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def backup_name() -> str:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"v5-{stamp}-{uuid.uuid4().hex[:8]}"


def private_directory(path: Path, *, create: bool = True) -> None:
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.is_dir():
        raise RuntimeError(f"private path is not a directory: {path}")
    if path.stat().st_uid != os.getuid():
        raise RuntimeError(f"private directory is not owned by current user: {path}")
    os.chmod(path, 0o700)


def require_private_file(path: Path, label: str) -> None:
    if not path.is_file():
        raise RuntimeError(f"{label} missing: {path}")
    state = path.stat()
    if state.st_uid != os.getuid():
        raise RuntimeError(f"{label} is not owned by current user: {path}")
    if state.st_mode & 0o022:
        raise RuntimeError(f"{label} is group/world writable: {path}")


def validated_manifest(manifest_path: Path) -> dict:
    require_private_file(manifest_path, "rollback manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != 1:
        raise RuntimeError("unsupported rollback manifest format")
    if manifest.get("bundle") != list(BUNDLE):
        raise RuntimeError("rollback manifest bundle does not match the v5 bundle")
    target = Path(manifest.get("target", ""))
    if not target.is_absolute() or target == Path("/"):
        raise RuntimeError("rollback manifest target must be a safe absolute path")
    before = manifest.get("target_before_sha256")
    if not isinstance(before, dict) or set(before) != set(BUNDLE):
        raise RuntimeError("rollback manifest source snapshot is incomplete")
    modes = manifest.get("target_before_mode")
    if not isinstance(modes, dict) or set(modes) != set(BUNDLE):
        raise RuntimeError("rollback manifest source modes are incomplete")
    saved = manifest_path.parent / "files"
    private_directory(manifest_path.parent, create=False)
    private_directory(saved, create=False)
    for name in BUNDLE:
        digest = before[name]
        mode = modes[name]
        if digest is None:
            if mode is not None:
                raise RuntimeError(f"rollback source mode exists without a file: {name}")
            continue
        if not isinstance(mode, int) or not 0 <= mode <= 0o777:
            raise RuntimeError(f"rollback source mode is invalid: {name}")
        path = saved / name
        require_private_file(path, f"rollback source {name}")
        if sha256(path) != digest:
            raise RuntimeError(f"rollback source hash mismatch: {name}")
    raw_backup = manifest.get("database_backup")
    if raw_backup is not None:
        database_backup = Path(raw_backup)
        if database_backup.resolve() != (manifest_path.parent / "bus.db").resolve():
            raise RuntimeError("rollback database path escapes its backup bundle")
        require_private_file(database_backup, "rollback database")
        expected = manifest.get("database_backup_sha256")
        if not expected or sha256(database_backup) != expected:
            raise RuntimeError("rollback database hash mismatch")
        if database_state(database_backup)["integrity"] != "ok":
            raise RuntimeError("rollback database failed integrity_check")
    return manifest


def restore_sources(manifest: dict, manifest_path: Path) -> None:
    target = Path(manifest["target"])
    saved = manifest_path.parent / "files"
    before = manifest["target_before_sha256"]
    modes = manifest["target_before_mode"]
    for name in manifest.get("bundle", BUNDLE):
        destination = target / name
        if before.get(name) is None:
            destination.unlink(missing_ok=True)
        else:
            atomic_copy(saved / name, destination, mode=modes[name])
    if current_hashes(target) != before or current_modes(target) != modes:
        raise RuntimeError("restored source files differ from the rollback snapshot")


def migrate_and_verify(target: Path) -> dict:
    environment = dict(os.environ)
    environment["CLAUDEBUS_HOME"] = str(target)
    environment["CLAUDEBUS_ID"] = "v5-installer"
    process = subprocess.run(
        [sys.executable, str(target / "claudebus.py"), "stats"],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if process.returncode != 0:
        raise RuntimeError(
            "installed bus failed migration/status probe: "
            + (process.stderr.strip() or process.stdout.strip())
        )
    state = database_state(target / "bus.db")
    if state["integrity"] != "ok":
        raise RuntimeError("installed bus database failed integrity_check")
    return state


def apply(source: Path, target: Path, backup_root: Path, allow_active: bool) -> dict:
    source_hashes = validate_bundle(source)
    with install_lock(target):
        if validate_bundle(source) != source_hashes:
            raise RuntimeError("source bundle changed while activation lock was acquired")
        waiters = live_waiters(target)
        if waiters and not allow_active:
            raise RuntimeError(
                "live bus listeners must be drained before activation: "
                + ", ".join(f"{w['identity']}:{w['pid']}" for w in waiters)
            )
        before_hashes = current_hashes(target)
        before_modes = current_modes(target)
        db_path = target / "bus.db"
        db_before = database_state(db_path)
        if db_before["exists"] and db_before["integrity"] != "ok":
            raise RuntimeError("existing bus database failed integrity_check")
        private_directory(backup_root)
        backup_dir = backup_root / backup_name()
        files_dir = backup_dir / "files"
        backup_dir.mkdir(mode=0o700, exist_ok=False)
        files_dir.mkdir(mode=0o700)
        for name, digest in before_hashes.items():
            if digest is not None:
                atomic_copy(target / name, files_dir / name, mode=0o600)
                if sha256(files_dir / name) != digest:
                    raise RuntimeError(f"source backup hash mismatch: {name}")
        database_backup = None
        if db_before["exists"]:
            database_backup = backup_dir / "bus.db"
            snapshot_database(db_path, database_backup)

        manifest_path = backup_dir / "manifest.json"
        manifest = {
            "format": 1,
            "status": "prepared",
            "bundle": list(BUNDLE),
            "source": str(source),
            "target": str(target),
            "source_sha256": source_hashes,
            "target_before_sha256": before_hashes,
            "target_before_mode": before_modes,
            "database_before": db_before,
            "database_backup": str(database_backup) if database_backup else None,
            "database_backup_sha256": sha256(database_backup) if database_backup else None,
            "live_waiters_at_apply": waiters,
            "rollback_source_command": (
                f"{shlex_quote(sys.executable)} {shlex_quote(str(target / 'install_bus.py'))} "
                f"rollback --manifest {shlex_quote(str(manifest_path))}"
            ),
            "rollback_database_command": (
                f"{shlex_quote(sys.executable)} {shlex_quote(str(target / 'install_bus.py'))} "
                f"rollback --manifest {shlex_quote(str(manifest_path))} --restore-database"
            ),
        }
        atomic_json(manifest_path, manifest)
        try:
            for name in BUNDLE:
                atomic_copy(source / name, target / name)
            installed_hashes = current_hashes(target)
            if installed_hashes != source_hashes:
                raise RuntimeError("installed source hashes differ from the validated bundle")
            after_db = migrate_and_verify(target)
            manifest.update({
                "status": "activated",
                "installed_sha256": installed_hashes,
                "database_after": after_db,
            })
            atomic_json(manifest_path, manifest)
        except Exception:
            restore_sources(manifest, manifest_path)
            if database_backup:
                remove_sqlite_sidecars(db_path)
                atomic_copy(database_backup, db_path)
                restored_hash = sha256(db_path)
                restored_state = database_state(db_path)
                remove_sqlite_sidecars(db_path)
                if (restored_hash != manifest["database_backup_sha256"]
                        or restored_state["integrity"] != "ok"):
                    raise RuntimeError("automatic database rollback verification failed")
            elif not db_before["exists"]:
                remove_sqlite_sidecars(db_path)
                db_path.unlink(missing_ok=True)
            manifest["status"] = "activation_failed_rolled_back"
            atomic_json(manifest_path, manifest)
            raise
        return {"ok": True, "action": "apply", "manifest": str(manifest_path), **manifest}


def shlex_quote(value: str) -> str:
    import shlex
    return shlex.quote(value)


def preserve_failed_state(target: Path, destination: Path) -> dict:
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    files = destination / "files"
    files.mkdir(mode=0o700)
    for name in BUNDLE:
        path = target / name
        if path.is_file():
            atomic_copy(path, files / name, mode=0o600)
    db_path = target / "bus.db"
    if db_path.exists():
        snapshot_database(db_path, destination / "bus.db")
    evidence = {
        "target_sha256": current_hashes(target),
        "database": database_state(db_path),
    }
    atomic_json(destination / "state.json", evidence)
    return evidence


def rollback(manifest_path: Path, restore_database: bool, allow_active: bool) -> dict:
    manifest = validated_manifest(manifest_path)
    target = Path(manifest["target"])
    raw_backup = manifest.get("database_backup")
    if restore_database and not raw_backup:
        raise RuntimeError("manifest has no pre-activation database backup")
    with install_lock(target):
        waiters = live_waiters(target)
        if waiters and not allow_active:
            raise RuntimeError(
                "live bus listeners must be drained before rollback: "
                + ", ".join(f"{w['identity']}:{w['pid']}" for w in waiters)
            )
        failure_dir = manifest_path.parent / f"failed-state-{backup_name()}"
        failed = preserve_failed_state(target, failure_dir)
        restore_sources(manifest, manifest_path)
        database_restored = False
        if restore_database:
            restored_database = target / "bus.db"
            remove_sqlite_sidecars(restored_database)
            atomic_copy(Path(raw_backup), restored_database)
            restored_hash = sha256(restored_database)
            restored_state = database_state(restored_database)
            remove_sqlite_sidecars(restored_database)
            if (restored_hash != manifest["database_backup_sha256"]
                    or restored_state["integrity"] != "ok"):
                raise RuntimeError("restored database failed integrity_check")
            database_restored = True
        elif database_state(target / "bus.db")["integrity"] != "ok":
            raise RuntimeError("preserved database failed integrity_check")
        manifest.update({
            "status": "rolled_back",
            "rollback_failed_state": str(failure_dir),
            "rollback_database_restored": database_restored,
        })
        atomic_json(manifest_path, manifest)
        return {
            "ok": True,
            "action": "rollback",
            "manifest": str(manifest_path),
            "failed_state": str(failure_dir),
            "failed_state_evidence": failed,
            "database_restored": database_restored,
            "database_preserved_reason": (
                None if database_restored
                else "default preserves post-activation messages; use --restore-database explicitly"
            ),
        }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    sub = result.add_subparsers(dest="action", required=True)
    for name in ("plan", "apply"):
        command = sub.add_parser(name)
        command.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
        command.add_argument("--target", type=Path, default=DEFAULT_TARGET)
        if name == "apply":
            command.add_argument("--backup-root", type=Path, default=DEFAULT_BACKUP_ROOT)
            command.add_argument("--allow-active", action="store_true")
    command = sub.add_parser("rollback")
    command.add_argument("--manifest", type=Path, required=True)
    command.add_argument("--restore-database", action="store_true")
    command.add_argument("--allow-active", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.action == "plan":
            result = plan(args.source.resolve(), args.target.resolve())
        elif args.action == "apply":
            result = apply(
                args.source.resolve(), args.target.resolve(),
                args.backup_root.resolve(), args.allow_active,
            )
        else:
            result = rollback(
                args.manifest.resolve(), args.restore_database, args.allow_active,
            )
    except Exception as exc:
        print(json.dumps({"ok": False, "action": args.action, "error": str(exc)},
                         sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
