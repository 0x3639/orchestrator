#!/usr/bin/env python3
"""Create and restore offline orchestrator sync checkpoints (Linux, Python 3)."""

import argparse
from contextlib import ExitStack, contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import uuid


ROOTS = ("events", "queues")
PENDING = ".sync-restore-pending"


def require_directory(path):
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise ValueError(f"expected a directory at {path}")


def sync_dir(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def move(source, destination):
    source.rename(destination)
    sync_dir(destination.parent)
    if source.parent != destination.parent:
        sync_dir(source.parent)


def read_json(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"expected a regular file at {path}")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object at {path}")
    return value


def write_json(path, value):
    with path.open("x") as out:
        json.dump(value, out, sort_keys=True)
        out.flush()
        os.fsync(out.fileno())


def blocked_lock(lock):
    return lock.is_symlink() and os.readlink(lock) == "." and lock.with_name(".sync-restore-lock").is_file()


def block_start(lock):
    # Upstream opens .lock O_RDWR|O_CREAT, which fails on a directory even
    # for root. Keep the original flock inode so a killed restore can resume
    # without letting a reboot start a signer with a mixture of the two trees.
    if blocked_lock(lock):
        return
    saved = lock.with_name(".sync-restore-lock")
    if not os.path.lexists(saved):
        os.link(lock, saved)
        sync_dir(lock.parent)
    elif not saved.samefile(lock):
        raise ValueError(f"unexpected restore lock at {saved}")
    with tempfile.TemporaryDirectory(dir=lock.parent) as tmp:
        marker = Path(tmp) / "stopped"
        marker.symlink_to(".")
        os.replace(marker, lock)
        sync_dir(lock.parent)


@contextmanager
def stopped_signer(data):
    require_directory(data)
    if data.stat().st_uid != os.geteuid():
        raise ValueError(f"run as the service user who owns {data}")
    # The release stores events and queues under the service user's home,
    # but takes its instance lock in config.DataPath if that was overridden.
    config = data / "config.json"
    lock_dir = data
    if os.path.lexists(config):
        # Go's encoding/json matches config struct fields case-insensitively.
        settings = {key.lower(): value for key, value in read_json(config).items()}
        configured = settings.get("datapath")
        if configured:
            lock_dir = Path(configured)
            if not lock_dir.is_absolute():
                raise ValueError("config.DataPath must be absolute to locate the running signer's lock")
        tss = settings.get("tssconfig") or {}
        if not isinstance(tss, dict):
            raise ValueError("expected TssConfig to be a JSON object")
        tss_dir = {k.lower(): v for k, v in tss.items()}.get("basedir")
        producer = settings.get("producerkeyfilename") or "producer"
        if not isinstance(producer, str):
            raise ValueError("expected ProducerKeyFileName to be a string")
        # Go's path.Join joins even an absolute-looking key name to DataPath.
        protected = [lock_dir, lock_dir / producer.lstrip("/")]
        for path in (tss_dir, settings.get("producerkeyfilepassphrasefile")):
            if path:
                protected.append(Path(path))
        for path in protected:
            location = path.resolve()
            for name in ROOTS:
                root = data / name
                if location == root or root in location.parents:
                    raise ValueError(f"signer configuration or key material overlaps a sync directory at {location}")
    with ExitStack() as locks:
        for directory in sorted({data.resolve(), lock_dir.resolve(strict=True)}):
            lock = directory / ".lock"
            if blocked_lock(lock):
                if not (data / PENDING).is_dir():
                    raise ValueError(f"restore lock at {lock} has no matching journal")
                lock = lock.with_name(".sync-restore-lock")
            fd = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            locks.callback(os.close, fd)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError(f"{directory} is in use; stop the orchestrator before continuing") from None
        yield lock_dir / ".lock"


def scan_tree(source, destination=None, installed=None):
    """Return a relative-path inventory with SHA-256 for files, copying if requested."""
    entries = {}

    def visit(path, relative):
        mode = path.lstat().st_mode
        target = destination / relative if destination is not None else None
        if stat.S_ISDIR(mode):
            entries[str(relative)] = None
            if target is not None:
                target.mkdir(mode=0o700)
            for child in sorted(path.iterdir()):
                visit(child, relative / child.name)
            if target is not None:
                sync_dir(target)
        elif stat.S_ISREG(mode):
            digest = hashlib.sha256()
            with ExitStack() as files:
                src = files.enter_context(path.open("rb"))
                dst = files.enter_context(target.open("xb")) if target is not None else None
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    digest.update(chunk)
                    if dst is not None:
                        dst.write(chunk)
                if dst is not None:
                    dst.flush()
                    os.fsync(dst.fileno())
            entries[str(relative)] = digest.hexdigest()
        else:
            raise ValueError(f"unexpected link or special file at {path}")

    for name in ROOTS:
        tree = source / name
        if installed is not None and not os.path.lexists(tree):
            tree = installed / name
        require_directory(tree)
        visit(tree, Path(name))
    if not entries.get("events/Zenon/CURRENT"):
        raise ValueError("missing Zenon LevelDB CURRENT in sync data")
    if destination is not None:
        sync_dir(destination)
    return entries


@contextmanager
def stage_for_publish(destination, prefix):
    # Python 3.6/3.7 clean up the original temporary directory by name.
    # Move its completed child so that the directory itself can be removed.
    with tempfile.TemporaryDirectory(prefix=prefix, dir=destination.parent) as tmp:
        stage = Path(tmp) / "stage"
        stage.mkdir(mode=0o700)
        yield stage
        sync_dir(stage)
        move(stage, destination)


def create(data, checkpoint):
    if os.path.lexists(checkpoint):
        raise ValueError(f"checkpoint already exists: {checkpoint.name}")
    with stage_for_publish(checkpoint, ".building-") as stage:
        entries = scan_tree(data, stage)
        write_json(stage / "manifest.json", {"version": 1, "entries": entries})


def checked_copy(checkpoint, destination=None):
    require_directory(checkpoint)
    manifest = read_json(checkpoint / "manifest.json")
    if manifest.get("version") != 1 or scan_tree(checkpoint, destination) != manifest.get("entries"):
        raise ValueError(f"checkpoint integrity check failed: {checkpoint}")


def restore(data, checkpoint, lock):
    pending = data / PENDING
    resuming = os.path.lexists(pending)
    if not resuming:
        for name in ROOTS:
            live = data / name
            if os.path.lexists(live):
                require_directory(live)
                if os.path.ismount(live):
                    raise ValueError(f"cannot rename mount point {live}")
        # Verify and fully stage both stores before moving either active tree.
        with stage_for_publish(pending, ".restoring-") as stage:
            (stage / "incoming").mkdir()
            (stage / "previous").mkdir()
            checked_copy(checkpoint, stage / "incoming")
            write_json(stage / "restore.json", {
                "checkpoint": checkpoint.name,
                "previous": "pre-restore-" + uuid.uuid4().hex,
            })
    require_directory(pending)
    state = read_json(pending / "restore.json")
    if state["checkpoint"] != checkpoint.name:
        raise ValueError(f"finish the interrupted restore of {state['checkpoint']} first")
    previous = state["previous"]
    if not re.fullmatch(r"pre-restore-[0-9a-f]{32}", previous):
        raise ValueError("invalid restore journal")
    retained = data / previous
    if resuming and not blocked_lock(lock) and not any((pending / "incoming").iterdir()):
        # Both stores were activated and startup was unblocked. A later
        # service run may have advanced the cursor before this final rename.
        move(pending, retained)
        return retained / "previous"
    if resuming:
        entries = scan_tree(pending / "incoming", installed=data)
        if entries != read_json(checkpoint / "manifest.json")["entries"]:
            raise ValueError(f"interrupted restore differs from checkpoint; inspect {pending}")
    block_start(lock)
    # Each rename is durable. The presence of an incoming tree tells us
    # whether that step is still pending after an interruption.
    for name in ROOTS:
        incoming = pending / "incoming" / name
        live = data / name
        old = pending / "previous" / name
        if os.path.lexists(incoming):
            require_directory(incoming)
            if os.path.lexists(live):
                if os.path.lexists(old):
                    raise ValueError(f"{live} reappeared during restore; keep the service stopped and inspect {pending}")
                move(live, old)
            move(incoming, live)
        else:
            require_directory(live)
    # Both trees are now installed and durable. Put back the original,
    # still-locked inode before publishing the completed recovery directory.
    os.replace(lock.with_name(".sync-restore-lock"), lock)
    sync_dir(lock.parent)
    move(pending, retained)
    return retained / "previous"


def run(command, data, name):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,79}", name):
        raise ValueError("use a checkpoint name of up to 80 letters, digits, dots, underscores or hyphens")
    data = data.resolve(strict=True)
    backups = data / "sync-backups"
    checkpoint = backups / name
    if command == "verify":
        require_directory(backups)
        checked_copy(checkpoint)
        return f"Verified {checkpoint}"
    with stopped_signer(data) as lock:
        backups.mkdir(mode=0o700, exist_ok=True)
        require_directory(backups)
        sync_dir(data)
        if command == "create":
            if os.path.lexists(data / PENDING):
                state = read_json(data / PENDING / "restore.json")
                raise ValueError(f"restore interrupted; finish restoring {state['checkpoint']}")
            create(data, checkpoint)
            return f"Created {checkpoint}"
        previous = restore(data, checkpoint, lock)
        return f"Restored {checkpoint}. Previous sync data retained in {previous}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("create", "verify", "restore"))
    parser.add_argument("name", help="local checkpoint name")
    parser.add_argument("--data-dir", type=Path, default=Path.home() / ".orchestrator",
                        help="directory containing the signer's events/ and queues/")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        print(run(args.command, args.data_dir, args.name))
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"Checkpoint failed: {error}\n")


if __name__ == "__main__":
    main()
