from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import sync_checkpoint as checkpoint


class CheckpointTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.data = Path(temporary.name).resolve()
        for name in ("events/Zenon", "events/Ethereum", "queues/Ethereum", "tss"):
            (self.data / name).mkdir(parents=True)
        self.cursor = self.data / "events/Ethereum/000001.log"
        self.queue = self.data / "queues/Ethereum/00001.dque"
        self.cursor.write_bytes(b"cursor and events")
        self.queue.write_bytes(b"pending unwrap")
        for name in ("Zenon", "Ethereum"):
            (self.data / "events" / name / "CURRENT").write_text("MANIFEST-000001\n")
        (self.data / "tss/share").write_bytes(b"test key")
        (self.data / "tss/unwrap-pool-votes").mkdir()
        self.votes = self.data / "tss/unwrap-pool-votes/000001.log"
        self.votes.write_bytes(b"first vote")
        (self.data / "producer").write_bytes(b"test producer")
        (self.data / "config.json").write_text("{}")
        self.snapshot = self.data / "sync-backups/healthy"

    def command(self, action, name="healthy"):
        return checkpoint.run(action, self.data, name)

    def test_create_and_restore_with_python_36_37_cleanup(self):
        @contextmanager
        def strict_tempdir(**kwargs):
            # These runtimes always rmtree the original name on context exit.
            directory = tempfile.mkdtemp(**kwargs)
            try:
                yield directory
            finally:
                shutil.rmtree(directory)

        with patch.object(tempfile, "TemporaryDirectory", strict_tempdir):
            self.command("create")
            self.cursor.write_bytes(b"later block")
            self.command("restore")
        self.assertEqual(self.cursor.read_bytes(), b"cursor and events")
        self.assertFalse((self.data / checkpoint.PENDING).exists())

    def test_checkpoint_round_trip(self):
        self.command("create")
        self.command("verify")
        self.assertEqual({p.name for p in self.snapshot.iterdir()}, {"events", "queues", "manifest.json"})
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.command("create")
        self.cursor.write_bytes(b"later block")
        self.queue.unlink()
        (self.data / "tss/share").write_bytes(b"current key")
        self.votes.write_bytes(b"latest vote")
        self.command("restore")
        self.assertEqual(self.cursor.read_bytes(), b"cursor and events")
        self.assertEqual(self.queue.read_bytes(), b"pending unwrap")
        self.assertEqual((self.data / "tss/share").read_bytes(), b"current key")
        self.assertEqual(self.votes.read_bytes(), b"latest vote")
        previous = next(self.data.glob("pre-restore-*/previous"))
        self.assertEqual((previous / "events/Ethereum/000001.log").read_bytes(), b"later block")
        self.assertFalse((previous / "queues/Ethereum/00001.dque").exists())
        self.command("verify")

    def test_damaged_checkpoint_leaves_live_state(self):
        self.command("create")
        self.cursor.write_bytes(b"active")
        (self.snapshot / "queues/Ethereum/00001.dque").write_bytes(b"damaged")
        with self.assertRaisesRegex(ValueError, "integrity"):
            self.command("restore")
        self.assertEqual(self.cursor.read_bytes(), b"active")
        self.assertEqual(self.queue.read_bytes(), b"pending unwrap")
        self.assertFalse((self.data / checkpoint.PENDING).exists())
        self.assertFalse((self.data / ".lock").is_symlink())

    def test_lock_matches_service_with_custom_data_path(self):
        lock_dir = self.data / "service"
        lock_dir.mkdir()
        (self.data / "config.json").write_text(json.dumps({"dataPath": str(lock_dir)}))
        self.command("create")
        with (lock_dir / ".lock").open("r+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.command("verify")
            for command in ("create", "restore"):
                with self.assertRaisesRegex(ValueError, "in use"):
                    self.command(command)

    def test_interrupted_restore_resumes_without_mixing_stores(self):
        self.command("create")
        self.cursor.write_bytes(b"old events")
        self.queue.write_bytes(b"old queue")
        instance_lock = self.data / ".lock"
        original_inode = instance_lock.stat().st_ino
        move = checkpoint.move

        def interrupt(source, destination):
            if source == self.data / "queues":
                raise OSError("simulated interruption")
            move(source, destination)

        with patch.object(checkpoint, "move", interrupt), self.assertRaisesRegex(OSError, "interruption"):
            self.command("restore")
        # Reproduce the release's O_RDWR|O_CREAT open after the tool exited.
        with self.assertRaises(IsADirectoryError):
            os.close(os.open(instance_lock, os.O_RDWR | os.O_CREAT, 0o644))
        with self.assertRaisesRegex(ValueError, "finish the interrupted restore"):
            self.command("restore", "another")
        self.command("restore")
        self.assertEqual(self.cursor.read_bytes(), b"cursor and events")
        self.assertEqual(self.queue.read_bytes(), b"pending unwrap")
        self.assertEqual(instance_lock.stat().st_ino, original_inode)
        self.assertFalse(instance_lock.is_symlink())
        previous = next(self.data.glob("pre-restore-*/previous"))
        self.assertEqual((previous / "queues/Ethereum/00001.dque").read_bytes(), b"old queue")
        self.assertEqual((previous / "events/Ethereum/000001.log").read_bytes(), b"old events")

    def test_interruption_after_unblocking_preserves_subsequent_sync(self):
        self.command("create")
        move = checkpoint.move

        def interrupt(source, destination):
            if source == self.data / checkpoint.PENDING:
                raise OSError("simulated interruption")
            move(source, destination)

        with patch.object(checkpoint, "move", interrupt), self.assertRaisesRegex(OSError, "interruption"):
            self.command("restore")
        self.assertFalse((self.data / ".lock").is_symlink())
        self.cursor.write_bytes(b"new progress after restored signer started")
        self.command("restore")
        self.assertEqual(self.cursor.read_bytes(), b"new progress after restored signer started")
        self.assertFalse((self.data / checkpoint.PENDING).exists())

    def test_reject_configured_signing_material_in_sync_trees(self):
        self.command("create")
        for config in ({"TssConfig": {"BaseDir": str(self.data / "events/keys")}},
                       {"ProducerKeyFileName": "queues/producer"}):
            (self.data / "config.json").write_text(json.dumps(config))
            for command in ("create", "restore"):
                with self.assertRaisesRegex(ValueError, "overlaps"):
                    self.command(command)

    def test_missing_both_stores_can_bootstrap(self):
        self.command("create")
        self.cursor.parents[1].rename(self.data / "broken-events")
        self.queue.parents[1].rename(self.data / "broken-queues")
        self.command("restore")
        self.assertEqual(self.queue.read_bytes(), b"pending unwrap")
        self.assertEqual(self.cursor.read_bytes(), b"cursor and events")

    def test_reject_links_and_paths_outside_backup_directory(self):
        (self.data / "events/Ethereum/key").symlink_to(self.data / "tss/share")
        with self.assertRaisesRegex(ValueError, "link or special file"):
            self.command("create")
        self.assertFalse(self.snapshot.exists())
        with self.assertRaisesRegex(ValueError, "checkpoint name"):
            self.command("restore", "../tss")


if __name__ == "__main__":
    unittest.main()
