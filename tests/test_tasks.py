import tempfile
import unittest
from pathlib import Path

from agent_bridge.store import Store


class TaskStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tempdir.name) / "bridge.sqlite3")
        self.lead = self.store.create_run(
            name="lead", agent="hermes", mode="interactive", command="hermes", cwd="."
        )
        self.worker = self.store.create_run(
            name="worker", agent="pi", mode="interactive", command="pi", cwd="."
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_dependencies_block_claim_until_parent_completes(self) -> None:
        first = self.store.create_task(
            title="inspect schema",
            description="Find the migration details.",
            created_by=self.lead.id,
        )
        second = self.store.create_task(
            title="update worker",
            description="Implement the verified schema change.",
            created_by=self.lead.id,
            depends_on=[first.id],
        )
        with self.assertRaises(ValueError):
            self.store.claim_task(second.id, self.worker.id)

        claimed = self.store.claim_task(first.id, self.worker.id)
        self.assertEqual(claimed.status, "in_progress")
        completed = self.store.complete_task(first.id, self.worker.id)
        self.assertEqual(completed.status, "completed")
        claimed_second = self.store.claim_task(second.id, self.worker.id)
        self.assertEqual(claimed_second.status, "in_progress")

    def test_task_description_is_bounded(self) -> None:
        with self.assertRaises(ValueError):
            self.store.create_task(
                title="bounded description",
                created_by=self.lead.id,
                description="x" * 16_385,
            )
        with self.assertRaises(ValueError):
            self.store.create_task(
                title="safe description",
                created_by=self.lead.id,
                description="unsafe\x00description",
            )

    def test_preassigned_task_can_only_be_claimed_by_assignee(self) -> None:
        task = self.store.create_task(
            title="assigned work",
            created_by=self.lead.id,
            assigned_to=self.worker.id,
        )
        with self.assertRaises(ValueError):
            self.store.claim_task(task.id, self.lead.id)
        claimed = self.store.claim_task(task.id, self.worker.id)
        self.assertEqual(claimed.assigned_to, self.worker.id)

    def test_concurrent_claim_is_atomic(self) -> None:
        import threading

        task = self.store.create_task(title="single owner", created_by=self.lead.id)
        other = self.store.create_run(
            name="other", agent="agy", mode="interactive", command="agy", cwd="."
        )
        barrier = threading.Barrier(2)
        results: list[str] = []

        def claim(run_id: str) -> None:
            barrier.wait()
            try:
                results.append(self.store.claim_task(task.id, run_id).assigned_to or "")
            except ValueError:
                results.append("rejected")

        threads = [
            threading.Thread(target=claim, args=(self.worker.id,)),
            threading.Thread(target=claim, args=(other.id,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(result in {self.worker.id, other.id} for result in results), 1)
        self.assertEqual(results.count("rejected"), 1)


if __name__ == "__main__":
    unittest.main()
