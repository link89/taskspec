"""Execution roles and compatibility with pre-refactor file-backed records."""
import asyncio
import json
import os
from pathlib import Path
import tempfile
import unittest

from taskspec.schema import SpecData, TaskInput, TaskState, SlurmJobData
from taskspec.service.spec import SpecService
from taskspec.task import Task
from taskspec.worker import Worker
from test_spec_service import DummyConnector, DummyExecutor


class RecordingRunner:
    def __init__(self, fail=False):
        self.submissions = []
        self.queries = []
        self.fail = fail
        self.result = TaskState.SUCCEEDED

    async def submit(self, run, env=None):
        self.submissions.append((run, env))
        if self.fail:
            raise ValueError("submission failed")
        run.set_slurm_job(SlurmJobData(id="42", state="PENDING"))
        run.set_state(TaskState.SUBMITTED)
        return run

    async def query_state(self, run):
        self.queries.append(run)
        return self.result


class TestRunRoles(unittest.IsolatedAsyncioTestCase):
    def make_service(self, directory, pool=False, runner=None):
        mode = {"worker_pool": {"min_workers": 1, "entrypoint": "sbatch worker.sh"}} if pool else {
            "on_demand": {"entrypoint": "sbatch task.sh"}}
        spec = SpecData(name="demo", executor="local", **mode)
        service = SpecService("demo", directory, spec,
                              DummyExecutor(DummyConnector(directory), runner),
                              "http://localhost")
        return service

    async def test_on_demand_submits_task_and_signals_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = RecordingRunner()
            service = self.make_service(directory, runner=runner)
            task = await service.create_task(TaskInput())
            self.assertIs(runner.submissions[0][0], task)
            self.assertIsNone(runner.submissions[0][1])
            self.assertIn(task.id, service._active_tasks)
            event = service._task_done_events[task.id]
            waiter = asyncio.create_task(service.wait_for_terminated(task.id, 1))
            await service.poll_state()
            await waiter
            self.assertTrue(event.is_set())
            self.assertEqual(task.state, TaskState.SUCCEEDED)
            self.assertFalse(os.path.exists(task.get_active_file()))
            self.assertNotIn(task.id, service._active_tasks)
            saved = json.loads(Path(task.get_data_file()).read_text())
            self.assertEqual(saved["state"], 2)
            self.assertEqual(saved["slurm_job"], {"id": "42", "state": "PENDING"})

    async def test_pool_queues_task_but_submits_independent_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = RecordingRunner()
            service = self.make_service(directory, pool=True, runner=runner)
            task = await service.create_task(TaskInput())
            self.assertEqual(runner.submissions, [])
            self.assertEqual(task.state, TaskState.IDLE)
            with self.assertRaisesRegex(ValueError, "must be queued"):
                task.get_entrypoint()
            self.assertEqual(service._queue.qsize(), 1)
            item = await service.get_from_queue(wait_s=1)
            self.assertEqual(item["id"], task.id)
            self.assertEqual(Path(task.get_qstate_file()).read_text(), "dequeue")
            worker = await service._create_worker()
            self.assertIsInstance(worker, Worker)
            self.assertNotIsInstance(worker, Task)
            self.assertFalse(hasattr(worker, "enqueue"))
            self.assertIs(runner.submissions[0][0], worker)
            self.assertEqual(runner.submissions[0][1], {
                "__TASK_QUEUE_URL": "http://localhost/specs/demo/queue/",
                "__TASK_QUEUE_TOKEN": service.get_queue_token(),
            })
            self.assertEqual(worker.get_entrypoint(), "sbatch worker.sh")
            self.assertIn("/workers/", worker.get_data_file())
            self.assertEqual(Path(worker.get_input_file()).name, "input.json")
            await service.poll_state()
            self.assertEqual(runner.queries, [worker])
            self.assertEqual(task.state, TaskState.IDLE)
            await service.complete_task(task.id, "ok")
            self.assertEqual(task.state, TaskState.SUCCEEDED)
            self.assertFalse(service._active_workers)

    async def test_deferred_task_is_prepared_without_submission(self):
        for pool in (False, True):
            with self.subTest(pool=pool), tempfile.TemporaryDirectory() as directory:
                runner = RecordingRunner()
                service = self.make_service(directory, pool=pool, runner=runner)
                task = await service.create_task(TaskInput(submit=False))
                self.assertEqual(task.state, TaskState.IDLE)
                self.assertEqual(runner.submissions, [])
                self.assertTrue(service._queue.empty())
                self.assertTrue(Path(task.get_input_file()).is_file())
                self.assertTrue(Path(task.get_active_file()).is_file())

    async def test_submission_failure_persists_error_and_removes_active_marker(self):
        for pool in (False, True):
            with self.subTest(pool=pool), tempfile.TemporaryDirectory() as directory:
                runner = RecordingRunner(fail=True)
                service = self.make_service(directory, pool=pool, runner=runner)
                with self.assertLogs("taskspec.service.spec", level="ERROR"):
                    with self.assertRaisesRegex(ValueError, "submission failed"):
                        if pool:
                            await service._create_worker()
                        else:
                            await service.create_task(TaskInput())
                run = runner.submissions[0][0]
                self.assertEqual(run.state, TaskState.ERROR)
                self.assertEqual(json.loads(Path(run.get_data_file()).read_text())["state"], 4)
                self.assertFalse(Path(run.get_active_file()).exists())
                self.assertFalse(service._active_tasks | service._active_workers)

    async def test_legacy_records_recover_and_round_trip_without_migration(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = RecordingRunner()
            service = self.make_service(directory, runner=runner)
            records = {}
            # Literal old JSON and paths, rather than writing fixtures with new classes.
            for subdir, state in (("tasks", 1), ("workers", 1)):
                meta = Path(directory, subdir, "AB", "1234", ".meta")
                meta.mkdir(parents=True)
                raw = {"id": "AB1234", "state": state, "created_at": 10,
                       "updated_at": 20, "slurm_job": {"id": "99", "state": "RUNNING"}}
                (meta / "task.json").write_text(json.dumps(raw))
                (meta / "input.json").write_text('{"idempotent_key":"old","files":[],"submit":true}')
                (meta / "active").touch()
                records[subdir] = raw
            service._load_active_tasks()
            task = service.get_task("AB1234")
            worker = service.get_worker("AB1234")
            self.assertIsInstance(task, Task)
            self.assertIsInstance(worker, Worker)
            self.assertIsNot(task, worker)
            self.assertIs(service.get_task(task.id), task)
            self.assertIs(service.get_worker(worker.id), worker)
            self.assertEqual(service._active_tasks, {"AB1234"})
            self.assertEqual(service._active_workers, {"AB1234"})
            self.assertIn(task.id, service._task_done_events)
            for record in (task, worker):
                self.assertEqual(record.get_input().idempotent_key, "old")
                self.assertEqual(record.data.full_task_dir, "")
                record.save()
                self.assertEqual(json.loads(Path(record.get_data_file()).read_text()),
                                 {**records[record.subdir], "full_task_dir": ""})
            await service.poll_state()
            self.assertEqual(runner.queries, [task, worker])
            self.assertFalse(service._active_tasks | service._active_workers)
            # Restarting does not resurrect either terminal execution.
            restarted = self.make_service(directory)
            restarted._load_active_tasks()
            self.assertFalse(restarted._active_tasks | restarted._active_workers)

    async def test_task_and_worker_prepare_different_files(self):
        with tempfile.TemporaryDirectory() as directory:
            service = self.make_service(directory, pool=True, runner=RecordingRunner())
            Path(directory, "task.txt").write_text("task data")
            Path(directory, "worker.sh").write_text("worker program")
            service._spec_data = SpecData(**{
                **service._spec_data.model_dump(),
                "files": ["task.txt"],
                "worker_pool": {"min_workers": 1, "entrypoint": "worker.sh", "files": ["worker.sh"]},
            })
            task = await service.create_task(TaskInput(submit=False, files=[
                {"name": "input.txt", "content": "generated"}]))
            worker = await service._create_worker()
            remote = Path(directory)
            self.assertEqual((remote / task.get_prefix() / "task.txt").read_text(), "task data")
            self.assertEqual((remote / task.get_prefix() / "input.txt").read_text(), "generated")
            self.assertFalse((remote / task.get_prefix() / "worker.sh").exists())
            self.assertEqual((remote / worker.get_prefix() / "worker.sh").read_text(), "worker program")
            self.assertFalse((remote / worker.get_prefix() / "task.txt").exists())
