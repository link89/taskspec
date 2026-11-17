import unittest

from taskspec.connector import CmdResult
from taskspec.runner import SlurmConfig, SlurmRunner
from taskspec.schema import SpecData, TaskData, TaskState
from taskspec.task import Task
from taskspec.worker import Worker


class DummyConnector:
    def __init__(self, base_dir: str, shell_result: CmdResult | list[CmdResult]):
        self._base_dir = base_dir
        self._shell_result = shell_result
        self.commands = []

    def get_base_dir(self) -> str:
        return self._base_dir

    async def load_text(self, path: str) -> str:
        self.state_path = path
        return "ok\n"

    async def shell(self, cmd: str) -> CmdResult:
        self.commands.append(cmd)
        if isinstance(self._shell_result, list):
            return self._shell_result.pop(0)
        return self._shell_result


class TestSlurmRunner(unittest.IsolatedAsyncioTestCase):
    def _make_task(self, spec: SpecData) -> Task:
        return Task(
            TaskData(id="AB1234", state=TaskState.IDLE, created_at=1, updated_at=1),
            spec,
            spec_dir="/local",
        )

    async def test_submit_registers_job_in_squeue_cache(self):
        connector = DummyConnector(
            "/remote",
            CmdResult(returncode=0, stdout="Submitted batch job 42\n", stderr=""),
        )
        runner = SlurmRunner(SlurmConfig(), connector)
        spec = SpecData(name="demo", executor="slurm", on_demand={"entrypoint": "sbatch run.sh"})
        task = self._make_task(spec)

        updated_task = await runner.submit(task)

        self.assertEqual(updated_task.slurm_job.id, "42")
        self.assertEqual(updated_task.slurm_job.state, "PENDING")
        self.assertEqual(updated_task.state, TaskState.SUBMITTED)
        self.assertIn("42", runner._squeue_data)
        self.assertEqual(runner._squeue_data["42"], "PENDING")

    async def test_worker_submission_and_state_file_use_worker_directory(self):
        spec = SpecData(name="demo", executor="slurm", state_file="result.state",
                        worker_pool={"min_workers": 1, "entrypoint": "sbatch worker.sh"})
        worker = Worker(TaskData(id="AB1234", created_at=1, updated_at=1),
                        spec, spec_dir="/local")
        connector = DummyConnector("/remote", [
            CmdResult(returncode=0, stdout="Submitted batch job 42\n", stderr=""),
            CmdResult(returncode=0, stdout="", stderr=""),
        ])
        runner = SlurmRunner(SlurmConfig(), connector, query_interval_s=0)
        await runner.submit(worker, env={"QUEUE_TOKEN": "a b"})
        self.assertIn("/remote/specs/demo/workers/AB/1234", connector.commands[0])
        self.assertIn("QUEUE_TOKEN='a b'", connector.commands[0])
        self.assertIn("sbatch worker.sh", connector.commands[0])
        self.assertEqual(await runner.query_state(worker), TaskState.SUCCEEDED)
        self.assertEqual(connector.state_path,
                         "/remote/specs/demo/workers/AB/1234/result.state")

    async def test_runner_accepts_protocol_without_task_or_worker(self):
        class ExecutionTarget:
            slurm_job = None
            state = TaskState.IDLE

            def get_prefix(self):
                return "execution"

            def get_entrypoint(self):
                return "sbatch job.sh"

            def get_state_file(self):
                return "execution/result"

            def set_slurm_job(self, slurm_job):
                self.slurm_job = slurm_job

            def set_state(self, state):
                self.state = state

        target = ExecutionTarget()
        connector = DummyConnector("/remote", [
            CmdResult(returncode=0, stdout="42", stderr=""),
            CmdResult(returncode=0, stdout="", stderr=""),
        ])
        runner = SlurmRunner(SlurmConfig(), connector, query_interval_s=0)
        self.assertIs(await runner.submit(target), target)
        self.assertEqual(target.state, TaskState.SUBMITTED)
        self.assertEqual(await runner.query_state(target), TaskState.SUCCEEDED)
        self.assertEqual(connector.state_path, "/remote/execution/result")

    async def test_update_squeue_logs_jobs_that_disappear_from_cache(self):
        connector = DummyConnector(
            "/remote",
            [
                CmdResult(returncode=0, stdout="42|RUNNING\n43|PENDING\n", stderr=""),
                CmdResult(returncode=0, stdout="43|RUNNING\n", stderr=""),
            ],
        )
        runner = SlurmRunner(SlurmConfig(), connector, query_interval_s=0)

        await runner._update_squeue()

        with self.assertLogs("taskspec.runner", level="INFO") as logs:
            await runner._update_squeue()

        self.assertIn("Slurm jobs disappeared from squeue: 42(RUNNING)", "\n".join(logs.output))

    async def test_update_squeue_logs_added_and_changed_jobs(self):
        connector = DummyConnector(
            "/remote",
            [
                CmdResult(returncode=0, stdout="42|PENDING\n", stderr=""),
                CmdResult(returncode=0, stdout="42|RUNNING\n44|PENDING\n", stderr=""),
            ],
        )
        runner = SlurmRunner(SlurmConfig(), connector, query_interval_s=0)

        await runner._update_squeue()

        with self.assertLogs("taskspec.runner", level="INFO") as logs:
            await runner._update_squeue()

        output = "\n".join(logs.output)
        self.assertIn("Slurm jobs appeared in squeue: 44(PENDING)", output)
        self.assertIn("Slurm jobs changed in squeue: 42(PENDING->RUNNING)", output)


if __name__ == "__main__":
    unittest.main()
