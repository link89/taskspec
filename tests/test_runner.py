import unittest

from taskspec.connector import CmdResult
from taskspec.runner import SlurmConfig, SlurmRunner
from taskspec.schema import SpecData, TaskData, TaskState


class DummyConnector:
    def __init__(self, base_dir: str, shell_result: CmdResult | list[CmdResult]):
        self._base_dir = base_dir
        self._shell_result = shell_result
        self.commands = []

    def get_base_dir(self) -> str:
        return self._base_dir

    async def shell(self, cmd: str) -> CmdResult:
        self.commands.append(cmd)
        if isinstance(self._shell_result, list):
            return self._shell_result.pop(0)
        return self._shell_result


class TestSlurmRunner(unittest.IsolatedAsyncioTestCase):
    async def test_submit_registers_job_in_squeue_cache(self):
        connector = DummyConnector(
            "/remote",
            CmdResult(returncode=0, stdout="Submitted batch job 42\n", stderr=""),
        )
        runner = SlurmRunner(SlurmConfig(), connector)
        spec = SpecData(name="demo", executor="slurm", on_demand={"entrypoint": "sbatch run.sh"})
        task = TaskData(id="AB1234", state=TaskState.IDLE, created_at=1, updated_at=1)

        updated_task = await runner.submit(spec, task)

        self.assertEqual(updated_task.slurm_job.id, "42")
        self.assertEqual(updated_task.slurm_job.state, "PENDING")
        self.assertIn("42", runner._squeue_data)
        self.assertEqual(runner._squeue_data["42"], "PENDING")

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