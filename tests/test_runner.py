import unittest

from taskspec.connector import CmdResult
from taskspec.runner import SlurmConfig, SlurmRunner
from taskspec.schema import SpecData, TaskData, TaskState


class DummyConnector:
    def __init__(self, base_dir: str, shell_result: CmdResult):
        self._base_dir = base_dir
        self._shell_result = shell_result
        self.commands = []

    def get_base_dir(self) -> str:
        return self._base_dir

    async def shell(self, cmd: str) -> CmdResult:
        self.commands.append(cmd)
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
        self.assertEqual(runner._squeue_data["42"], "")


if __name__ == "__main__":
    unittest.main()