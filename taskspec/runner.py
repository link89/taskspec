from pydantic import BaseModel
from typing import Optional

from .connector import Connector
from .schema import TaskData, SlurmJobData, SpecData

from pydantic import BaseModel

from typing import Optional
from logging import getLogger
from shlex import quote
import csv
import re
import os

from .connector import Connector
from .schema import TaskState


logger = getLogger(__name__)


class SlurmConfig(BaseModel):
    sbatch: str = "sbatch"
    squeue: str = "squeue"
    scancel: str = "scancel"


class RunnerConfig(BaseModel):
    slurm: Optional[SlurmConfig] = None


class Runner:
    async def submit(self, spec: SpecData, task: TaskData) -> TaskData:
        raise NotImplementedError

    async def query(self, spec: SpecData, task: TaskData):
        raise NotImplementedError


class SlurmRunner(Runner):

    def __init__(self,
                 config: SlurmConfig,
                 connector: Connector,
                 query_interval_s = 5):

        self.config = config
        self._connector = connector
        self._squeue_ids = set()
        self._query_interval_s = query_interval_s
        self._last_update_ts = 0

    async def submit(self, spec: SpecData, task: TaskData):
        base_dir = self._connector.get_base_dir()
        task_dir = os.path.join(base_dir, task.get_prefix(spec))
        entrypoint = spec.entrypoint

        # Verify entrypoint exists
        result = await self._connector.shell(f'cd {task_dir} && test -f {quote(entrypoint)}')
        if result.returncode != 0:
            raise FileNotFoundError(f"Not a file: {entrypoint}")

        # Submit job
        cmd = f"cd {task_dir} && {self.config.sbatch} {quote(entrypoint)}"
        result = await self._connector.shell(cmd)
        if result.returncode != 0:
            raise ValueError(f"Failed to submit job: {result.stderr}")

        job_id = self._parse_job_id(result.stdout)
        if not job_id:
            raise ValueError(f"Failed to parse job id: {result.stdout}, err: {result.stderr}")

        logger.info(f'Job submitted: {job_id}')
        job_data = SlurmJobData(id=job_id, state='PENDING')
        return task.model_copy(update={'slurm_job': job_data})

    async def query(self, task: TaskData):
        if not task.slurm_job:
            raise ValueError("Task has no associated Slurm job")
        job_id = task.slurm_job.id
        # TODO: query squeue for the state of the job
        # TODO: if job is not in squeue, check the state_file in task dir to determine if it is SUCCEEDED or FAILED, or assume it is SUCCEEDED if state_file is not found


    def _parse_job_id(self, stdout: str):
        m = re.search(r'\d+', stdout)
        return m.group(0) if m else ''


def parse_csv(text: str, delimiter="|"):
    """
    Parse CSV text to list of dictionaries
    """
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    return list(reader)