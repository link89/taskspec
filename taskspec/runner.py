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
        self._query_interval_s = query_interval_s
        self._last_update_ts = 0
        self._squeue_data = {}

    async def _update_squeue(self):
        import time
        now = time.time()
        if now - self._last_update_ts < self._query_interval_s:
            return

        # squeue -o "%i|%T" returns JOBID|STATE
        cmd = f"{self.config.squeue} --noheader -o \"%i|%T\""
        result = await self._connector.shell(cmd)
        if result.returncode == 0:
            stdout = result.stdout.decode() if isinstance(result.stdout, bytes) else result.stdout
            data = parse_csv(f"JOBID|STATE\n{stdout}")
            self._squeue_data = {row['JOBID']: row['STATE'] for row in data if 'JOBID' in row and 'STATE' in row}
            self._last_update_ts = now
        else:
            logger.error(f"Failed to run squeue: {result.stderr}")

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
        task.slurm_job = SlurmJobData(id=job_id, state='PENDING')
        return task

    async def query(self, spec: SpecData, task: TaskData):
        if not task.slurm_job:
            raise ValueError("Task has no associated Slurm job")

        await self._update_squeue()
        job_id = task.slurm_job.id

        if job_id in self._squeue_data:
            task.slurm_job.state = self._squeue_data[job_id]
            task.state = TaskState.SUBMITTED
            return task

        # Job not in squeue, check state_file
        base_dir = self._connector.get_base_dir()
        task_dir = os.path.join(base_dir, task.get_prefix(spec))
        state_path = os.path.join(task_dir, task.state_file)

        # Try to read state_file using connector.load_text
        try:
            raw_state = await self._connector.load_text(state_path)
            raw_state = raw_state.strip()
        except Exception as e:
            logger.warning(f"Job {job_id} not in squeue and state file {state_path} not found or unreadable: {e}")
            task.state = TaskState.FAILED
            return task

        if not raw_state:
            task.state = TaskState.FAILED
            return task

        first_line = raw_state.splitlines()[0].strip().lower()
        if first_line in ('ok', 'success', 'done'):
            task.state = TaskState.SUCCEEDED
        else:
            task.state = TaskState.FAILED
        return task


    def _parse_job_id(self, stdout: str):
        m = re.search(r'\d+', stdout)
        return m.group(0) if m else ''


def parse_csv(text: str, delimiter="|"):
    """
    Parse CSV text to list of dictionaries
    """
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    return list(reader)