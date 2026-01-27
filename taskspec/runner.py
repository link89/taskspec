from pydantic import BaseModel
from typing import Optional

from .connector import Connector
from .spec import TaskData, SlurmJobData

from pydantic import BaseModel

from typing import Optional
from logging import getLogger
from shlex import quote
import csv
import re
import os

from .connector import Connector

logger = getLogger(__name__)


class SlurmConfig(BaseModel):
    sbatch: str = "sbatch"
    squeue: str = "squeue"
    scancel: str = "scancel"
    scontrol: str = "scontrol"


class RunnerConfig(BaseModel):
    slurm: Optional[SlurmConfig] = None


class Runner:
    async def submit(self, task: TaskData) -> TaskData:
        raise NotImplementedError

    async def query(self, task: TaskData):
        raise NotImplementedError


class SlurmRunner(Runner):

    def __init__(self,
                 config: SlurmConfig,
                 connector: Connector):
        self.config = config
        self._connector = connector
        self._squeue_ids = set()

    async def submit(self, task: TaskData):
        base_dir = self._connector.get_base_dir()
        task_dir = os.path.join(base_dir, task.prefix)
        entrypoint = task.spec.entrypoint

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
            raise ValueError(f"Failed to parse job id from: {result.stdout}, err: {result.stderr}")

        logger.info(f'Job submitted: {job_id}')
        job_data = SlurmJobData(id=job_id)
        task.slurm_job = job_data
        return task

    async def query(self, task: TaskData):
        if not task.slurm_job:
            return None
        job_id = task.slurm_job.id

        # query jobs
        cmd = f'{self.config.squeue} -o "%i|%t|%r|%N" -j {job_id}'
        result = await self._connector.shell(cmd)
        if result.returncode != 0:
            if 'Invalid job id specified' in result.stderr:
                return None
            logger.error(f"Unexpected squeue error: {result.stderr}")
            return {'id': job_id, 'nodes': []}

        # query nodes
        nodes = []
        jobs = parse_csv(result.stdout, delimiter="|")
        if not jobs:
            logger.error(f"No job found for id: {job_id}")
            return None

        job = jobs[0]
        state = job.get('ST', '').strip()
        if state == 'R':
            nodelist = job.get('NODELIST', '').strip()
            result = await self._connector.shell(f'{self.config.scontrol} show hostname {nodelist}')
            if result.returncode == 0:
                nodes = result.stdout.strip().splitlines()
                logger.info(f"Job {job_id} is running on nodes: {nodes}")
            else:
                logger.error(f"Failed to parse nodelist: {result.stderr}")

        return { 'id': job_id, 'state': state, 'nodes': nodes }

    def _parse_job_id(self, stdout: str):
        m = re.search(r'\d+', stdout)
        return m.group(0) if m else ''


def parse_csv(text: str, delimiter="|"):
    """
    Parse CSV text to list of dictionaries
    """
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    return list(reader)