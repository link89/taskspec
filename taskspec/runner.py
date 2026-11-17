from pydantic import BaseModel
from typing import Optional, List
from logging import getLogger
from shlex import quote
import csv
import re
import os

from .connector import Connector
from .schema import SlurmJobData, TaskState
from .run import Run


logger = getLogger(__name__)


class SlurmConfig(BaseModel):
    squeue: str = "squeue"
    scancel: str = "scancel"


class RunnerConfig(BaseModel):
    slurm: Optional[SlurmConfig] = None


class Runner:
    async def submit(self, run: Run, env: Optional[dict[str, str]] = None) -> Run:
        raise NotImplementedError

    async def query_state(self, run: Run) -> TaskState:
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
            new_squeue_data = {row['JOBID']: row['STATE'] for row in data if 'JOBID' in row and 'STATE' in row}
            added_job_ids = sorted(set(new_squeue_data) - set(self._squeue_data))
            disappeared_job_ids = sorted(set(self._squeue_data) - set(new_squeue_data))
            changed_job_ids = sorted(
                job_id for job_id in set(self._squeue_data) & set(new_squeue_data)
                if self._squeue_data[job_id] != new_squeue_data[job_id]
            )
            if added_job_ids:
                logger.info(
                    "Slurm jobs appeared in squeue: %s",
                    ", ".join(
                        f"{job_id}({new_squeue_data[job_id]})"
                        for job_id in added_job_ids
                    ),
                )
            if disappeared_job_ids:
                logger.info(
                    "Slurm jobs disappeared from squeue: %s",
                    ", ".join(
                        f"{job_id}({self._squeue_data[job_id]})"
                        for job_id in disappeared_job_ids
                    ),
                )
            if changed_job_ids:
                logger.info(
                    "Slurm jobs changed in squeue: %s",
                    ", ".join(
                        f"{job_id}({self._squeue_data[job_id]}->{new_squeue_data[job_id]})"
                        for job_id in changed_job_ids
                    ),
                )
            self._squeue_data = new_squeue_data
            self._last_update_ts = now
        else:
            logger.error(f"Failed to run squeue: {result.stderr}")

    async def submit(self, run: Run, env: Optional[dict[str, str]] = None) -> Run:
        base_dir = self._connector.get_base_dir()
        run_dir = os.path.join(base_dir, run.get_prefix())
        entrypoint = run.get_entrypoint()

        env_str = ""
        if env:
            env_str = " ".join([f"{k}={quote(v)}" for k, v in env.items()])

        cmd = f"cd {run_dir} && {env_str} {entrypoint}"
        result = await self._connector.shell(cmd)
        if result.returncode != 0:
            raise ValueError(f"Failed to submit job: {result.stderr}")

        job_id = self._parse_job_id(result.stdout)
        if not job_id:
            raise ValueError(f"Failed to parse job id: {result.stdout}, err: {result.stderr}")

        logger.info(f'Job submitted: {job_id}')
        self._squeue_data[job_id] = 'PENDING'
        run.set_slurm_job(SlurmJobData(id=job_id, state='PENDING'))
        run.set_state(TaskState.SUBMITTED)
        return run

    async def query_state(self, run: Run) -> TaskState:
        if not run.slurm_job:
            raise ValueError("Run has no associated Slurm job")

        await self._update_squeue()
        job_id = run.slurm_job.id

        if job_id in self._squeue_data:
            run.slurm_job.state = self._squeue_data[job_id]
            return TaskState.SUBMITTED

        # Job not in squeue, check state_file
        base_dir = self._connector.get_base_dir()
        state_path = os.path.join(base_dir, run.get_state_file())

        # Try to read state_file using connector.load_text
        try:
            raw_state = await self._connector.load_text(state_path)
            raw_state = raw_state.strip()
        except Exception as e:
            logger.warning(f"Job {job_id} not in squeue and state file {state_path} not found or unreadable: {e}")
            return TaskState.FAILED

        if not raw_state:
            return TaskState.FAILED

        first_line = raw_state.splitlines()[0].strip().lower()
        if first_line in ('ok', 'success', 'done', 'pass'):
            return TaskState.SUCCEEDED
        else:
            return TaskState.FAILED


    def _parse_job_id(self, stdout: str):
        m = re.search(r'\d+', stdout)
        return m.group(0) if m else ''


def parse_csv(text: str, delimiter="|"):
    """
    Parse CSV text to list of dictionaries
    """
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    return list(reader)
