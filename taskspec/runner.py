from pydantic import BaseModel
from typing import Any, Optional

from .connector import Connector
from .spec import TaskData


class SlurmConfig(BaseModel):
    sbatch: str = "sbatch"
    squeue: str = "squeue"
    scancel: str = "scancel"
    sacct: str = "sacct"


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
        return task

    async def query(self, task: TaskData):
        pass