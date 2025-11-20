from pydantic import BaseModel
from typing import Optional

from .connector import Connector
from .task import TaskSpec


class SlurmConfig(BaseModel):
    sbatch: str = "sbatch"
    squeue: str = "squeue"
    scancel: str = "scancel"
    sacct: str = "sacct"


class RunnerConfig(BaseModel):
    slurm: Optional[SlurmConfig] = None


class Runner:
    def submit(self, task: TaskSpec):
        raise NotImplementedError


class SlurmRunner(Runner):
    def __init__(self, config: SlurmConfig):
        self.config = config

    def submit(self, task: TaskSpec):
        ...

