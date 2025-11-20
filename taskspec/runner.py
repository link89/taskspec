from pydantic import BaseModel
from typing import Optional


class SlurmConfig(BaseModel):
    sbatch: str = "sbatch"
    squeue: str = "squeue"
    scancel: str = "scancel"
    sacct: str = "sacct"


class RunnerConfig(BaseModel):
    slurm: Optional[SlurmConfig] = None


