import os
from pydantic import BaseModel, field_validator, Field, model_validator
from typing import Optional, List, Union
from enum import IntEnum


class SlurmJobData(BaseModel):
    id: str
    state: str


class InFile(BaseModel):
    src: str
    dst: str = ''

    @classmethod
    def from_any(cls, v: Union[str, dict, 'InFile']) -> 'InFile':
        if isinstance(v, str):
            if ':' in v:
                src, dst = v.split(':', 1)
                return cls(src=src, dst=dst)
            else:
                return cls(src=v)
        elif isinstance(v, dict):
            return cls.model_validate(v)
        return v


def process_in_files(v):
    if isinstance(v, str):
        v = [v]
    if not isinstance(v, list):
        raise ValueError("files must be string or list of strings/InFile")
    return [InFile.from_any(item) for item in v]


class OnDemand(BaseModel):
    entrypoint: str


class WorkerPool(BaseModel):
    workers: int
    files: List[InFile] = []
    entrypoint: str

    @field_validator('files', mode='before')
    @classmethod
    def process_files(cls, v):
        return process_in_files(v)


class TaskSpec(BaseModel):
    name: str = ""
    executor: str
    on_demand: Optional[OnDemand] = None
    worker_pool: Optional[WorkerPool] = None
    meta_dir: str = ".meta"
    state_file: str = ".STATE"
    poll_interval_s: int = 5
    files: List[InFile] = []
    """
    files will be used by each task created from this spec.
    """

    @model_validator(mode='after')
    def check_mode(self):
        if (self.on_demand is None) == (self.worker_pool is None):
            raise ValueError("Exactly one of on_demand or worker_pool must be provided")
        return self

    def get_dir(self, base_dir: str = '.') -> str:
        return os.path.normpath(os.path.join(base_dir, 'specs', self.name))

    @field_validator('files', mode='before')
    @classmethod
    def process_files(cls, v):
        return process_in_files(v)


class FileData(BaseModel):
    name: str
    content: str


class TaskState(IntEnum):
    IDLE = 0
    SUBMITTED = 1
    SUCCEEDED = 2
    FAILED = 3
    ERROR = 4  # internal error, e.g. failed to submit

    @classmethod
    def is_terminated(cls, state: "TaskState") -> bool:
        return state in (cls.SUCCEEDED, cls.FAILED, cls.ERROR)


class TaskInput(BaseModel):
    idempotent_key: str = ''
    params: dict = {}
    files: List[FileData] = []
    submit: bool = True


class TaskData(BaseModel):
    id: str
    state: TaskState = TaskState.IDLE
    created_at: int
    updated_at: int
    slurm_job: Optional[SlurmJobData] = None
    is_worker: bool = Field(default=False, exclude=True)

    def get_dir(self, spec_dir: str) -> str:
        base = 'workers' if self.is_worker else 'tasks'
        return os.path.normpath(os.path.join(spec_dir, base, self.id[:2], self.id[2:]))

    def get_prefix(self, spec: 'TaskSpec') -> str:
        base = 'workers' if self.is_worker else 'tasks'
        return f'specs/{spec.name}/{base}/{self.id[:2]}/{self.id[2:]}'
