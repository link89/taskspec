from pydantic import BaseModel, field_validator
from typing import Optional, List
from enum import IntEnum


class SlurmJobData(BaseModel):
    id: str
    state: str


class InFile(BaseModel):
    src: str
    dst: str = ''


class TaskSpec(BaseModel):
    name: str = ""
    executor: str
    entrypoint: str
    files: List[InFile] = []
    """
    files will be used by each task created from this spec.
    """

    @field_validator('files', mode='before')
    @classmethod
    def process_in_files(cls, v):
        if isinstance(v, str):
            v = [v]
        if not isinstance(v, list):
            raise ValueError("files must be string or list of strings/InFile")
        files = []
        for item in v:
            if isinstance(item, str):
                if ':' in item:
                    src, dst = item.split(':', 1)
                    files.append(InFile(src=src, dst=dst))
                else:
                    files.append(InFile(src=item))
            else:
                files.append(InFile.model_validate(item))
        return files


class FileData(BaseModel):
    name: str
    content: str


class TaskState(IntEnum):
    IDLE = 0
    SUBMITTED = 1
    SUCCEEDED = 2
    FAILED = 3
    ERROR = 4  # internal error, e.g. failed to submit


class TaskInput(BaseModel):
    idempotent_key: str = ''
    params: dict = {}
    files: List[FileData] = []
    submit: bool = True


class TaskData(BaseModel):
    id: str
    input: TaskInput
    state: TaskState = TaskState.IDLE
    created_at: int
    slurm_job: Optional[SlurmJobData] = None
    state_file: str = '.STATE'

    def get_prefix(self, spec: 'TaskSpec') -> str:
        return f'specs/{spec.name}/tasks/{self.id}'
