from pydantic import BaseModel, field_validator
from typing import Optional, List


class SlurmJobData(BaseModel):
    id: str


class InFile(BaseModel):
    src: str
    dst: str = ''


class TaskSpec(BaseModel):
    executor: str
    entrypoint: str
    in_files: List[InFile] = []
    stage_file: str = 'STAGE'

    @field_validator('in_files', mode='before')
    @classmethod
    def process_in_files(cls, v):
        if not isinstance(v, list):
            raise ValueError("in_files must be a list")
        in_files = []
        for item in v:
            if isinstance(item, str):
                if ':' in item:
                    src, dst = item.split(':', 1)
                    in_files.append(InFile(src=src, dst=dst))
                else:
                    in_files.append(InFile(src=item))
            else:
                in_files.append(InFile.model_validate(item))
        return in_files


class FileData(BaseModel):
    name: str
    content: str


class TaskInput(BaseModel):
    idempotent_key: str = ''
    params: dict = {}
    files: List[FileData] = []


class TaskData(BaseModel):
    id: str
    prefix: str
    spec: TaskSpec
    input: TaskInput
    created_at: int
    slurm_job: Optional[SlurmJobData] = None

