from pydantic import BaseModel
from typing import Optional, List


class TaskSpec(BaseModel):
    name: str
    executor: str
    entrypoint: str
    in_files: List[str] = []


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
    slurm_job_id: Optional[int] = None

