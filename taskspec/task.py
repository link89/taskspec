from pydantic import BaseModel
from typing import Literal, List


class TaskSpec(BaseModel):
    name: str
    id_type: Literal['uuid4']
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
    prefix: str
    id: str
    spec: TaskSpec
    input: TaskInput
    created_at: int

