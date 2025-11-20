from pydantic import BaseModel
from typing import Literal


class RuntimeSpec(BaseModel):
    executor: str
    script: str


class TaskSpec(BaseModel):
    name: str
    id_type: Literal['uuid4', 'sha256']
    description: str
    runtime: RuntimeSpec


