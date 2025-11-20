from pydantic import BaseModel
from typing import Literal


class TaskSpec(BaseModel):
    name: str
    id_type: Literal['uuid4', 'sha256']
    entrypoint: str = 'main.sh'

