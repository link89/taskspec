from pydantic import BaseModel


class TaskSpec(BaseModel):
    name: str
    description: str