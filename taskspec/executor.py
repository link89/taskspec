from pydantic import BaseModel
from typing import Optional

from .connector import SshConfig, Connector


class ExecutorConfig(BaseModel):
    name: str
    ssh: Optional[SshConfig] = None


class ExecutorService:
    connector: Connector

    def __init__(self, config: ExecutorConfig):
        self.config = config

class ExecutorManager:
    pass