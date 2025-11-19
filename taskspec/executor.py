from pydantic import BaseModel
from typing import Optional
import os


class SshConfig(BaseModel):
    host: str
    port: int = 22
    config_file: str = os.path.expanduser("~/.ssh/config")


class ExecutorConfig(BaseModel):
    name: str
    ssh: Optional[SshConfig] = None


class ExecutorService:
    def __init__(self, config: ExecutorConfig):
        self.config = config
