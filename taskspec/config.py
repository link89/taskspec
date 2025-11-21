from pydantic import BaseModel
from typing import List

from .executor import ExecutorConfig


class ServerConfig(BaseModel):
    host: str = '127.0.0.1'
    port: int = 8011
    base_url: str = ''


class Config(BaseModel):
    executors: List[ExecutorConfig]
    server: ServerConfig = ServerConfig()

