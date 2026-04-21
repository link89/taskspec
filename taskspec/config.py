from pydantic import BaseModel
from typing import List

from .executor import ExecutorConfig


class Config(BaseModel):
    base_dir: str
    executors: List[ExecutorConfig]
    server_url: str = 'http://127.0.0.1:8011'
    public_url: str = ''
