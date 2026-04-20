from pydantic import BaseModel
from typing import List

from .executor import ExecutorConfig


class Config(BaseModel):
    base_dir: str
    executors: List[ExecutorConfig]
    base_url: str = 'http://127.0.0.1:8011'
