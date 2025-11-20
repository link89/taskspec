from pydantic import BaseModel
from typing import Optional

from .connector import ConnectorConfig, Connector
from .runner import RunnerConfig


class ExecutorConfig(BaseModel):
    name: str
    connector: Optional[ConnectorConfig] = None
    runner: RunnerConfig

class ExecutorService:
    connector: Connector

    def __init__(self, config: ExecutorConfig):
        self.config = config
        if config.connector is None:
            from .connector import LocalConnector
            self.connector = LocalConnector()
        elif config.connector.ssh is not None:
            from .connector import SshConnector
            self.connector = SshConnector(config.connector.ssh)
        else:
            raise NotImplementedError

class ExecutorManager:
    pass