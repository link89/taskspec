from pydantic import BaseModel
from typing import Optional

from .connector import ConnectorConfig, Connector
from .runner import RunnerConfig, Runner


class ExecutorConfig(BaseModel):
    name: str
    connector: Optional[ConnectorConfig] = None
    runner: RunnerConfig


class ExecutorService:
    _connector: Connector
    _runner: Runner

    def __init__(self, config: ExecutorConfig):
        self.config = config
        if config.connector is None:
            from .connector import LocalConnector
            self._connector = LocalConnector()
        elif config.connector.ssh is not None:
            from .connector import SshConnector
            self._connector = SshConnector(config.connector.ssh)
        else:
            raise ValueError("No valid connector configuration provided")

        if config.runner.slurm is not None:
            from .runner import SlurmRunner
            self._runner = SlurmRunner(config.runner.slurm, self._connector)
        else:
            raise ValueError("No valid runner configuration provided")

    async def submit_task(self, task):
        ...

    async def query_task(self, task):
        ...
