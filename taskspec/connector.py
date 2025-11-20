from pydantic import BaseModel
from typing import Optional

import asyncio.subprocess as sp
import os

class Connector:
    async def run(self, command: str, cwd: Optional[str] = None):
        raise NotImplementedError

    def open(self, path: str):
        raise NotImplementedError


class LocalConnector(Connector):
    async def run(self, command: str, cwd: Optional[str] = None):
        process = await sp.create_subprocess_shell(
            command,
            cwd=cwd,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )
        stdout, stderr = await process.communicate()
        return process.returncode, stdout, stderr

    def open(self, path: str):
        return open(path, 'r')


class SshConfig(BaseModel):
    host: str
    port: int = 22
    base_dir: str
    config_file: str = os.path.expanduser("~/.ssh/config")


class SshConnector(Connector):
    def __init__(self, config: SshConfig):
        self.config = config

    async def run(self, command: str, cwd: Optional[str] = None):
        ...

    def open(self, path: str):
        ...