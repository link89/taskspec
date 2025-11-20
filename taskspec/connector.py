from pydantic import BaseModel
from typing import Optional

import asyncssh
from asyncssh import SSHClientConnection

import asyncio.subprocess as sp
import os


class Connector:
    async def shell(self, cmd: str, cwd: Optional[str] = None):
        raise NotImplementedError

    async def open(self, path: str):
        raise NotImplementedError


class LocalConnector(Connector):
    async def shell(self, cmd: str, cwd: Optional[str] = None):
        process = await sp.create_subprocess_shell(
            cmd,
            cwd=cwd,
            stdout=sp.PIPE,
            stderr=sp.PIPE
        )
        stdout, stderr = await process.communicate()
        return process.returncode, stdout, stderr

    async def open(self, path: str):
        return open(path, 'r')


class SshConfig(BaseModel):
    host: str
    port: int = 22
    base_dir: str
    config_file: str = os.path.expanduser("~/.ssh/config")


class SshConnector(Connector):
    def __init__(self, config: SshConfig):
        self.config = config
        self._conn: Optional[SSHClientConnection] = None

    async def _get_conn(self):
        if self._conn is not None:
            try:
                await self._conn.run("echo hi")
            except Exception:
                self._close()
        if self._conn is None:
            self._conn = await asyncssh.connect(
                self.config.host,
                port=self.config.port,
                config=self.config.config_file,
            )
        return self._conn


    async def shell(self, cmd: str, cwd: Optional[str] = None):
        ...

    async def open(self, path: str):
        ...

    def _close(self):
        try:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        except Exception:
            pass


class ConnectorConfig(BaseModel):
    ssh: Optional[SshConfig] = None