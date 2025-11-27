from pydantic import BaseModel
from typing import Optional, AsyncGenerator, Any

import asyncssh
from asyncssh import SSHClientConnection
from collections import namedtuple

import asyncio.subprocess as sp
import asyncio
import shutil
import shlex
import os

CmdResult = namedtuple("CmdResult", ["returncode", "stdout", "stderr"])

class Connector:
    def get_base_dir(self) -> str:
        raise NotImplementedError

    async def dump_text(self, text: str, path: str, encoding='utf-8') -> None:
        raise NotImplementedError

    async def put(self, src: str, dst: str) -> None:
        raise NotImplementedError

    async def shell(self, cmd: str) -> CmdResult:
        raise NotImplementedError

    async def mkdir(self, path: str, exist_ok: bool=True) -> None:
        raise NotImplementedError

    def get_fstream(self, path: str, buffer_size: int=4096) -> AsyncGenerator[bytes, Any]:
        raise NotImplementedError


class LocalConnector(Connector):
    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir

    async def mkdir(self, path: str, exist_ok: bool=True):
        os.makedirs(path, exist_ok=exist_ok)

    async def shell(self, cmd: str):
        process = await sp.create_subprocess_shell(
            cmd,
            stdout=sp.PIPE,
            stderr=sp.PIPE,
        )
        stdout, stderr = await process.communicate()
        return CmdResult(
            returncode=process.returncode,
            stdout=stdout,
            stderr=stderr,
        )

    async def dump_text(self, text: str, path: str, encoding='utf-8'):
        with open(path, 'w', encoding=encoding) as f:
            f.write(text)

    async def put(self, src: str, dst: str):
        shutil.copyfile(src, dst)

    async def get_fstream(self, path: str, buffer_size: int = 4096):
        with open(path, 'rb') as f:
            while True:
                buffer = f.read(buffer_size)
                if not buffer:
                    break
                yield buffer
                await asyncio.sleep(0)  # Yield control to the event loop

    def get_base_dir(self) -> str:
        return self._base_dir

class SshConfig(BaseModel):
    host: str
    port: int = 22
    base_dir: str
    config_file: str = os.path.expanduser("~/.ssh/config")

class SshConnector(Connector):
    def __init__(self, config: SshConfig):
        self.config = config
        self._conn: Optional[SSHClientConnection] = None

    def get_base_dir(self):
        return self.config.base_dir

    async def mkdir(self, path: str, exist_ok: bool=True) -> None:
        cmd = f'mkdir {"-p" if exist_ok else ""} {shlex.quote(path)}'
        await self.shell(cmd)

    async def dump_text(self, text: str, path: str, encoding='utf-8') -> None:
        conn = await self._get_conn()
        async with conn.start_sftp_client() as sftp:
            async with sftp.open(path, 'w', encoding=encoding) as f:
                await f.write(text)

    async def put(self, src: str, dst: str) -> None:
        conn = await self._get_conn()
        async with conn.start_sftp_client() as sftp:
            await sftp.put(src, dst)

    async def shell(self, cmd: str):
        conn = await self._get_conn()
        result = await conn.run(cmd)
        return CmdResult(
            returncode=result.exit_status,
            stdout=result.stdout,
            stderr=result.stderr,
        )

    async def get_fstream(self, path: str, buffer_size: int = 4096):
        conn = await self._get_conn()
        async with conn.start_sftp_client() as sftp:
            async with sftp.open(path, 'rb') as f:
                while True:
                    buffer = await f.read(buffer_size)
                    if not buffer:
                        break
                    yield buffer

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

    def _close(self):
        try:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
        except Exception:
            pass


class ConnectorConfig(BaseModel):
    ssh: Optional[SshConfig] = None