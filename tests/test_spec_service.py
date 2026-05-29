import json
import os
import tempfile
import unittest

from taskspec.schema import SpecData, TaskData, TaskState
from taskspec.service.spec import SpecService


class DummyConnector:
    def __init__(self, base_dir: str):
        self._base_dir = base_dir
        self.requested_path = None
        self.requested_offset = None

    def get_base_dir(self) -> str:
        return self._base_dir

    async def mkdir(self, path: str, exist_ok: bool = True):
        os.makedirs(path, exist_ok=exist_ok)

    async def put(self, src: str, dst: str):
        with open(src, "rb") as src_handle, open(dst, "wb") as dst_handle:
            dst_handle.write(src_handle.read())

    async def dump_text(self, text: str, path: str, encoding: str = "utf-8"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding=encoding) as handle:
            handle.write(text)

    async def get_abs_path(self, path: str) -> str:
        return path

    async def exists(self, path: str) -> bool:
        raise AssertionError("SpecService.get_task_file should not call connector.exists")

    async def get_fstream(self, path: str, buffer_size: int = 4096, offset: int = 0):
        self.requested_path = path
        self.requested_offset = offset

        async def stream():
            yield b"payload"

        return stream()


class DummyExecutor:
    def __init__(self, connector: DummyConnector, runner=None):
        self.connector = connector
        self.runner = runner


class TestSpecService(unittest.IsolatedAsyncioTestCase):
    async def test_get_task_file_delegates_directly_to_get_fstream(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            spec = SpecData(name="demo", executor="local", on_demand={"entrypoint": "run.sh"})
            connector = DummyConnector(temp_dir)
            service = SpecService(
                name="demo",
                dir=temp_dir,
                spec=spec,
                executor=DummyExecutor(connector),
                public_url="http://127.0.0.1:8000",
            )

            task_id = "A1TASK"
            task_dir = os.path.join(temp_dir, "tasks", task_id[:2], task_id[2:], spec.meta_dir)
            os.makedirs(task_dir, exist_ok=True)
            with open(os.path.join(task_dir, "task.json"), "w", encoding="utf-8") as handle:
                json.dump(
                    TaskData(
                        id=task_id,
                        state=TaskState.IDLE,
                        created_at=1,
                        updated_at=1,
                    ).model_dump(),
                    handle,
                )

            stream = await service.get_task_file(task_id, "output.txt", offset=7)
            chunks = [chunk async for chunk in stream]

            self.assertEqual(chunks, [b"payload"])
            self.assertEqual(
                connector.requested_path,
                os.path.join(temp_dir, "specs", "demo", "tasks", task_id[:2], task_id[2:], "output.txt"),
            )
            self.assertEqual(connector.requested_offset, 7)


if __name__ == "__main__":
    unittest.main()