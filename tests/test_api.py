import unittest
from types import SimpleNamespace

import httpx

from taskspec.api import make_fastapi_app
from taskspec.schema import TaskData


class DummyRootService:
    def init(self) -> None:
        return None

    def get_spec_service(self, spec_name: str):
        return DummySpecService()


class DummySpecService:
    async def get_task_async(self, task_id: str, wait: int = 0):
        return SimpleNamespace(task_data=TaskData(id=task_id, created_at=1, updated_at=2))

    async def get_task_full_dir(self, task_id: str):
        return f"/remote/tasks/{task_id}"

    async def get_task_file(self, task_id: str, file_path: str, offset: int = 0):
        raise FileNotFoundError(f"File not found: {file_path}")


class TestApi(unittest.IsolatedAsyncioTestCase):
    async def test_get_task_returns_documented_full_task_dir(self):
        app = make_fastapi_app("", root_service=DummyRootService())
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/specs/demo/tasks/task-1")
            openapi = (await client.get("/openapi.json")).json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {
            "id": "task-1", "state": 0, "created_at": 1, "updated_at": 2,
            "slurm_job": None, "full_task_dir": "/remote/tasks/task-1",
        })
        schema_ref = openapi["paths"]["/specs/{spec_name}/tasks/{task_id}"]["get"][
            "responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        schema = openapi["components"]["schemas"][schema_ref.rsplit("/", 1)[-1]]
        self.assertNotIn("full_task_dir", schema["required"])
        self.assertEqual(schema["properties"]["full_task_dir"]["default"], "")
        self.assertEqual(schema["properties"]["full_task_dir"]["type"], "string")
        self.assertIn("description", schema["properties"]["full_task_dir"])
        self.assertEqual(TaskData(id="old", created_at=1, updated_at=2).full_task_dir, "")

    async def test_constants_endpoint_returns_task_state_mappings(self):
        app = make_fastapi_app("", root_service=DummyRootService())
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/constants")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "task_state_to_name": {
                    "0": "IDLE",
                    "1": "SUBMITTED",
                    "2": "SUCCEEDED",
                    "3": "FAILED",
                    "4": "ERROR",
                },
                "task_name_to_state": {
                    "IDLE": 0,
                    "SUBMITTED": 1,
                    "SUCCEEDED": 2,
                    "FAILED": 3,
                    "ERROR": 4,
                },
            },
        )

    async def test_get_task_file_returns_404_for_missing_file(self):
        app = make_fastapi_app("", root_service=DummyRootService())
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get(
                "/specs/demo/tasks/task-1/files/output.txt",
                headers={"Authorization": "Bearer test:key"},
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "File not found: output.txt"})


if __name__ == "__main__":
    unittest.main()
