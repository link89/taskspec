import unittest

from fastapi.testclient import TestClient

from taskspec.api import make_fastapi_app


class DummyRootService:
    def init(self) -> None:
        return None

    def get_spec_service(self, spec_name: str):
        return DummySpecService()


class DummySpecService:
    async def get_task_file(self, task_id: str, file_path: str, offset: int = 0):
        raise FileNotFoundError(f"File not found: {file_path}")


class TestApi(unittest.TestCase):
    def test_constants_endpoint_returns_task_state_mappings(self):
        app = make_fastapi_app("", root_service=DummyRootService())
        client = TestClient(app)

        response = client.get("/constants")

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

    def test_get_task_file_returns_404_for_missing_file(self):
        app = make_fastapi_app("", root_service=DummyRootService())
        client = TestClient(app)

        response = client.get(
            "/specs/demo/tasks/task-1/files/output.txt",
            headers={"Authorization": "Bearer test:key"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "File not found: output.txt"})


if __name__ == "__main__":
    unittest.main()