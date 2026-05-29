import unittest

from fastapi.testclient import TestClient

from taskspec.api import make_fastapi_app


class DummyRootService:
    def init(self) -> None:
        return None


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


if __name__ == "__main__":
    unittest.main()