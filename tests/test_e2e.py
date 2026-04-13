import os
import time
import signal
import subprocess
import requests
import unittest

class TestTaskSpecE2E(unittest.TestCase):
    session: requests.Session
    server_process = None
    server_url = "http://127.0.0.1:8011"

    @classmethod
    def setUpClass(cls):
        # Bypass proxy for local requests
        cls.session = requests.Session()
        cls.session.trust_env = False

        # Start the server from the project root
        cls.server_process = subprocess.Popen(
            ["python3", "-m", "taskspec.cli", "start_server", "demo/"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )

        # Wait for server to be ready
        max_retries = 15
        ready = False
        for i in range(max_retries):
            try:
                # Check if server is up by hitting /docs
                resp = cls.session.get(f"{cls.server_url}/docs", timeout=2)
                if resp.status_code == 200:
                    ready = True
                    break
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
                time.sleep(1)

        if not ready:
            # Cleanup on failure
            os.killpg(os.getpgid(cls.server_process.pid), signal.SIGTERM)
            stdout, stderr = cls.server_process.communicate(timeout=1)
            print(f"Server failed to start.\nStdout: {stdout.decode()}\nStderr: {stderr.decode()}")
            raise RuntimeError("Server failed to start")

    @classmethod
    def tearDownClass(cls):
        if cls.server_process:
            try:
                os.killpg(os.getpgid(cls.server_process.pid), signal.SIGTERM)
                cls.server_process.wait(timeout=5)
            except Exception as e:
                print(f"Error stopping server: {e}")
                os.killpg(os.getpgid(cls.server_process.pid), signal.SIGKILL)

    def test_hello_world_submission_and_file_retrieval(self):
        spec_name = "hello-world"

        # 1. Submit a task
        payload = {
            "submit": True,
            "params": {"test": "data"}
        }
        resp = self.session.post(f"{self.server_url}/specs/{spec_name}/tasks", json=payload)
        self.assertEqual(resp.status_code, 200, f"Failed to submit task: {resp.text}")

        data = resp.json()
        task_id = data["id"]
        self.assertIn("slurm_job", data)
        self.assertIsNotNone(data["slurm_job"]["id"])
        job_id = data["slurm_job"]["id"]
        print(f"\n[Test] Submitted job {job_id} with task_id {task_id}")

        # 2. Verify metadata retrieval
        resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["id"], task_id)

        # 3. Wait for and verify output.txt
        max_wait = 45
        start_time = time.time()
        file_found = False

        print("[Test] Polling for output.txt...")
        while time.time() - start_time < max_wait:
            resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}/files/output.txt")
            if resp.status_code == 200:
                content = resp.text.strip()
                self.assertEqual(content, "Hello, World!")
                file_found = True
                break
            elif resp.status_code == 404:
                time.sleep(3)
            else:
                print(f"[Test] Unexpected status {resp.status_code}: {resp.text}")
                time.sleep(3)

        self.assertTrue(file_found, "Timed out waiting for output.txt")
        print("[Test] Successfully verified output.txt content.")

    def test_metadata_only_no_submit(self):
        spec_name = "hello-world"
        payload = {"submit": False}
        resp = self.session.post(f"{self.server_url}/specs/{spec_name}/tasks", json=payload)
        self.assertEqual(resp.status_code, 200)
        task_id = resp.json()["id"]

        resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["id"], task_id)
        self.assertFalse(data["input"]["submit"])

if __name__ == "__main__":
    unittest.main()
