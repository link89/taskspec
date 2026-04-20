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
    auth_file = "demo/auth.jsonl"
    test_key = "testuser"
    test_secret = "testpass"

    @classmethod
    def setUpClass(cls):
        # 1. Add auth key to demo
        subprocess.run([
            "python3", "-m", "taskspec.cli", "add_auth_key", "demo/",
            "--key", cls.test_key, "--secret", cls.test_secret
        ], check=True)

        # 2. Setup session with auth headers
        cls.session = requests.Session()
        cls.session.trust_env = False
        cls.session.headers.update({
            "Authorization": f"Bearer {cls.test_key}:{cls.test_secret}"
        })

        # 3. Start the server from the project root (no --no_auth)
        cls.server_process = subprocess.Popen(
            ["python3", "-m", "taskspec.cli", "start_server", "demo/"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid
        )

        # Wait for server to be ready using /health endpoint (no auth needed)
        max_retries = 30
        ready = False
        print("[Test] Waiting for server /health...")
        for i in range(max_retries):
            try:
                resp = requests.get(f"{cls.server_url}/health", timeout=2)
                if resp.status_code == 200:
                    ready = True
                    break
            except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
                pass
            time.sleep(1)

        if not ready:
            # Cleanup on failure
            os.killpg(os.getpgid(cls.server_process.pid), signal.SIGTERM)
            stdout, stderr = cls.server_process.communicate(timeout=1)
            print(f"Server failed to start.\nStdout: {stdout.decode()}\nStderr: {stderr.decode()}")
            raise RuntimeError("Server failed to start")
        
        print("[Test] Server is ready.")

    @classmethod
    def tearDownClass(cls):
        if cls.server_process:
            try:
                os.killpg(os.getpgid(cls.server_process.pid), signal.SIGTERM)
                cls.server_process.wait(timeout=5)
            except Exception as e:
                print(f"Error stopping server: {e}")
                os.killpg(os.getpgid(cls.server_process.pid), signal.SIGKILL)
        
        # Cleanup auth file
        if os.path.exists(cls.auth_file):
            os.remove(cls.auth_file)

    def test_auth_failures(self):
        spec_name = "hello-world"
        url = f"{self.server_url}/specs/{spec_name}/tasks/nonexistent"
        
        # 1. No auth headers
        resp = requests.get(url)
        self.assertEqual(resp.status_code, 401)
        
        # 2. Invalid secret
        headers = {"Authorization": f"Bearer {self.test_key}:wrong"}
        resp = requests.get(url, headers=headers)
        self.assertEqual(resp.status_code, 403)
        
        # 3. Invalid format
        headers = {"Authorization": f"Bearer {self.test_key}"}
        resp = requests.get(url, headers=headers)
        self.assertEqual(resp.status_code, 401)

    def test_hello_world_submission_and_file_retrieval(self):
        spec_name = "hello-world"

        # 1. Submit a task with additional files
        payload = {
            "submit": True,
            "params": {"test": "data"},
            "files": [
                {"name": "input_file.txt", "content": "this is input file content"}
            ]
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
        
        # 3. Verify input file existence immediately (should be uploaded before submission)
        resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}/files/input_file.txt")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.text, "this is input file content")
        print("[Test] Verified input_file.txt content.")

        # 4. Wait for and verify output.txt
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

        # 5. Wait for task state to be terminated
        print("[Test] Waiting for task state to become terminated...")
        terminated = False
        while time.time() - start_time < max_wait:
            resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}")
            if resp.status_code == 200:
                state = resp.json()["state"]
                # TaskState.SUCCEEDED = 2, FAILED = 3, ERROR = 4
                if state >= 2:
                    terminated = True
                    print(f"[Test] Task terminated with state {state}")
                    break
            time.sleep(2)
        self.assertTrue(terminated, "Task did not reach terminated state")

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

        resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}/input")
        self.assertEqual(resp.status_code, 200)
        input_data = resp.json()
        self.assertFalse(input_data["submit"])

    def test_worker_pool_submission(self):
        spec_name = "worker-test"

        payload = {
            "submit": True,
            "params": {"test": "workerpool"},
            "files": []
        }
        # We might need to wait a bit for RootService to pick up the new spec if we just created it.
        # But RootService.init() is only called at startup. 
        # So we should probably assume it was already there or restart?
        # The user says "创建一次后就会被保存", so it should be there in subsequent runs.
        
        resp = self.session.post(f"{self.server_url}/specs/{spec_name}/tasks", json=payload)
        self.assertEqual(resp.status_code, 200, f"Failed to submit task: {resp.text}")
        task_id = resp.json()["id"]
        print(f"\n[Test] Submitted task {task_id} to worker pool")

        # 2. Wait for task to be succeeded
        max_wait = 60
        start_time = time.time()
        succeeded = False
        print(f"[Test] Polling for task {task_id} state...")
        while time.time() - start_time < max_wait:
            resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}")
            if resp.status_code == 200:
                state = resp.json()["state"]
                if state == 2: # SUCCEEDED
                    succeeded = True
                    break
                elif state >= 3: # FAILED or ERROR
                    self.fail(f"Task failed with state {state}")
            time.sleep(2)
        
        self.assertTrue(succeeded, "Timed out waiting for task to succeed in worker pool")

        # 3. Verify output file
        resp = self.session.get(f"{self.server_url}/specs/{spec_name}/tasks/{task_id}/files/output.txt")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.text.strip(), "Worker Pool Result")
        print("[Test] Successfully verified worker pool task output.")

if __name__ == "__main__":
    unittest.main()
