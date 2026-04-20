import os
import time
import requests
import json

def worker():
    url = os.environ.get("__TASK_QUEUE_URL")
    token = os.environ.get("__TASK_QUEUE_TOKEN")
    print(f"Worker started with URL: {url}")
    while True:
        try:
            resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params={"wait": 5})
            if resp.status_code == 200:
                data = resp.json()
                task_id = data["id"]
                task_dir = data["task_dir"]
                print(f"Pulled task {task_id}")
                output_path = os.path.join(task_dir, "output.txt")
                with open(output_path, "w") as f:
                    f.write("Worker Pool Result")
                # Write .STATE file for on-demand compatibility if queried
                with open(os.path.join(task_dir, ".STATE"), "w") as f:
                    f.write("OK")
                requests.delete(f"{url}tasks/{task_id}", headers={"Authorization": f"Bearer {token}"}, params={"state": "ok"})
                print(f"Completed task {task_id}")
            elif resp.status_code == 204:
                continue
            else:
                print(f"Error pulling: {resp.status_code}")
                time.sleep(1)
        except Exception as e:
            print(f"Worker Exception: {e}")
            time.sleep(1)

if __name__ == "__main__":
    worker()
