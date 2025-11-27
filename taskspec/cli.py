import os
import yaml
import uvicorn

from .config import Config
from .api import make_fastapi_app
from .task import TaskService
from .executor import ExecutorServiceManager


def start_server(path: str):
    config_file = os.path.join(path, "config.yml")
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found at {config_file}")
    with open(config_file, 'r') as f:
        config = Config(**yaml.safe_load(f), base_dir=path)
    executor_manager = ExecutorServiceManager(config.executors, config.base_dir)
    task_service = TaskService(config.base_dir, executor_manager)
    app = make_fastapi_app(base_url=config.server.base_url, task_service=task_service)
    uvicorn.run(app, host=config.server.host, port=config.server.port)


if __name__ == "__main__":
    from fire import Fire
    Fire({"start_server": start_server})

