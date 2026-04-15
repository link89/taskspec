import os
import yaml
import uvicorn
import logging

from .config import Config
from .api import make_fastapi_app
from .service import RootService
from .service.auth import AuthService
from .executor import ExecutorServiceManager
from .util import gen_task_id

logging.basicConfig(level=logging.INFO)

def add_auth_key(path: str, key: str, secret: str = ""):
    auth_file = os.path.join(path, "auth.jsonl")
    if not secret:
        secret = gen_task_id()
        print(f"Generated secret for {key}: {secret}")
    AuthService.add_key(auth_file, key, secret)
    print(f"Added auth key: {key} to {auth_file}")

def start_server(path: str, no_auth: bool = False):
    config_file = os.path.join(path, "config.yml")
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found at {config_file}")
    with open(config_file, 'r') as f:
        config = Config(**yaml.safe_load(f), base_dir=path)
    
    auth_service = None
    if not no_auth:
        auth_file = os.path.join(path, "auth.jsonl")
        if not os.path.exists(auth_file):
            print(f"Error: auth file {auth_file} not found. Start server with --no_auth to disable authentication.")
            return
        auth_service = AuthService(auth_file)
        auth_service.load()
    
    executor_manager = ExecutorServiceManager(config.executors, config.base_dir)
    root_service = RootService(config.base_dir, executor_manager)
    app = make_fastapi_app(base_url=config.server.base_url, root_service=root_service, auth_service=auth_service)
    uvicorn.run(app, host=config.server.host, port=config.server.port)


if __name__ == "__main__":
    from fire import Fire
    Fire({"start_server": start_server, "add_auth_key": add_auth_key})
