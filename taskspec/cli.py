import os
import yaml

from .config import Config


def start_server(path: str):
    config_file = os.path.join(path, "config.yml")
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found at {config_file}")
    with open(config_file, 'r') as f:
        config = Config(**yaml.safe_load(f))

    from .api import make_fastapi_app
    app = make_fastapi_app(base_url=config.server.base_url)
    import uvicorn
    uvicorn.run(app, host=config.server.host, port=config.server.port)



