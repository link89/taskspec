from fastapi import FastAPI, APIRouter


async def get_specs():
    ...


async def get_spec(spec_name: str):
    ...


async def get_tasks(spec_name: str):
    ...


async def get_task(spec_name: str, task_id: str):
    ...

async def create_task(spec_name: str):
    ...

async def get_task_files(spec_name: str, task_id: str):
    ...

async def get_task_file(spec_name: str, task_id: str, file_name: str):
    ...


def make_http_app(base_url: str) -> FastAPI:
    router = APIRouter()

    router.add_api_route("/specs", endpoint=get_specs, methods=["GET"])
    router.add_api_route("/specs/{spec_name}", endpoint=get_spec, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks", endpoint=get_tasks, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}", endpoint=get_task, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks", endpoint=create_task, methods=["POST"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}/files", endpoint=get_task_files, methods=["GET"])

    app = FastAPI(root_path=base_url)
    app.include_router(router)
    return app

