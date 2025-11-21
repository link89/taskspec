from fastapi import FastAPI, APIRouter
from .task import TaskInput

class Controller:

    async def get_specs(self, ):
        ...


    async def get_spec(self, spec_name: str):
        ...


    async def get_tasks(self, spec_name: str):
        ...


    async def get_task(self, spec_name: str, task_id: str):
        ...


    async def create_task(self, spec_name: str, task_input: TaskInput):
        ...


    async def get_task_files(self, spec_name: str, task_id: str):
        ...


    async def get_task_file(self, spec_name: str, task_id: str, file_name: str):
        ...



def make_fastapi_app(base_url: str) -> FastAPI:
    controller = Controller()
    router = APIRouter()
    router.add_api_route("/specs",
                         endpoint=controller.get_specs, methods=["GET"])
    router.add_api_route("/specs/{spec_name}",
                         endpoint=controller.get_spec, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks",
                         endpoint=controller.get_tasks, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}",
                         endpoint=controller.get_task, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks",
                         endpoint=controller.create_task, methods=["POST"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}/files",
                         endpoint=controller.get_task_files, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}/file",
                         endpoint=controller.get_task_file, methods=["GET"])

    app = FastAPI(root_path=base_url)
    app.include_router(router)
    return app

