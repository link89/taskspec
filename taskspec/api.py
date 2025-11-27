from fastapi import FastAPI, APIRouter
from .spec import  TaskInput
from .task import TaskService


class Controller:

    def __init__(self, task_service: TaskService):
        self._task_service = task_service

    async def get_task(self, spec_name: str, task_id: str):
        ...

    async def create_task(self, spec_name: str, task_input: TaskInput):
        task_data = await self._task_service.create_task(spec_name, task_input)
        return task_data

    async def get_task_files(self, spec_name: str, task_id: str, file_path: str):
        ...

def make_fastapi_app(base_url: str, task_service: TaskService) -> FastAPI:
    controller = Controller(task_service=task_service)
    router = APIRouter()
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}",
                         endpoint=controller.get_task, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks",
                         endpoint=controller.create_task, methods=["POST"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}/files/{file_path:path}",
                         endpoint=controller.get_task_files, methods=["GET"])

    app = FastAPI(root_path=base_url)
    app.include_router(router)
    return app

