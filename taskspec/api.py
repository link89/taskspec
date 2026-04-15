from fastapi import FastAPI, APIRouter, Depends, Header, HTTPException
from fastapi.responses import StreamingResponse
from .schema import  TaskInput
from .service import RootService
from .service.auth import AuthService


class Controller:

    def __init__(self, root_service: RootService):
        self._root_service = root_service

    async def get_task(self, spec_name: str, task_id: str):
        spec_service = self._root_service.get_spec_service(spec_name)
        return spec_service.get_task(task_id)

    async def get_task_input(self, spec_name: str, task_id: str):
        spec_service = self._root_service.get_spec_service(spec_name)
        return spec_service.get_task_input(task_id)

    async def create_task(self, spec_name: str, task_input: TaskInput):
        spec_service = self._root_service.get_spec_service(spec_name)
        task_data = await spec_service.create_task(task_input)
        return task_data

    async def get_task_file(self, spec_name: str, task_id: str, file_path: str):
        spec_service = self._root_service.get_spec_service(spec_name)
        fstream = spec_service.get_task_file(task_id, file_path)
        return StreamingResponse(fstream)

    async def health(self):
        return {"status": "ok"}


def make_fastapi_app(base_url: str, root_service: RootService, auth_service: AuthService = None) -> FastAPI:
    controller = Controller(root_service=root_service)

    async def verify_auth(
        authorization: str = Header(None)
    ):
        if auth_service is None:
            return
        
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Authentication required")
        
        token = authorization[len("Bearer "):]
        if ":" not in token:
            raise HTTPException(status_code=401, detail="Invalid token format")
        
        key, secret = token.split(":", 1)
        if not auth_service.verify(key, secret):
            raise HTTPException(status_code=403, detail="Invalid authentication credentials")

    public_router = APIRouter()
    public_router.add_api_route("/health", endpoint=controller.health, methods=["GET"])

    router = APIRouter(dependencies=[Depends(verify_auth)])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}",
                         endpoint=controller.get_task, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}/input",
                         endpoint=controller.get_task_input, methods=["GET"])
    router.add_api_route("/specs/{spec_name}/tasks",
                         endpoint=controller.create_task, methods=["POST"])
    router.add_api_route("/specs/{spec_name}/tasks/{task_id}/files/{file_path:path}",
                         endpoint=controller.get_task_file, methods=["GET"])

    app = FastAPI(root_path=base_url)
    app.include_router(public_router)
    app.include_router(router)

    @app.on_event("startup")
    async def startup_event():
        root_service.init()

    return app
