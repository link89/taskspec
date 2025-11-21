from pydantic import BaseModel
from typing import Optional, List
import yaml
import os

from .executor import ExecutorServiceManager

class TaskSpec(BaseModel):
    name: str
    executor: str
    entrypoint: str
    in_files: List[str] = []


class FileData(BaseModel):
    name: str
    content: str


class TaskInput(BaseModel):
    idempotent_key: str = ''
    params: dict = {}
    files: List[FileData] = []


class TaskData(BaseModel):
    id: str
    prefix: str
    spec: TaskSpec
    input: TaskInput
    created_at: int
    slurm_job_id: Optional[int] = None


class TaskService:

    def __init__(self, base_dir: str,
                 executor_mgr: ExecutorServiceManager):
        self._base_dir = base_dir
        self._executor_mgr = executor_mgr

    async def create_task(self, spec_name: str, task_input: TaskInput) -> TaskData:
        spec = self.get_spec(spec_name)
        task_data = TaskData(
            id='generated_task_id',
            prefix='task_prefix',
            spec=spec,
            input=task_input,
            created_at=0
        )
        return task_data



    async def get_task(self, spec_name: str, task_id: str) -> Optional[TaskData]:
        ...


    async def get_task_files(self, spec_name: str, task_id: str) -> List[str]:
        ...

    async def get_task_file(self, spec_name: str, task_id: str, path: str):
        ...


    def get_spec(self, spec_name: str) -> TaskSpec:
        spec_dir = self._get_spec_dir(spec_name)
        spec_file = os.path.join(spec_dir, 'spec.yml')
        if not os.path.exists(spec_file):
            raise FileNotFoundError(f"Spec file not found: {spec_file}")
        with open(spec_file, 'r') as f:
            spec_dict = yaml.safe_load(f)
        return TaskSpec(**spec_dict)


    def _get_spec_dir(self, spec_name: str) -> str:
        return os.path.join(self._base_dir, 'specs', spec_name)

    def _get_task_dir(self, spec_name: str, task_id: str) -> str:
        spec_dir = self._get_spec_dir(spec_name)
        return os.path.join(spec_dir, 'tasks', task_id)

