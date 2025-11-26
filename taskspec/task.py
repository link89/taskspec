from pydantic import BaseModel
from typing import Optional, List
import json
import time
import yaml
import os

import uuid

from .executor import ExecutorServiceManager

class TaskSpec(BaseModel):
    name: str
    executor: str
    entrypoint: str
    in_files: List[str] = []
    stage_file: str = 'STAGE'


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
        self._unfinished_tasks = []
        # TODO: load unfinished tasks from disk

    async def polling_loop(self):
        """
        Periodically poll the status of all running tasks and update their state.
        """
        # TODO


    async def create_task(self, spec_name: str, task_input: TaskInput) -> TaskData:
        spec = self.get_spec(spec_name)
        executor = self._executor_mgr.get_executor(spec.executor)

        task_id = str(uuid.uuid4())
        task_prefix = f'specs/{spec_name}/tasks/{task_id}'
        task_dir = os.path.join(self._base_dir, task_prefix)
        meta_dir = os.path.join(task_dir, '.meta')
        os.makedirs(meta_dir, exist_ok=True)

        task_data = TaskData(
            id=task_id,
            prefix=task_prefix,
            spec=spec,
            input=task_input,
            created_at=int(time.time()),
        )
        self._update_task(task_data)
        await executor.runner.submit(task_data)
        return task_data


    async def get_task(self, spec_name: str, task_id: str) -> Optional[TaskData]:
        ...



    async def get_task_files(self, spec_name: str, task_id: str) -> List[str]:
        ...

    async def get_task_file(self, spec_name: str, task_id: str, file_path: str):
        ...


    def get_spec(self, spec_name: str) -> TaskSpec:
        spec_dir = self._get_spec_dir(spec_name)
        spec_file = os.path.join(spec_dir, 'config.yml')
        if not os.path.exists(spec_file):
            raise FileNotFoundError(f"Spec file not found: {spec_file}")
        with open(spec_file, 'r') as f:
            spec_dict = yaml.safe_load(f)
        return TaskSpec(**spec_dict)

    def _get_spec_dir(self, spec_name: str) -> str:
        return os.path.join(self._base_dir, 'specs', spec_name)

    def _update_task(self, task_data: TaskData):
        task_data_file = os.path.join(self._base_dir, task_data.prefix,
                                      '.meta', 'task_data.json')
        with open(task_data_file, 'w', encoding='utf-8') as f:
            json.dump(task_data.model_dump(), f)

