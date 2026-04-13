from logging import getLogger
import json
import time
import os
import uuid
import glob
from typing import Dict, Any, AsyncGenerator

from ..executor import ExecutorService
from ..schema import TaskSpec, TaskInput, TaskData, TaskState
from ..util import gen_task_id

logger = getLogger(__name__)

class SpecService:
    def __init__(self, name: str, dir: str, spec: TaskSpec, executor: ExecutorService):
        self.name = name
        self.dir = dir
        self._spec = spec
        self._executor = executor
        self._unfinished_tasks: Dict[str, TaskData] = {}

    def init(self) -> None:
        self._load_unfinished_tasks()

    async def create_task(self, task_input: TaskInput) -> TaskData:
        task_id = gen_task_id(task_input.idempotent_key)
        task_dir = self._get_task_dir(task_id)
        
        # prepare local task directories
        local_meta_dir = os.path.join(task_dir, '.meta')
        os.makedirs(local_meta_dir, exist_ok=True)

        # prepare remote executor directories
        remote_base_dir = self._executor.connector.get_base_dir()
        task_prefix = f'specs/{self.name}/tasks/{task_id}'
        remote_task_dir = os.path.join(remote_base_dir, task_prefix)
        await self._executor.connector.mkdir(remote_task_dir, exist_ok=True)

        # copy files to executor
        for file in self._spec.files:
            src_path = file.src
            dst_path = file.dst or src_path
            real_src_path = os.path.join(self.dir, src_path)
            real_dst_path = os.path.join(remote_task_dir, dst_path)
            await self._executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await self._executor.connector.put(real_src_path, real_dst_path)

        # generate files from TaskInput
        for file_data in task_input.files:
            real_dst_path = os.path.join(remote_task_dir, file_data.name)
            await self._executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await self._executor.connector.dump_text(file_data.content, real_dst_path)

        task_data = TaskData(
            id=task_id,
            state=TaskState.IDLE,
            input=task_input,
            created_at=int(time.time()),
        )

        self._save_task(task_data)
        if task_input.submit:
            try:
                task_data = await self._executor.runner.submit(self._spec, task_data)
                if task_data.state not in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.ERROR):
                    self._unfinished_tasks[task_id] = task_data
            except Exception:
                task_data.state = TaskState.ERROR
                raise
            finally:
                self._save_task(task_data)
        return task_data

    def get_task(self, task_id: str) -> TaskData:
        if task_id in self._unfinished_tasks:
            return self._unfinished_tasks[task_id]
        
        task_dir = self._get_task_dir(task_id)
        task_data_file = os.path.join(task_dir, '.meta', 'task_data.json')
        if not os.path.exists(task_data_file):
            raise FileNotFoundError(f"Task data file not found: {task_data_file}")
        with open(task_data_file, 'r', encoding='utf-8') as f:
            task_data = json.load(f)
        return TaskData(**task_data)

    def get_task_file(self, task_id: str, file_path: str) -> AsyncGenerator[bytes, Any]:
        task_data = self.get_task(task_id)
        file_path = os.path.normpath(file_path)
        remote_base_dir = self._executor.connector.get_base_dir()
        remote_task_dir = os.path.join(remote_base_dir, task_data.get_prefix(self._spec))
        remote_file_path = os.path.normpath(os.path.join(remote_task_dir, file_path))
        
        if not remote_file_path.startswith(os.path.normpath(remote_task_dir)):
            raise ValueError(f"Illegal file path: {file_path}")
        return self._executor.connector.get_fstream(remote_file_path)

    def _save_task(self, task_data: TaskData) -> None:
        task_dir = self._get_task_dir(task_data.id)
        task_data_file = os.path.join(task_dir, '.meta', 'task_data.json')
        os.makedirs(os.path.dirname(task_data_file), exist_ok=True)
        with open(task_data_file, 'w', encoding='utf-8') as f:
            json.dump(task_data.model_dump(), f)

    def _load_unfinished_tasks(self) -> None:
        tasks_dir = os.path.join(self.dir, 'tasks')
        if not os.path.exists(tasks_dir):
            return
        for task_id in os.listdir(tasks_dir):
            try:
                task_data = self.get_task(task_id)
                if task_data.state not in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.ERROR):
                    self._unfinished_tasks[task_id] = task_data
            except Exception as e:
                logger.warning(f"Failed to load task {task_id}: {e}")

    def _get_task_dir(self, task_id: str) -> str:
        return os.path.join(self.dir, 'tasks', task_id)
