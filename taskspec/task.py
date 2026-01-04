from pydantic import BaseModel
from typing import Optional, List
from logging import getLogger
import json
import time
import yaml
import os

import uuid

from .executor import ExecutorServiceManager
from .spec import TaskSpec, TaskInput, TaskData

logger = getLogger(__name__)

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
        spec_dir = self._get_spec_dir(spec_name)
        # TODO: check idempotent_key uniqueness

        task_id = str(uuid.uuid4())

        # prepare local task directories
        task_prefix = self._get_task_prefix(spec_name, task_id)
        task_dir = os.path.join(self._base_dir, task_prefix)
        meta_dir = os.path.join(task_dir, '.meta')
        os.makedirs(meta_dir, exist_ok=True)

        # prepare remote executor directories
        remote_task_dir = os.path.join(executor.connector.get_base_dir(), task_prefix)
        await executor.connector.mkdir(remote_task_dir, exist_ok=True)

        # copy in_files to executor
        for in_file in spec.in_files:
            src_path = in_file.src
            dst_path = in_file.dst
            if not dst_path:
                dst_path = src_path

            real_src_path = os.path.join(spec_dir, src_path)
            real_dst_path = os.path.join(remote_task_dir, dst_path)
            await executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await executor.connector.put(real_src_path, real_dst_path)

        # generate files from TaskInput
        for file_data in task_input.files:
            real_dst_path = os.path.join(remote_task_dir, file_data.name)
            await executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await executor.connector.dump_text(file_data.content, real_dst_path)

        task_data = TaskData(
            id=task_id,
            prefix=task_prefix,
            spec=spec,
            input=task_input,
            created_at=int(time.time()),
        )
        self.save_task(task_data)

        await executor.runner.submit(task_data)
        self.save_task(task_data)
        return task_data

    def get_task_file(self, spec_name: str, task_id: str, file_path: str):
        # normalize file_path
        file_path = os.path.normpath(file_path)
        if file_path.startswith('..'):
            raise ValueError("file_path cannot navigate outside the task directory")

        task_data = self.get_task(spec_name, task_id)
        executor = self._executor_mgr.get_executor(task_data.spec.executor)
        remote_task_dir = os.path.join(executor.connector.get_base_dir(), task_data.prefix)
        full_file_path = os.path.join(remote_task_dir, file_path)
        # TODO: ensure file not outside task directory
        return executor.connector.get_fstream(full_file_path)

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

    def _get_task_prefix(self, spec_name: str, task_id: str):
        return f'specs/{spec_name}/tasks/{task_id}'

    def get_task(self, spec_name: str, task_id: str) -> TaskData:
        task_prefix = self._get_task_prefix(spec_name, task_id)
        task_data_file = os.path.join(self._base_dir, task_prefix,
                                      '.meta', 'task_data.json')
        if not os.path.exists(task_data_file):
            raise FileNotFoundError(f"Task data file not found: {task_data_file}")
        with open(task_data_file, 'r', encoding='utf-8') as f:
            task_data_dict = json.load(f)
        return TaskData(**task_data_dict)


    def save_task(self, task_data: TaskData):
        task_data_file = os.path.join(self._base_dir, task_data.prefix,
                                      '.meta', 'task_data.json')
        with open(task_data_file, 'w', encoding='utf-8') as f:
            json.dump(task_data.model_dump(), f)


