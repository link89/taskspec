from logging import getLogger
import json
import time
import os
import uuid
import glob
import asyncio
from typing import Dict, Any, AsyncGenerator, Set

from ..executor import ExecutorService
from ..schema import SpecData, TaskInput, TaskData, TaskState
from ..util import gen_task_id

logger = getLogger(__name__)

class SpecService:
    def __init__(self, name: str, dir: str, spec: SpecData, executor: ExecutorService):
        self.name = name
        self.dir = dir
        self._spec = spec
        self._executor = executor
        self._unfinished_tasks: Set[str] = set()

    def init(self) -> None:
        self._load_unfinished_tasks()
        asyncio.create_task(self._poll_loop())

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.poll_state()
            except Exception as e:
                logger.error(f"Error in poll_loop for {self.name}: {e}")
            await asyncio.sleep(self._spec.poll_interval_s)

    async def poll_state(self) -> None:
        if not self._unfinished_tasks:
            return

        # Create a copy of the set to iterate over while potentially modifying it
        task_ids = list(self._unfinished_tasks)
        for task_id in task_ids:
            try:
                task_data = self.get_task(task_id)
                old_state = task_data.state
                new_state = await self._executor.runner.query_state(self._spec, task_data)

                if new_state != old_state:
                    task_data.state = new_state
                    self._save_task(task_data)
                    logger.info(f"Task {task_id} state changed from {old_state} to {new_state}")

                if new_state in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.ERROR):
                    self._unfinished_tasks.remove(task_id)
                    logger.info(f"Task {task_id} finished, removed from unfinished_tasks")
            except Exception as e:
                logger.error(f"Failed to poll state for task {task_id}: {e}")

    async def create_task(self, task_input: TaskInput) -> TaskData:
        task_id = gen_task_id(task_input.idempotent_key)
        task_dir = self._get_task_dir(task_id)

        # prepare local task directories
        os.makedirs(task_dir, exist_ok=True)

        # prepare remote executor directories
        remote_base_dir = self._executor.connector.get_base_dir()

        task_data = TaskData(
            id=task_id,
            state=TaskState.IDLE,
            input=task_input,
            created_at=int(time.time()),
        )
        task_prefix = task_data.get_prefix(self._spec)
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

        self._save_task(task_data)
        if task_input.submit:
            try:
                task_data = await self._executor.runner.submit(self._spec, task_data)
                if task_data.state not in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.ERROR):
                    self._unfinished_tasks.add(task_id)
            except Exception:
                task_data.state = TaskState.ERROR
                raise
            finally:
                self._save_task(task_data)
        return task_data

    def get_task(self, task_id: str) -> TaskData:
        task_dir = self._get_task_dir(task_id)
        task_data_file = os.path.join(task_dir, self._spec.task_file)
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
        task_data_file = os.path.join(task_dir, self._spec.task_file)
        os.makedirs(os.path.dirname(task_data_file), exist_ok=True)
        with open(task_data_file, 'w', encoding='utf-8') as f:
            json.dump(task_data.model_dump(), f)

    def _load_unfinished_tasks(self) -> None:
        tasks_dir = os.path.join(self.dir, 'tasks')
        if not os.path.exists(tasks_dir):
            return
        # Use glob to find all task directories in nested structure: tasks/??/*
        pattern = os.path.join(tasks_dir, '??', '*')
        for task_path in glob.glob(pattern):
            if not os.path.isdir(task_path):
                continue
            # task_id is the last two parts of the path combined
            parts = task_path.split(os.sep)
            # parts[-2] is the first 2 chars, parts[-1] is the rest
            task_id = parts[-2] + parts[-1]
            try:
                task_data = self.get_task(task_id)
                if task_data.state not in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.ERROR):
                    self._unfinished_tasks.add(task_id)
            except Exception as e:
                logger.warning(f"Failed to load task {task_id}: {e}")

    def _get_task_dir(self, task_id: str) -> str:
        assert len(task_id) > 2, "task_id should >2"
        return os.path.join(self.dir, 'tasks', task_id[:2], task_id[2:])
