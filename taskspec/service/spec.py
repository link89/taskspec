from logging import getLogger
import json
import time
import os
import uuid
import glob
import asyncio
from typing import Dict, Any, AsyncGenerator, Set, Optional

from ..executor import ExecutorService
from ..schema import SpecData, TaskInput, TaskData, TaskState
from ..util import gen_task_id, fset, fget, fdel

logger = getLogger(__name__)

class SpecService:
    def __init__(self, name: str, dir: str, spec: SpecData, executor: ExecutorService,
                 public_url: str):
        self.name = name
        self.dir = dir
        self._spec = spec
        self._executor = executor
        self._active_tasks: Set[str] = set()
        self._worker_tasks: Set[str] = set()
        self._queue: asyncio.Queue = asyncio.Queue()
        self._public_url: str = public_url

    def init(self) -> None:
        self._load_active_tasks()
        asyncio.create_task(self._poll_loop())

    def get_queue_token(self) -> str:
        queue_dir = os.path.join(self.dir, "queue")
        os.makedirs(queue_dir, exist_ok=True)
        token_file = os.path.join(queue_dir, "token")
        token = fget(token_file)
        if not token:
            token = str(uuid.uuid4())
            fset(token_file, token)
        return token.strip()

    async def _manage_workers(self) -> None:
        if not self._spec.worker_pool:
            return

        needed = self._spec.worker_pool.workers - len(self._worker_tasks)
        if needed > 0:
            logger.info(f"Creating {needed} workers for {self.name}")
            for _ in range(needed):
                try:
                    await self._create_worker()
                except Exception as e:
                    logger.error(f"Failed to create worker for {self.name}: {e}")

    async def _create_worker(self) -> TaskData:
        worker_input = TaskInput(submit=True)
        return await self.create_task(worker_input, is_worker=True)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.poll_state()
                await self._manage_workers()
            except Exception as e:
                logger.error(f"Error in poll_loop for {self.name}: {e}")
            await asyncio.sleep(self._spec.poll_interval_s)

    async def poll_state(self) -> None:
        await self._poll_items(self._active_tasks, is_worker=False)
        await self._poll_items(self._worker_tasks, is_worker=True)

    async def _poll_items(self, target_set: Set[str], is_worker: bool) -> None:
        if not target_set:
            return

        # Create a copy of the set to iterate over while potentially modifying it
        task_ids = list(target_set)
        for task_id in task_ids:
            try:
                task_data = self.get_task(task_id, is_worker=is_worker)
                old_state = task_data.state
                new_state = await self._executor.runner.query_state(self._spec, task_data)

                if new_state != old_state:
                    task_data.state = new_state
                    task_data.updated_at = int(time.time())
                    self._save_task(task_data)
                    logger.info(f"{'Worker' if is_worker else 'Task'} {task_id} state changed from {old_state} to {new_state}")

                if TaskState.is_terminated(new_state):
                    target_set.remove(task_id)
                    logger.info(f"{'Worker' if is_worker else 'Task'} {task_id} finished, removed from active set")
                    # remove active file
                    fdel(self._get_task_active_file(task_id, is_worker=is_worker))
            except Exception as e:
                logger.error(f"Failed to poll state for {'worker' if is_worker else 'task'} {task_id}: {e}")

    async def create_task(self, task_input: TaskInput, is_worker: bool = False) -> TaskData:
        task_id = gen_task_id(task_input.idempotent_key)
        task_dir = self._get_task_dir(task_id, is_worker=is_worker)

        # prepare local task directories
        os.makedirs(task_dir, exist_ok=True)

        # prepare remote executor directories
        remote_base_dir = self._executor.connector.get_base_dir()

        now = int(time.time())
        task_data = TaskData(
            id=task_id,
            state=TaskState.IDLE,
            created_at=now,
            updated_at=now,
            is_worker=is_worker
        )
        task_prefix = task_data.get_prefix(self._spec)
        remote_task_dir = os.path.join(remote_base_dir, task_prefix)
        await self._executor.connector.mkdir(remote_task_dir, exist_ok=True)

        # copy files to executor
        input_files = self._spec.files
        if is_worker and self._spec.worker_pool:
            input_files = self._spec.worker_pool.files

        for file in input_files:
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

        self._save_task_input(task_id, task_input, is_worker=is_worker)
        self._save_task(task_data)

        # create active file when task is created
        fset(self._get_task_active_file(task_id, is_worker=is_worker))

        if task_input.submit:
            if self._spec.worker_pool and not is_worker:
                # Submit to queue in worker_pool mode
                await self.submit_to_queue(task_id)
            else:
                # Current on-demand mode
                try:
                    env = None
                    if is_worker:
                        env = {
                            "__TASK_QUEUE_URL": f"{self._public_url}/specs/{self.name}/queue/",
                            "__TASK_QUEUE_TOKEN": self.get_queue_token()
                        }
                    task_data = await self._executor.runner.submit(self._spec, task_data, env=env)
                    if not TaskState.is_terminated(task_data.state):
                        if is_worker:
                            self._worker_tasks.add(task_id)
                        else:
                            self._active_tasks.add(task_id)
                    else:
                        # if terminated immediately, remove active file
                        fdel(self._get_task_active_file(task_id, is_worker=is_worker))
                except Exception:
                    task_data.state = TaskState.ERROR
                    fdel(self._get_task_active_file(task_id, is_worker=is_worker))
                    raise
                finally:
                    task_data.updated_at = int(time.time())
                    self._save_task(task_data)
        return task_data

    async def submit_to_queue(self, task_id: str) -> None:
        task_dir = self._get_task_dir(task_id, is_worker=False)
        meta_dir = os.path.join(task_dir, self._spec.meta_dir)
        fset(os.path.join(meta_dir, "qstate"), "enqueue")
        await self._queue.put(task_id)
        logger.info(f"Task {task_id} submitted to queue")

    async def get_from_queue(self, wait_s: int = 30) -> Optional[Dict[str, str]]:
        try:
            task_id = await asyncio.wait_for(self._queue.get(), timeout=wait_s)
        except asyncio.TimeoutError:
            return None

        task_data = self.get_task(task_id, is_worker=False)
        task_dir = self._get_task_dir(task_id, is_worker=False)
        meta_dir = os.path.join(task_dir, self._spec.meta_dir)
        fset(os.path.join(meta_dir, "qstate"), "dequeue")

        remote_base_dir = self._executor.connector.get_base_dir()
        remote_task_dir = os.path.join(remote_base_dir, task_data.get_prefix(self._spec))

        return {
            "id": task_id,
            "full_task_dir": await self._executor.connector.get_abs_path(remote_task_dir)
        }

    async def complete_task(self, task_id: str, state: str) -> None:
        task_data = self.get_task(task_id, is_worker=False)
        if state == "ok":
            task_data.state = TaskState.SUCCEEDED
        else:
            task_data.state = TaskState.FAILED

        task_data.updated_at = int(time.time())
        self._save_task(task_data)
        fdel(self._get_task_active_file(task_id, is_worker=False))
        logger.info(f"Task {task_id} completed with state {state}")

    def get_task(self, task_id: str, is_worker: bool = False) -> TaskData:
        task_data_file = self._get_task_data_file(task_id, is_worker)
        if not os.path.exists(task_data_file):
            raise FileNotFoundError(f"Task data file not found: {task_data_file}")
        with open(task_data_file, 'r', encoding='utf-8') as f:
            task_data = json.load(f)
        data = TaskData(**task_data)
        data.is_worker = is_worker
        return data

    def get_task_input(self, task_id: str, is_worker: bool = False) -> TaskInput:
        task_input_file = self._get_task_input_file(task_id, is_worker)
        if not os.path.exists(task_input_file):
            raise FileNotFoundError(f"Task input file not found: {task_input_file}")
        with open(task_input_file, 'r', encoding='utf-8') as f:
            task_input = json.load(f)
        return TaskInput(**task_input)

    async def get_task_file(self, task_id: str, file_path: str, is_worker: bool = False) -> AsyncGenerator[bytes, Any]:
        task_data = self.get_task(task_id, is_worker)
        file_path = os.path.normpath(file_path)
        remote_base_dir = self._executor.connector.get_base_dir()
        remote_task_dir = os.path.join(remote_base_dir, task_data.get_prefix(self._spec))
        remote_file_path = os.path.normpath(os.path.join(remote_task_dir, file_path))

        if not remote_file_path.startswith(os.path.normpath(remote_task_dir)):
            raise ValueError(f"Illegal file path: {file_path}")

        if not await self._executor.connector.exists(remote_file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        return self._executor.connector.get_fstream(remote_file_path)

    def _save_task(self, task_data: TaskData) -> None:
        task_data_file = self._get_task_data_file(task_data.id, task_data.is_worker)
        os.makedirs(os.path.dirname(task_data_file), exist_ok=True)
        with open(task_data_file, 'w', encoding='utf-8') as f:
            json.dump(task_data.model_dump(), f)

    def _save_task_input(self, task_id: str, task_input: TaskInput, is_worker: bool = False) -> None:
        task_input_file = self._get_task_input_file(task_id, is_worker)
        os.makedirs(os.path.dirname(task_input_file), exist_ok=True)
        with open(task_input_file, 'w', encoding='utf-8') as f:
            json.dump(task_input.model_dump(), f)

    def _load_active_tasks(self) -> None:
        self._scan_active_items('tasks', self._active_tasks, is_worker=False)
        self._scan_active_items('workers', self._worker_tasks, is_worker=True)

    def _scan_active_items(self, sub_dir: str, target_set: Set[str], is_worker: bool) -> None:
        base_dir = os.path.join(self.dir, sub_dir)
        if not os.path.exists(base_dir):
            return
        # Optimization: Use glob to find all task active files in nested structure: {sub_dir}/??/*/active
        pattern = os.path.join(base_dir, '??', '*', self._spec.meta_dir, "active")
        for active_file_path in glob.glob(pattern):
            if not os.path.isfile(active_file_path):
                continue
            # Parts should be /.../{sub_dir}/{aa}/{rest}/{meta_dir}/active
            parts = active_file_path.split(os.sep)
            # active file path ends with /{sub_dir}/{aa}/{rest}/{meta_dir}/active
            # So task_id consists of parts[-4] and parts[-3]
            task_id = parts[-4] + parts[-3]
            try:
                task_data = self.get_task(task_id, is_worker=is_worker)
                if not TaskState.is_terminated(task_data.state):
                    target_set.add(task_id)
                else:
                    # found terminated but active file still exists, remove it
                    fdel(active_file_path)
            except Exception as e:
                logger.warning(f"Failed to load {sub_dir} {task_id}: {e}")

    def _get_task_dir(self, task_id: str, is_worker: bool = False) -> str:
        assert len(task_id) > 2, "task_id should >2"
        base = 'workers' if is_worker else 'tasks'
        return os.path.normpath(os.path.join(self.dir, base, task_id[:2], task_id[2:]))

    def _get_task_data_file(self, task_id: str, is_worker: bool = False) -> str:
        return os.path.join(self._get_task_dir(task_id, is_worker), self._spec.meta_dir, "task.json")

    def _get_task_input_file(self, task_id: str, is_worker: bool = False) -> str:
        return os.path.join(self._get_task_dir(task_id, is_worker), self._spec.meta_dir, "input.json")

    def _get_task_active_file(self, task_id: str, is_worker: bool = False) -> str:
        return os.path.join(self._get_task_dir(task_id, is_worker), self._spec.meta_dir, "active")
