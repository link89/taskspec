from logging import getLogger
import json
import time
import os
import uuid
import glob
import asyncio
from collections import deque
from typing import Any, AsyncGenerator, Deque, Dict, Optional, Set
from weakref import WeakValueDictionary

from ..executor import ExecutorService
from ..schema import SpecData, TaskData, TaskInput, TaskKind, TaskState
from ..task import Task
from ..util import fdel, fget, fset, gen_task_id

logger = getLogger(__name__)


class SpecService:
    def __init__(
        self,
        name: str,
        dir: str,
        spec: SpecData,
        executor: ExecutorService,
        public_url: str,
    ):
        self.name = name
        self.dir = dir
        self._spec = spec
        self._executor = executor
        self._public_url = public_url

        self._active_tasks: Set[str] = set()
        self._worker_tasks: Set[str] = set()
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued_at: Dict[str, float] = {}
        self._enqueue_events: Deque[float] = deque()
        self._dequeue_events: Deque[float] = deque()
        self._task_done_events: Dict[str, asyncio.Event] = {}
        self._tasks: WeakValueDictionary[tuple[str, TaskKind], Task] = WeakValueDictionary()

    def init(self) -> None:
        self._load_active_tasks()
        logger.info(
            "Initialized spec %s with %d active tasks and %d active workers",
            self.name,
            len(self._active_tasks),
            len(self._worker_tasks),
        )
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

        pool = self._spec.worker_pool
        current_workers = len(self._worker_tasks)
        needed = pool.min_workers - current_workers
        if needed > 0:
            logger.info("Creating %d workers for %s", needed, self.name)
            for _ in range(needed):
                await self._create_worker_safe()
            return

        scale_up_by = self._get_scale_up_count(current_workers)
        if scale_up_by <= 0:
            return

        logger.info("Scaling up %d workers for %s", scale_up_by, self.name)
        for _ in range(scale_up_by):
            await self._create_worker_safe()

    async def _create_worker_safe(self) -> None:
        try:
            await self._create_worker()
        except Exception as exc:
            logger.error("Failed to create worker for %s: %s", self.name, exc)

    def _get_scale_up_count(self, current_workers: int) -> int:
        pool = self._spec.worker_pool
        if not pool:
            return 0
        max_workers = pool.max_workers if pool.max_workers is not None else pool.min_workers
        if current_workers >= max_workers:
            return 0

        stats = self._get_scaling_stats()
        try:
            result = eval(pool.scale_up_condition, {"__builtins__": {}}, stats)
        except Exception as exc:
            logger.error("Failed to evaluate scale_up_condition for %s: %s", self.name, exc)
            return 0

        if isinstance(result, bool):
            requested = 1 if result else 0
        elif isinstance(result, int):
            requested = result
        elif isinstance(result, float):
            requested = round(result)
        else:
            logger.error(
                "scale_up_condition for %s must return bool, int, or float, got %s",
                self.name,
                type(result).__name__,
            )
            return 0

        if requested <= 0:
            return 0
        return min(requested, max_workers - current_workers)

    def _get_scaling_stats(self) -> Dict[str, Any]:
        pool = self._spec.worker_pool
        if not pool:
            return {}

        now = time.time()
        self._trim_stat_events(now, pool.stat_window_s)
        wait_times = [now - queued_at for queued_at in self._queued_at.values()]
        return {
            "q_size": self._queue.qsize(),
            "w_active": len(self._worker_tasks),
            "max_wait": max(wait_times) if wait_times else 0.0,
            "avg_wait": sum(wait_times) / len(wait_times) if wait_times else 0.0,
            "in_rate": len(self._enqueue_events) * 60.0 / pool.stat_window_s,
            "out_rate": len(self._dequeue_events) * 60.0 / pool.stat_window_s,
        }

    def _trim_stat_events(self, now: float, window_s: int) -> None:
        cutoff = now - window_s
        while self._enqueue_events and self._enqueue_events[0] < cutoff:
            self._enqueue_events.popleft()
        while self._dequeue_events and self._dequeue_events[0] < cutoff:
            self._dequeue_events.popleft()

    async def _create_worker(self) -> Task:
        return await self.create_task(TaskInput(submit=True), TaskKind.WORKER)

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.poll_state()
                await self._manage_workers()
            except Exception as exc:
                logger.error("Error in poll_loop for %s: %s", self.name, exc)
            await asyncio.sleep(self._spec.poll_interval_s)

    async def poll_state(self) -> None:
        if self._spec.on_demand:
            await self._poll_items(self._active_tasks, TaskKind.NORMAL)
        await self._poll_items(self._worker_tasks, TaskKind.WORKER)

    async def _poll_items(self, target_set: Set[str], kind: TaskKind) -> None:
        if not target_set:
            return

        for task_id in list(target_set):
            try:
                task = self.get_task(task_id, kind)
                old_state = task.state
                new_state = await self._executor.runner.query_state(self._spec, task)
                if new_state != old_state:
                    task.set_state(new_state)
                    task.touch()
                    task.save()
                    logger.info(
                        "%s %s in spec %s state changed from %s to %s",
                        task.get_kind_name().capitalize(),
                        task_id,
                        self.name,
                        old_state.name,
                        new_state.name,
                    )

                if TaskState.is_terminated(new_state):
                    target_set.remove(task_id)
                    task.deactivate()
                    logger.info(
                        "%s %s in spec %s reached terminal state %s and was removed from active set",
                        task.get_kind_name().capitalize(),
                        task_id,
                        self.name,
                        new_state.name,
                    )
                    if kind == TaskKind.NORMAL:
                        self._signal_task_done(task_id)
            except Exception as exc:
                logger.error(
                    "Failed to poll state for %s %s in spec %s: %s",
                    "worker" if kind == TaskKind.WORKER else "task",
                    task_id,
                    self.name,
                    exc,
                )

    async def create_task(self, task_input: TaskInput, kind: TaskKind = TaskKind.NORMAL) -> Task:
        task_id = gen_task_id(task_input.idempotent_key)
        task = self._new_task(
            TaskData(
                id=task_id,
                state=TaskState.IDLE,
                created_at=int(time.time()),
                updated_at=int(time.time()),
            ),
            kind,
        )
        task_type = task.get_kind_name()
        task_dir = task.get_task_dir()
        os.makedirs(task_dir, exist_ok=True)

        remote_task_dir = os.path.join(self._executor.connector.get_base_dir(), task.get_prefix())
        logger.info(
            "Creating %s %s for spec %s (submit=%s, remote_dir=%s)",
            task_type,
            task_id,
            self.name,
            task_input.submit,
            remote_task_dir,
        )
        await self._executor.connector.mkdir(remote_task_dir, exist_ok=True)

        for file in task.get_input_files():
            src_path = file.src
            dst_path = file.dst or src_path
            real_src_path = os.path.join(self.dir, src_path)
            real_dst_path = os.path.join(remote_task_dir, dst_path)
            await self._executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await self._executor.connector.put(real_src_path, real_dst_path)

        for file_data in task_input.files:
            real_dst_path = os.path.join(remote_task_dir, file_data.name)
            await self._executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await self._executor.connector.dump_text(file_data.content, real_dst_path)

        task.save_input(task_input)
        task.save()
        task.activate()
        logger.info(
            "Prepared %s %s for spec %s with %d copied files and %d generated files",
            task_type,
            task_id,
            self.name,
            len(task.get_input_files()),
            len(task_input.files),
        )

        if task_input.submit:
            if self._spec.worker_pool and kind == TaskKind.NORMAL:
                logger.info("Submitting %s %s for spec %s to worker queue", task_type, task_id, self.name)
                self._task_done_events[task_id] = asyncio.Event()
                await self.submit_to_queue(task_id)
            else:
                try:
                    logger.info("Submitting %s %s for spec %s via runner", task_type, task_id, self.name)
                    await self._executor.runner.submit(self._spec, task)
                    if not TaskState.is_terminated(task.state):
                        if kind == TaskKind.WORKER:
                            self._worker_tasks.add(task_id)
                        else:
                            self._active_tasks.add(task_id)
                            self._task_done_events[task_id] = asyncio.Event()
                        logger.info(
                            "Tracking %s %s for spec %s in %s set with state %s",
                            task_type,
                            task_id,
                            self.name,
                            "worker" if kind == TaskKind.WORKER else "active",
                            task.state.name,
                        )
                    else:
                        task.deactivate()
                        logger.info(
                            "%s %s for spec %s terminated immediately with state %s",
                            task_type.capitalize(),
                            task_id,
                            self.name,
                            task.state.name,
                        )
                except Exception:
                    task.set_state(TaskState.ERROR)
                    task.deactivate()
                    logger.exception("Failed to submit %s %s for spec %s", task_type, task_id, self.name)
                    raise
                finally:
                    task.touch()
                    task.save()
        return task

    async def submit_to_queue(self, task_id: str) -> None:
        task = self.get_task(task_id, TaskKind.NORMAL)
        task.enqueue()
        now = time.time()
        self._queued_at[task_id] = now
        self._enqueue_events.append(now)
        await self._queue.put(task_id)
        logger.info(
            "Task %s submitted to queue for spec %s (queue_size=%d)",
            task_id,
            self.name,
            self._queue.qsize(),
        )

    async def get_from_queue(self, wait_s: int = 30) -> Optional[Dict[str, str]]:
        try:
            task_id = await asyncio.wait_for(self._queue.get(), timeout=wait_s)
        except asyncio.TimeoutError:
            return None

        self._queued_at.pop(task_id, None)
        self._dequeue_events.append(time.time())

        task = self.get_task(task_id, TaskKind.NORMAL)
        task.dequeue()
        remote_task_dir = os.path.join(self._executor.connector.get_base_dir(), task.get_prefix())
        logger.info(
            "Dequeued task %s for spec %s (remaining_queue_size=%d)",
            task_id,
            self.name,
            self._queue.qsize(),
        )
        return {
            "id": task_id,
            "full_task_dir": await self._executor.connector.get_abs_path(remote_task_dir),
        }

    async def complete_task(self, task_id: str, state: str) -> None:
        task = self.get_task(task_id, TaskKind.NORMAL)
        task.set_state(TaskState.SUCCEEDED if state == "ok" else TaskState.FAILED)
        task.touch()
        task.save()
        task.deactivate()
        self._signal_task_done(task_id)
        logger.info(
            "Task %s in spec %s completed with reported state %s and final task state %s",
            task_id,
            self.name,
            state,
            task.state.name,
        )

    def _signal_task_done(self, task_id: str) -> None:
        event = self._task_done_events.pop(task_id, None)
        if event is not None:
            event.set()

    async def wait_for_terminated(self, task_id: str, wait_s: float) -> None:
        task = self.get_task(task_id, TaskKind.NORMAL)
        if TaskState.is_terminated(task.state):
            return

        event = self._task_done_events.get(task_id)
        if event is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(event.wait()), timeout=wait_s)
        except asyncio.TimeoutError:
            pass

    def get_task(self, task_id: str, kind: TaskKind = TaskKind.NORMAL) -> Task:
        key = (task_id, TaskKind(kind))
        task = self._tasks.get(key)
        if task is not None:
            return task

        task_data_file = self._get_task_data_file(task_id, kind)
        if not os.path.exists(task_data_file):
            raise FileNotFoundError(f"Task data file not found: {task_data_file}")
        with open(task_data_file, "r", encoding="utf-8") as handle:
            task_data = TaskData(**json.load(handle))
        return self._new_task(task_data, kind)

    async def get_task_async(
        self,
        task_id: str,
        kind: TaskKind = TaskKind.NORMAL,
        wait: float = 0,
    ) -> Task:
        if wait > 0 and kind == TaskKind.NORMAL:
            await self.wait_for_terminated(task_id, wait_s=wait)
        return self.get_task(task_id, kind)

    def get_task_input(self, task_id: str, kind: TaskKind = TaskKind.NORMAL) -> TaskInput:
        return self.get_task(task_id, kind).get_input()

    async def get_task_file(
        self,
        task_id: str,
        file_path: str,
        kind: TaskKind = TaskKind.NORMAL,
        offset: int = 0,
    ) -> AsyncGenerator[bytes, Any]:
        task = self.get_task(task_id, kind)
        file_path = os.path.normpath(file_path)
        remote_task_dir = os.path.join(self._executor.connector.get_base_dir(), task.get_prefix())
        remote_file_path = os.path.normpath(os.path.join(remote_task_dir, file_path))
        if not remote_file_path.startswith(os.path.normpath(remote_task_dir)):
            raise ValueError(f"Illegal file path: {file_path}")
        return await self._executor.connector.get_fstream(remote_file_path, offset=offset)

    def _new_task(self, task_data: TaskData, kind: TaskKind) -> Task:
        task = Task(
            task_data,
            self._spec,
            kind,
            spec_dir=self.dir,
            public_url=self._public_url,
            queue_token_provider=self.get_queue_token,
            save_task_data=self._save_task,
            save_task_input=self._save_task_input,
            load_task_input=self._load_task_input,
        )
        self._tasks[(task.id, TaskKind(kind))] = task
        return task

    def _save_task(self, task: Task) -> None:
        task_data_file = task.get_data_file()
        os.makedirs(os.path.dirname(task_data_file), exist_ok=True)
        with open(task_data_file, "w", encoding="utf-8") as handle:
            json.dump(task.task_data.model_dump(), handle)

    def _save_task_input(self, task: Task, task_input: TaskInput) -> None:
        task_input_file = task.get_input_file()
        os.makedirs(os.path.dirname(task_input_file), exist_ok=True)
        with open(task_input_file, "w", encoding="utf-8") as handle:
            json.dump(task_input.model_dump(), handle)

    def _load_task_input(self, task: Task) -> TaskInput:
        task_input_file = task.get_input_file()
        if not os.path.exists(task_input_file):
            raise FileNotFoundError(f"Task input file not found: {task_input_file}")
        with open(task_input_file, "r", encoding="utf-8") as handle:
            return TaskInput(**json.load(handle))

    def _load_active_tasks(self) -> None:
        self._scan_active_items("tasks", self._active_tasks, TaskKind.NORMAL)
        self._scan_active_items("workers", self._worker_tasks, TaskKind.WORKER)

    def _scan_active_items(self, sub_dir: str, target_set: Set[str], kind: TaskKind) -> None:
        base_dir = os.path.join(self.dir, sub_dir)
        if not os.path.exists(base_dir):
            return

        pattern = os.path.join(base_dir, "??", "*", self._spec.meta_dir, "active")
        for active_file_path in glob.glob(pattern):
            if not os.path.isfile(active_file_path):
                continue
            parts = active_file_path.split(os.sep)
            task_id = parts[-4] + parts[-3]
            try:
                task = self.get_task(task_id, kind)
                if not TaskState.is_terminated(task.state):
                    target_set.add(task_id)
                    if kind == TaskKind.NORMAL:
                        self._task_done_events[task_id] = asyncio.Event()
                else:
                    fdel(active_file_path)
            except Exception as exc:
                logger.warning("Failed to load %s %s: %s", sub_dir, task_id, exc)

    def _get_task_data_file(self, task_id: str, kind: TaskKind = TaskKind.NORMAL) -> str:
        return os.path.join(self._get_task_dir(task_id, kind), self._spec.meta_dir, "task.json")

    def _get_task_dir(self, task_id: str, kind: TaskKind = TaskKind.NORMAL) -> str:
        assert len(task_id) > 2, "task_id should >2"
        base = "workers" if kind == TaskKind.WORKER else "tasks"
        return os.path.normpath(os.path.join(self.dir, base, task_id[:2], task_id[2:]))
