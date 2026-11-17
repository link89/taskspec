from logging import getLogger
import json
import time
import os
import uuid
import glob
import asyncio
from collections import deque
from typing import Any, AsyncGenerator, Callable, Deque, Dict, Optional, Set, TypeVar
from weakref import WeakValueDictionary

from ..executor import ExecutorService
from ..schema import SpecData, TaskData, TaskInput, TaskState
from ..task import Task
from ..worker import Worker
from ..util import fdel, fget, fset, gen_task_id

logger = getLogger(__name__)

Record = TypeVar("Record", Task, Worker)


class SpecService:
    def __init__(
        self,
        name: str,
        spec_dir: str,
        spec_data: SpecData,
        executor: ExecutorService,
        public_url: str,
    ):
        self.name = name
        self.dir = spec_dir
        self._spec_data = spec_data
        self._executor = executor
        self._public_url = public_url

        self._active_tasks: Set[str] = set()
        self._active_workers: Set[str] = set()
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued_at: Dict[str, float] = {}
        self._enqueue_events: Deque[float] = deque()
        self._dequeue_events: Deque[float] = deque()
        self._task_done_events: Dict[str, asyncio.Event] = {}
        self._tasks: WeakValueDictionary[str, Task] = WeakValueDictionary()
        self._workers: WeakValueDictionary[str, Worker] = WeakValueDictionary()

    def init(self) -> None:
        self._load_active_tasks()
        logger.info(
            "Initialized spec %s with %d active tasks and %d active workers",
            self.name,
            len(self._active_tasks),
            len(self._active_workers),
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
        if not self._spec_data.worker_pool:
            return

        pool = self._spec_data.worker_pool
        current_workers = len(self._active_workers)
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
        pool = self._spec_data.worker_pool
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
        pool = self._spec_data.worker_pool
        if not pool:
            return {}

        now = time.time()
        self._trim_stat_events(now, pool.stat_window_s)
        wait_times = [now - queued_at for queued_at in self._queued_at.values()]
        return {
            "q_size": self._queue.qsize(),
            "w_active": len(self._active_workers),
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

    async def _create_worker(self) -> Worker:
        worker = Worker(self._new_data(gen_task_id()), self._spec_data, spec_dir=self.dir)
        self._workers[worker.id] = worker
        await self._prepare_files(worker, TaskInput())
        await self._submit_run(
            worker, self._active_workers,
            env={
                "__TASK_QUEUE_URL": f"{self._public_url}/specs/{self.name}/queue/",
                "__TASK_QUEUE_TOKEN": self.get_queue_token(),
            },
        )
        return worker

    async def _poll_loop(self) -> None:
        while True:
            try:
                await self.poll_state()
                await self._manage_workers()
            except Exception as exc:
                logger.error("Error in poll_loop for %s: %s", self.name, exc)
            await asyncio.sleep(self._spec_data.poll_interval_s)

    async def poll_state(self) -> None:
        if self._spec_data.on_demand:
            await self._poll_runs(self._active_tasks, self.get_task, self._signal_task_done)
        await self._poll_runs(self._active_workers, self.get_worker)

    async def _poll_runs(
        self,
        target_set: Set[str],
        load: Callable[[str], Task | Worker],
        on_done: Optional[Callable[[str], None]] = None,
    ) -> None:
        for run_id in list(target_set):
            try:
                run = load(run_id)
                old_state = run.state
                new_state = await self._executor.runner.query_state(run)
                if new_state != old_state:
                    run.set_state(new_state)
                    run.touch()
                    run.save()
                    logger.info(
                        "%s in spec %s state changed from %s to %s",
                        run, self.name, old_state.name, new_state.name,
                    )
                if run.is_terminated():
                    target_set.remove(run_id)
                    run.deactivate()
                    logger.info(
                        "%s in spec %s reached terminal state %s and was removed from active set",
                        run, self.name, new_state.name,
                    )
                    if on_done:
                        on_done(run_id)
            except Exception as exc:
                logger.error("Failed to poll %s in spec %s: %s", run_id, self.name, exc)

    @staticmethod
    def _new_data(record_id: str) -> TaskData:
        now = int(time.time())
        return TaskData(id=record_id, created_at=now, updated_at=now)

    async def create_task(self, task_input: TaskInput) -> Task:
        task = Task(
            self._new_data(gen_task_id(task_input.idempotent_key)),
            self._spec_data, spec_dir=self.dir,
        )
        self._tasks[task.id] = task
        await self._prepare_files(task, task_input)
        if task_input.submit:
            if self._spec_data.worker_pool:
                self._task_done_events[task.id] = asyncio.Event()
                await self.submit_to_queue(task.id)
            else:
                await self._submit_run(task, self._active_tasks)
                if not task.is_terminated():
                    self._task_done_events[task.id] = asyncio.Event()
        return task

    async def _prepare_files(self, record: Task | Worker, input_data: TaskInput) -> None:
        local_dir = record.get_dir()
        os.makedirs(local_dir, exist_ok=True)

        remote_dir = os.path.join(self._executor.connector.get_base_dir(), record.get_prefix())
        logger.info(
            "Creating %s for spec %s (submit=%s, remote_dir=%s)",
            record,
            self.name,
            input_data.submit,
            remote_dir,
        )
        await self._executor.connector.mkdir(remote_dir, exist_ok=True)

        for file in record.get_input_files():
            src_path = file.src
            dst_path = file.dst or src_path
            real_src_path = os.path.join(self.dir, src_path)
            real_dst_path = os.path.join(remote_dir, dst_path)
            await self._executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await self._executor.connector.put(real_src_path, real_dst_path)

        for file_data in input_data.files:
            real_dst_path = os.path.join(remote_dir, file_data.name)
            await self._executor.connector.mkdir(os.path.dirname(real_dst_path), exist_ok=True)
            await self._executor.connector.dump_text(file_data.content, real_dst_path)

        record.save_input(input_data)
        record.save()
        record.activate()
        logger.info(
            "Prepared %s for spec %s with %d copied files and %d generated files",
            record,
            self.name,
            len(record.get_input_files()),
            len(input_data.files),
        )

    async def _submit_run(
        self, run: Task | Worker, target_set: Set[str],
        env: Optional[dict[str, str]] = None,
    ) -> None:
        try:
            logger.info("Submitting %s for spec %s via runner", run, self.name)
            await self._executor.runner.submit(run, env=env)
            if not run.is_terminated():
                target_set.add(run.id)
                logger.info("Tracking %s for spec %s", run, self.name)
            else:
                run.deactivate()
                logger.info("%s for spec %s terminated immediately", run, self.name)
        except Exception:
            run.set_state(TaskState.ERROR)
            run.deactivate()
            logger.exception("Failed to submit %s for spec %s", run, self.name)
            raise
        finally:
            run.touch()
            run.save()

    async def submit_to_queue(self, task_id: str) -> None:
        task = self.get_task(task_id)
        task.enqueue()
        now = time.time()
        self._queued_at[task_id] = now
        self._enqueue_events.append(now)
        await self._queue.put(task_id)
        logger.info(
            "%s submitted to queue for spec %s (queue_size=%d)",
            task,
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

        task = self.get_task(task_id)
        task.dequeue()
        remote_task_dir = os.path.join(self._executor.connector.get_base_dir(), task.get_prefix())
        logger.info(
            "Dequeued %s for spec %s (remaining_queue_size=%d)",
            task,
            self.name,
            self._queue.qsize(),
        )
        return {
            "id": task_id,
            "full_task_dir": await self._executor.connector.get_abs_path(remote_task_dir),
        }

    async def complete_task(self, task_id: str, state: str) -> None:
        task = self.get_task(task_id)
        task.set_state(TaskState.SUCCEEDED if state == "ok" else TaskState.FAILED)
        task.touch()
        task.save()
        task.deactivate()
        self._signal_task_done(task_id)
        logger.info(
            "%s in spec %s completed with reported state %s",
            task,
            self.name,
            state,
        )

    def _signal_task_done(self, task_id: str) -> None:
        event = self._task_done_events.pop(task_id, None)
        if event is not None:
            event.set()

    async def wait_for_terminated(self, task_id: str, wait_s: float) -> None:
        task = self.get_task(task_id)
        if task.is_terminated():
            return

        event = self._task_done_events.get(task_id)
        if event is None:
            return
        try:
            await asyncio.wait_for(asyncio.shield(event.wait()), timeout=wait_s)
        except asyncio.TimeoutError:
            pass

    def get_task(self, task_id: str) -> Task:
        return self._load_record(task_id, Task, self._tasks)

    def get_worker(self, worker_id: str) -> Worker:
        return self._load_record(worker_id, Worker, self._workers)

    def _load_record(
        self, record_id: str, cls: type[Record], cache: WeakValueDictionary[str, Record],
    ) -> Record:
        record = cache.get(record_id)
        if record is not None:
            return record
        assert len(record_id) > 2, "record_id should >2"
        data_file = os.path.join(
            self.dir, cls.subdir, record_id[:2], record_id[2:],
            self._spec_data.meta_dir, "task.json",
        )
        if not os.path.exists(data_file):
            raise FileNotFoundError(f"{cls.__name__} data file not found: {data_file}")
        with open(data_file, "r", encoding="utf-8") as handle:
            data = TaskData(**json.load(handle))
        record = cls(data, self._spec_data, spec_dir=self.dir)
        cache[record_id] = record
        return record

    async def get_task_async(
        self,
        task_id: str,
        wait: float = 0,
    ) -> Task:
        if wait > 0:
            await self.wait_for_terminated(task_id, wait_s=wait)
        return self.get_task(task_id)

    def get_task_input(self, task_id: str) -> TaskInput:
        return self.get_task(task_id).get_input()

    async def get_task_full_dir(self, task_id: str) -> str:
        task = self.get_task(task_id)
        remote_task_dir = os.path.join(self._executor.connector.get_base_dir(), task.get_prefix())
        return await self._executor.connector.get_abs_path(remote_task_dir)

    async def get_task_file(
        self,
        task_id: str,
        file_path: str,
        offset: int = 0,
    ) -> AsyncGenerator[bytes, Any]:
        task = self.get_task(task_id)
        file_path = os.path.normpath(file_path)
        remote_task_dir = os.path.join(self._executor.connector.get_base_dir(), task.get_prefix())
        remote_file_path = os.path.normpath(os.path.join(remote_task_dir, file_path))
        if not remote_file_path.startswith(os.path.normpath(remote_task_dir)):
            raise ValueError(f"Illegal file path: {file_path}")
        return await self._executor.connector.get_fstream(remote_file_path, offset=offset)

    def _load_active_tasks(self) -> None:
        self._scan_active_items(Task.subdir, self._active_tasks, self.get_task)
        for task_id in self._active_tasks:
            self._task_done_events[task_id] = asyncio.Event()
        self._scan_active_items(Worker.subdir, self._active_workers, self.get_worker)

    def _scan_active_items(
        self, sub_dir: str, target_set: Set[str],
        load: Callable[[str], Task | Worker],
    ) -> None:
        pattern = os.path.join(self.dir, sub_dir, "??", "*", self._spec_data.meta_dir, "active")
        for active_file_path in glob.glob(pattern):
            if not os.path.isfile(active_file_path):
                continue
            parts = active_file_path.split(os.sep)
            record_id = parts[-4] + parts[-3]
            try:
                record = load(record_id)
                if not record.is_terminated():
                    target_set.add(record_id)
                else:
                    fdel(active_file_path)
            except Exception as exc:
                logger.warning("Failed to load %s %s: %s", sub_dir, record_id, exc)
