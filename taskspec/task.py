import json
import os
import time
from typing import Callable, List, Optional

from .schema import InFile, SpecData, TaskData, TaskInput, TaskKind, TaskState
from .util import fdel, fset


class Task:
    def __init__(
        self,
        task_data: TaskData,
        spec_data: SpecData,
        kind: TaskKind,
        *,
        spec_dir: str,
        public_url: str,
        queue_token_provider: Callable[[], str],
        save_task_data: Callable[["Task"], None],
        save_task_input: Callable[["Task", TaskInput], None],
        load_task_input: Callable[["Task"], TaskInput],
    ):
        self.task_data = task_data
        self.spec_data = spec_data
        self.kind = TaskKind(kind)
        self._spec_dir = spec_dir
        self._public_url = public_url
        self._queue_token_provider = queue_token_provider
        self._save_task_data = save_task_data
        self._save_task_input = save_task_input
        self._load_task_input = load_task_input

    @property
    def id(self) -> str:
        return self.task_data.id

    @property
    def state(self) -> TaskState:
        return self.task_data.state

    @property
    def created_at(self) -> int:
        return self.task_data.created_at

    @property
    def updated_at(self) -> int:
        return self.task_data.updated_at

    @property
    def slurm_job(self):
        return self.task_data.slurm_job

    def get_kind_name(self) -> str:
        return "worker" if self.kind == TaskKind.WORKER else "task"

    def get_subdir(self) -> str:
        return "workers" if self.kind == TaskKind.WORKER else "tasks"

    def get_task_dir(self) -> str:
        assert len(self.id) > 2, "task_id should >2"
        return os.path.normpath(
            os.path.join(self._spec_dir, self.get_subdir(), self.id[:2], self.id[2:])
        )

    def get_meta_dir(self) -> str:
        return os.path.join(self.get_task_dir(), self.spec_data.meta_dir)

    def get_prefix(self) -> str:
        return f"specs/{self.spec_data.name}/{self.get_subdir()}/{self.id[:2]}/{self.id[2:]}"

    def get_data_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "task.json")

    def get_input_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "input.json")

    def get_active_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "active")

    def get_qstate_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "qstate")

    def get_input_files(self) -> List[InFile]:
        if self.kind == TaskKind.WORKER and self.spec_data.worker_pool:
            return self.spec_data.worker_pool.files
        return self.spec_data.files

    def get_entrypoint(self) -> str:
        if self.kind == TaskKind.WORKER:
            if not self.spec_data.worker_pool:
                raise ValueError("Worker task requires worker_pool spec")
            return self.spec_data.worker_pool.entrypoint
        if self.spec_data.on_demand:
            return self.spec_data.on_demand.entrypoint
        if self.spec_data.worker_pool:
            return self.spec_data.worker_pool.entrypoint
        raise ValueError("Spec must have either on_demand or worker_pool")

    def get_remote_env(self) -> Optional[dict[str, str]]:
        if self.kind != TaskKind.WORKER:
            return None
        return {
            "__TASK_QUEUE_URL": f"{self._public_url}/specs/{self.spec_data.name}/queue/",
            "__TASK_QUEUE_TOKEN": self._queue_token_provider(),
        }

    def activate(self) -> None:
        fset(self.get_active_file())

    def deactivate(self) -> None:
        fdel(self.get_active_file())

    def enqueue(self) -> None:
        fset(self.get_qstate_file(), "enqueue")

    def dequeue(self) -> None:
        fset(self.get_qstate_file(), "dequeue")

    def get_input(self) -> TaskInput:
        return self._load_task_input(self)

    def save_input(self, task_input: TaskInput) -> None:
        self._save_task_input(self, task_input)

    def set_state(self, state: TaskState) -> None:
        self.task_data.state = state

    def set_slurm_job(self, slurm_job) -> None:
        self.task_data.slurm_job = slurm_job

    def touch(self, updated_at: Optional[int] = None) -> None:
        self.task_data.updated_at = int(time.time()) if updated_at is None else updated_at

    def save(self) -> None:
        self._save_task_data(self)

    def to_json(self) -> str:
        return json.dumps(self.task_data.model_dump())
