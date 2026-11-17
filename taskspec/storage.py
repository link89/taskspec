import json
import os
import time
from typing import Optional

from .schema import SpecData, TaskData, TaskInput, TaskState, SlurmJobData
from .util import fdel, fset


class FileRecord:
    """Shared storage layout, retaining the legacy task.json format for workers."""

    subdir: str

    def __init__(
        self,
        data: TaskData,
        spec_data: SpecData,
        *,
        spec_dir: str,
    ):
        self.data = data
        self.spec_data = spec_data
        self._spec_dir = spec_dir

    @property
    def id(self) -> str:
        return self.data.id

    @property
    def state(self) -> TaskState:
        return self.data.state

    @property
    def created_at(self) -> int:
        return self.data.created_at

    @property
    def updated_at(self) -> int:
        return self.data.updated_at

    @property
    def slurm_job(self) -> Optional[SlurmJobData]:
        return self.data.slurm_job

    def get_dir(self) -> str:
        assert len(self.id) > 2, "task_id should >2"
        return os.path.normpath(
            os.path.join(self._spec_dir, self.subdir, self.id[:2], self.id[2:])
        )

    def get_meta_dir(self) -> str:
        return os.path.join(self.get_dir(), self.spec_data.meta_dir)

    def get_prefix(self) -> str:
        return f"specs/{self.spec_data.name}/{self.subdir}/{self.id[:2]}/{self.id[2:]}"

    def get_data_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "task.json")

    def get_input_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "input.json")

    def get_active_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "active")

    def get_state_file(self) -> str:
        return os.path.join(self.get_prefix(), self.spec_data.state_file)

    def activate(self) -> None:
        fset(self.get_active_file())

    def deactivate(self) -> None:
        fdel(self.get_active_file())

    def get_input(self) -> TaskInput:
        task_input_file = self.get_input_file()
        if not os.path.exists(task_input_file):
            raise FileNotFoundError(f"Task input file not found: {task_input_file}")
        with open(task_input_file, "r", encoding="utf-8") as handle:
            return TaskInput(**json.load(handle))

    def save_input(self, task_input: TaskInput) -> None:
        task_input_file = self.get_input_file()
        os.makedirs(os.path.dirname(task_input_file), exist_ok=True)
        with open(task_input_file, "w", encoding="utf-8") as handle:
            json.dump(task_input.model_dump(), handle)

    def set_state(self, state: TaskState) -> None:
        self.data.state = state

    def set_slurm_job(self, slurm_job: SlurmJobData) -> None:
        self.data.slurm_job = slurm_job

    def is_terminated(self) -> bool:
        return TaskState.is_terminated(self.state)

    def touch(self, updated_at: Optional[int] = None) -> None:
        self.data.updated_at = int(time.time()) if updated_at is None else updated_at

    def save(self) -> None:
        task_data_file = self.get_data_file()
        os.makedirs(os.path.dirname(task_data_file), exist_ok=True)
        with open(task_data_file, "w", encoding="utf-8") as handle:
            json.dump(self.data.model_dump(), handle)

    def to_json(self) -> str:
        return json.dumps(self.data.model_dump())

    def __str__(self) -> str:
        return f"{type(self).__name__}(id={self.id}, state={self.state.name})"
