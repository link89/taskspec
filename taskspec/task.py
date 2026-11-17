import os
from typing import List

from .schema import InFile, TaskData
from .storage import FileRecord
from .util import fset


class Task(FileRecord):
    subdir = "tasks"

    @property
    def task_data(self) -> TaskData:
        return self.data

    def get_input_files(self) -> List[InFile]:
        return self.spec_data.files

    def get_entrypoint(self) -> str:
        if not self.spec_data.on_demand:
            raise ValueError("Worker-pool tasks must be queued, not submitted to a Runner")
        return self.spec_data.on_demand.entrypoint

    def get_qstate_file(self) -> str:
        return os.path.join(self.get_meta_dir(), "qstate")

    def enqueue(self) -> None:
        fset(self.get_qstate_file(), "enqueue")

    def dequeue(self) -> None:
        fset(self.get_qstate_file(), "dequeue")
