from typing import List

from .schema import InFile
from .storage import FileRecord


class Worker(FileRecord):
    subdir = "workers"

    def get_input_files(self) -> List[InFile]:
        if not self.spec_data.worker_pool:
            raise ValueError("Worker requires a worker_pool spec")
        return self.spec_data.worker_pool.files

    def get_entrypoint(self) -> str:
        if not self.spec_data.worker_pool:
            raise ValueError("Worker requires a worker_pool spec")
        return self.spec_data.worker_pool.entrypoint
