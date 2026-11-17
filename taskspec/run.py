from typing import Optional, Protocol

from .schema import SlurmJobData, TaskState


class Run(Protocol):
    """Current execution target, not a persistent execution-history entity.

    A task can have at most one active execution; its existing metadata holds
    the current execution state. No separate run identity is needed here.
    """

    def get_prefix(self) -> str: ...
    def get_entrypoint(self) -> str: ...
    def get_state_file(self) -> str: ...

    @property
    def slurm_job(self) -> Optional[SlurmJobData]: ...

    def set_slurm_job(self, slurm_job: SlurmJobData) -> None: ...
    def set_state(self, state: TaskState) -> None: ...
