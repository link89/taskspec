# TaskSpec Project Overview

TaskSpec is a task execution service designed to manage, submit, and track computational tasks across different executors (e.g., Slurm via SSH). It provides a structured way to define task specifications and handle large volumes of task data efficiently.

## Main Functions

### 1. Hierarchical Service Architecture
The system is organized into a two-level service hierarchy:
- **RootService**: Discovers and manages multiple `SpecService` instances by scanning the `specs/` directory.
- **SpecService**: Manages the lifecycle of tasks for a specific `TaskSpec`, including task creation, submission, and state tracking.

### 2. Task Specification & Execution
- **Spec Definition**: Uses `config.yml` within `specs/{spec_name}/` to define executors, entrypoints, and required files.
- **Remote Execution**: Supports submitting jobs to remote HPC clusters using SSH connectors and Slurm runners.
- **File Management**: Automatically handles uploading spec-defined files and task-specific input files to the executor.

### 3. Optimized Task Storage
- **Sharded Directory Structure**: Tasks are stored locally using a nested directory format (e.g., `tasks/ab/cdef...`) based on the first two characters of the `task_id`. This prevents filesystem performance degradation when managing thousands of tasks.
- **Configurable Metadata**: Task metadata is stored within each task directory, with the filename configurable via the `task_file` attribute (defaulting to `.task.json`).

### 4. Advanced Task ID Generation
- **Idempotency**: Supports generating deterministic task IDs using MD5 hashing of an `idempotent_key`.
- **Encoding**: All IDs are encoded using Base32 (without padding) for URL-friendly and compact representation (e.g., `YQQ2G3HLGNBK5FOSNIJJZUZP6Q`).

### 5. REST API
Provides a FastAPI-based interface for:
- Submitting new tasks (`POST /specs/{spec_name}/tasks`).
- Querying task status and metadata (`GET /specs/{spec_name}/tasks/{task_id}`).
- Retrieving task output files (`GET /specs/{spec_name}/tasks/{task_id}/files/{path}`).

## How to Run Tests

The project includes an End-to-End (E2E) test suite that validates the entire flow from server startup to job completion on a simulated or real HPC environment.

### Prerequisites
- Ensure `pytest` and `requests` are installed.
- Configure SSH access to the executor (e.g., `demo-hpc`) if running against real hardware.

### Running E2E Tests
Execute the following command from the project root:

```bash
PYTHONPATH=. pytest tests/test_e2e.py -s
```

The `-s` flag is recommended to see the live polling logs and task ID information during the test execution.
