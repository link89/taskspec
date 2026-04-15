# TaskSpec Project Overview

TaskSpec is a task execution service designed to manage, submit, and track computational tasks across different executors (e.g., Slurm via SSH). It provides a structured way to define task specifications and handle large volumes of task data efficiently.

## Main Functions

### 1. Hierarchical Service Architecture
The system is organized into a two-level service hierarchy:
- **RootService**: Discovers and manages multiple `SpecService` instances by scanning the `specs/` directory.
- **SpecService**: Manages the lifecycle of tasks for a specific `TaskSpec`, including task creation, submission, and background state polling.

### 2. Task Specification & Execution
- **Spec Definition**: Uses `config.yml` within `specs/{spec_name}/` to define executors, entrypoints, and required files.
- **Remote Execution**: Supports submitting jobs to remote HPC clusters using SSH connectors and Slurm runners.
- **Background Polling**: The service automatically polls the status of unfinished tasks at a configurable interval (`poll_interval_s`, default 5s) and updates their state on disk.

### 3. Optimized Task Storage & Metadata
- **Sharded Directory Structure**: Tasks are stored locally using a nested directory format (e.g., `tasks/ab/cdef...`) based on the first two characters of the `task_id`. 
- **Metadata Separation**: To minimize write amplification, task data is split into:
  - `{meta_dir}/input.json`: Static input data (`TaskInput`).
  - `{meta_dir}/task.json`: Mutable task state and Slurm job info (`TaskData`).
  - The metadata directory is configurable via `meta_dir` (defaulting to `.meta`).
- **Updated Timestamps**: Each `TaskData` record includes an `updated_at` field to track the latest state change.

### 4. Authentication & Security
- **Bearer Token Auth**: All protected API routes require an `Authorization: Bearer {key}:{secret}` header.
- **Credential Management**: Use `python -m taskspec.cli add_auth_key` to manage keys and secrets. Secrets are stored securely using salted SHA256 hashes in `auth.jsonl`.
- **Public Health Check**: A `/health` endpoint is available for monitoring server readiness without authentication.

### 5. REST API
Provides a FastAPI-based interface for:
- **Health Check**: `GET /health` (Public).
- **Submit Task**: `POST /specs/{spec_name}/tasks`.
- **Query Task Data**: `GET /specs/{spec_name}/tasks/{task_id}`.
- **Query Task Input**: `GET /specs/{spec_name}/tasks/{task_id}/input`.
- **Retrieve Files**: `GET /specs/{spec_name}/tasks/{task_id}/files/{path}`.

## How to Run Tests

The project includes an End-to-End (E2E) test suite that validates the entire flow from server startup to job completion.

### Running E2E Tests
To ensure a clean environment (unsetting proxy variables that might interfere with local communication), use the provided test runner:

```bash
./run-test.sh
```

The script unsets `http_proxy`, `https_proxy`, etc., and executes `pytest -s tests/test_e2e.py`.
