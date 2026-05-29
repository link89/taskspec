# Change Log

## v0.0.7
- refine logs

## v0.0.6
- refine logs

## v0.0.5
- bugfix: wrong task state of slurm job
- bugfix: return 404 if file is not found

## v0.0.4
- refine get_task_file
- provide constants endpoint

## v0.0.3
- support elastic scale up
- support read file with offset

## v0.0.2
- add command line entry

## v0.0.1

### Features
- **Execution Systems**: Support for "On-Demand" and "Worker Pool" task execution modes.
- **Task Management**: Lifecycle tracking with IDLE, SUBMITTED, SUCCEEDED, FAILED, and ERROR states.
- **Connectors**:
    - **Local**: Execute tasks on the local filesystem.
    - **SSH**: Remote execution and file management via SSH/SFTP.
- **Slurm Integration**: Built-in support for submitting and monitoring jobs on Slurm clusters.
- **Idempotency**: Support for `idempotent_key` to ensure safe, repeated task submissions.
- **API**: FastAPI-based service providing endpoints for task management, file access, and worker pool queues.
- **Security**: Bearer token authentication for API access control.
- **CLI Tool**: Command-line interface for server management and auth configuration.
- **Configuration**: YAML-based specification for executors, runners, and task environments.
