# Change Log

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
