# TaskSpec

TaskSpec is a database-less, general-purpose asynchronous task scheduler.

It is designed for workloads that need to be submitted, tracked, and resumed through an API, with a strong focus on running tasks on HPC clusters such as Slurm-backed environments. Task state lives in the task directory and its spec files, so you can operate without a central database while still exposing a simple programmatic interface.

Typical use cases include:

- API-driven task submission and status checks
- HPC batch execution on Slurm or similar job systems
- Lightweight deployments that avoid database operations and extra infrastructure


## Installation

```bash
pip install taskspec
```

## Getting Started

```
taskspec start ./demo
```

For more examples and configuration details, see the demo workspace under `demo/`.