# TaskSpec Project Overview

TaskSpec is a task execution service designed to manage, submit, and track computational tasks across different executors (e.g., Slurm via SSH). It provides a structured way to define task specifications and handle large volumes of task data efficiently.


## How to Run Tests

The project includes an End-to-End (E2E) test suite that validates the entire flow from server startup to job completion.

### Running E2E Tests
To ensure a clean environment (unsetting proxy variables that might interfere with local communication), use the provided test runner:

```bash
./run-test.sh
```
