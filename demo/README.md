# Demo scripts

## Setup

```bash
cd demo
uv sync
```

## Local Prefect server (recommended for `serve()` + schedules)

```bash
prefect server start
```

## Run demos

```bash
uv run demo/01_weather.py
uv run demo/02_weather_flow.py
uv run demo/03_weather_flow_names.py
uv run demo/04_weather_flow_logging.py
uv run demo/05_vm_create.py
uv run demo/06_transactions.py
uv run demo/07_dynamic_workflows.py
uv run demo/schedules.py
uv run demo/08_task_retries.py
uv run demo/09_task_caching.py
uv run demo/10_artifacts.py
```