from __future__ import annotations

import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")

import time
from datetime import timedelta

from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash



@task(
    task_run_name="expensive-computation({x})",
    persist_result=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def expensive_step(x: int) -> int:
    time.sleep(2)
    return x * 2


@flow(name="caching-demo", log_prints=True)
def caching_demo() -> dict[str, int]:
    logger = get_run_logger()
    logger.info("Calling the same task twice with identical inputs.")

    a1 = expensive_step(21)
    a2 = expensive_step(21)  # should be a cache hit

    logger.info("Calling with a different input (cache miss).")
    b = expensive_step(22)

    return {"first": a1, "second": a2, "different_input": b}


if __name__ == "__main__":
    caching_demo.serve(name="caching-demo", tags=["demo", "caching"])

