from __future__ import annotations

import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")
import random
from datetime import datetime, timezone

from prefect import flow, get_run_logger, task
from prefect.tasks import exponential_backoff



@task(
    task_run_name="unstable-step",
    retries=4,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
)
def sometimes_fails(fail_probability: float = 0.7) -> float:
    roll = random.random()
    if roll < fail_probability:
        raise RuntimeError(f"Transient failure (roll={roll:.3f} < p={fail_probability:.2f})")
    return roll


@flow(name="retries-demo", log_prints=True)
def retries_demo(fail_probability: float = 0.7) -> float:
    logger = get_run_logger()
    logger.info("Starting retries demo at %s", datetime.now(timezone.utc).isoformat())
    return sometimes_fails(fail_probability=fail_probability)


if __name__ == "__main__":
    retries_demo.serve(
        name="retries-demo",
        tags=["demo", "retries"],
        parameters={"fail_probability": 0.8},
    )

