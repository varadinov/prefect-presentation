from __future__ import annotations

import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")
from datetime import datetime, timedelta, timezone

from prefect import flow, get_run_logger
from prefect.schedules import Cron, Interval



@flow(name="schedules-demo", log_prints=True)
def scheduled_flow(message: str = "Hello from a scheduled Prefect run!") -> None:
    logger = get_run_logger()
    logger.info("message=%s", message)
    print(f"[{datetime.now(timezone.utc).isoformat()}] {message}")


if __name__ == "__main__":
    scheduled_flow.serve(
        name="schedules-demo",
        tags=["demo", "schedules"],
        schedules=[
            Interval(
                timedelta(minutes=1),
                anchor_date=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
                timezone="UTC",
                slug="every-1-min",
                parameters={"message": "Interval schedule (every 1 minutes)"},
            ),
            Cron(
                "*/2 * * * *",
                timezone="UTC",
                slug="every-2-min",
                parameters={"message": "Cron schedule (every 2 minutes)"},
            ),
        ],
        parameters={"message": "Default parameters (manual run)"},
    )
