from __future__ import annotations

import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")

from datetime import datetime, timezone

from prefect import flow, task
from prefect.artifacts import (
    create_link_artifact,
    create_markdown_artifact,
    create_table_artifact,
)



@task
def build_report() -> dict:
    now = datetime.now(timezone.utc)
    return {
        "generated_at": now.isoformat(),
        "items": [
            {"name": "pipelines", "status": "ok"},
            {"name": "alerts", "status": "ok"},
            {"name": "latency", "status": "warn"},
        ],
    }


@task
def publish_artifacts(report: dict) -> None:
    create_markdown_artifact(
        key="demo-markdown-report",
        description="Markdown report (demo)",
        markdown=(
            "# Demo report\n\n"
            f"Generated at: `{report['generated_at']}`\n\n"
            "## Summary\n"
            "- pipelines: ok\n"
            "- alerts: ok\n"
            "- latency: warn\n"
        ),
    )

    create_table_artifact(
        key="demo-table-report",
        description="Table report (demo)",
        table=report["items"],
    )

    create_link_artifact(
        key="demo-link",
        description="Link artifact (demo)",
        link="https://docs.prefect.io/v3/",
        link_text="Prefect v3 docs",
    )


@flow(name="artifacts-demo", log_prints=True)
def artifacts_demo() -> None:
    report = build_report()
    publish_artifacts(report)


if __name__ == "__main__":
    artifacts_demo.serve(name="artifacts-demo", tags=["demo", "artifacts"])

