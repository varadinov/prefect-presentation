from __future__ import annotations
import os
os.environ.setdefault("PREFECT_API_URL", "http://127.0.0.1:4200/api")


import json
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task, unmapped


@task
def load_weather_data(path: str | Path) -> Any:
    p = Path(path)
    return json.loads(p.read_text(encoding="utf-8"))


@task(task_run_name="forecast-{city}")
def forecast_for_city(city: str, data: dict[str, Any]) -> dict[str, Any]:
    """
    Pretend this is an API call; we use a local JSON file so the demo is stable.
    """
    logger = get_run_logger()
    temperature_c = data.get(city)
    if temperature_c is None:
        raise ValueError(f"No weather data found for city={city!r}")
    logger.info("Loaded forecast for %s", city)
    return {"city": city, "temperature_c": float(temperature_c)}


@task(task_run_name="alert-{city}")
def send_alert(city: str, temperature_c: float) -> None:
    logger = get_run_logger()
    logger.warning("ALERT: %s is cold (%.1f°C)!", city, temperature_c)


@flow(flow_run_name="notify-{city}")
def notify(city: str, temperature_c: float) -> None:
    send_alert(city, temperature_c)


@flow(name="07-dynamic-workflows")
def dynamic_workflows(
    *,
    threshold_c: float = 0.0,
    data_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """
    Dynamic workflow patterns shown here:
    - Dynamic inputs: cities are discovered at runtime from a JSON file.
    - Dynamic mapping: `forecast_for_city.map(cities, unmapped(data))`.
    - Dynamic branching/subflows: we create subflow runs only for the cold cities.
    """
    logger = get_run_logger()

    if data_path is None:
        data_path = Path(__file__).with_name("01_weather_data.json")

    raw_data = load_weather_data(data_path)
    # The repo's `01_weather_data.json` is a list of {"city": "...", "temperature": "..."} objects.
    # Normalize it into a dict[str, float] so we can map cleanly.
    if isinstance(raw_data, list):
        data: dict[str, float] = {row["city"]: float(row["temperature"]) for row in raw_data}
    elif isinstance(raw_data, dict):
        data = {k: float(v) for k, v in raw_data.items()}
    else:
        raise TypeError(f"Unexpected weather data format: {type(raw_data)!r}")

    cities = sorted(data.keys())
    logger.info("Discovered %d cities at runtime: %s", len(cities), ", ".join(cities))

    # Dynamic mapping across the runtime-discovered list of cities.
    forecasts = forecast_for_city.map(cities, unmapped(data))
    resolved_forecasts: list[dict[str, Any]] = (
        forecasts.result() if hasattr(forecasts, "result") else list(forecasts)
    )

    # Dynamic branching: spawn subflows only when needed.
    for fc in resolved_forecasts:
        temp_c = float(fc["temperature_c"])
        if temp_c < threshold_c:
            notify(fc["city"], temp_c)

    return resolved_forecasts


if __name__ == "__main__":
    dynamic_workflows.serve(
        name="dynamic-workflows",
        tags=["demo", "dynamic", "mapping"],
        parameters={"threshold_c": 5.0},
    )
