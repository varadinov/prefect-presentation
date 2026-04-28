from datetime import datetime, timezone
import requests
import json
from prefect import flow, task
from prefect.logging import get_run_logger

@task(task_run_name="Fetch weather — {city}")
def fetch_weather(city: str) -> str:
    logger = get_run_logger()
    url = f"https://wttr.in/{city}?format=j1"
    logger.info(f"Fetching weather for {city} from {url}")
    result = requests.get(url)
    result.raise_for_status()
    data = result.json()
    logger.info(f"Weather data: {data['current_condition'][0]['temp_C']}")
    return data["current_condition"][0]["temp_C"]

@task(task_run_name="Save weather data — {filename}")
def save_to_file(weather_data: list, filename: str) -> None:
    logger = get_run_logger()
    logger.info(f"Saving weather data to {filename}")
    with open(filename, "w") as f:
        json.dump(weather_data, f)
    logger.info(f"Weather data saved to {filename}")

@flow(flow_run_name=lambda: f"weather-{datetime.now(timezone.utc):%Y-%m-%d-%H%M}")
def weather_collector() -> None:
    logger = get_run_logger()
    logger.info("Starting weather collector")
    cities = ["Sofia", "London", "New York"]
    weather_data = []
    for city in cities:
        weather_data.append({"city": city, "temperature": fetch_weather(city) })
    save_to_file(weather_data, "01_weather_data.json")
    logger.info("Weather collector completed")
    
if __name__ == "__main__":
    weather_collector()
    print("Press any key to exit...")
    input()