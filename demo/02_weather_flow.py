import requests
import json
from prefect import flow, task

@task
def fetch_weather(city: str) -> str:
    url = f"https://wttr.in/{city}?format=j1"
    data = requests.get(url).json()
    return data["current_condition"][0]["temp_C"]

@task
def save_to_file(weather_data: list, filename: str) -> None:
    with open(filename, "w") as f:
        json.dump(weather_data, f)

@flow
def weather_collector() -> None:
    cities = ["Sofia", "London", "New York"]
    weather_data = []
    for city in cities:
        weather_data.append({"city": city, "temperature": fetch_weather(city) })
    save_to_file(weather_data, "01_weather_data.json")

if __name__ == "__main__":
    weather_collector()
    print("Press any key to exit...")
    input()