import requests
import json


def fetch_weather(city: str) -> dict:
    url = f"https://wttr.in/{city}?format=j1"
    data = requests.get(url).json()

    return data["current_condition"][0]["temp_C"]


def save_to_file(weather_data: list, filename: str) -> None:
    with open(filename, "w") as f:
        json.dump(weather_data, f)


def main() -> None:
    cities: list[str] = ["Sofia", "London", "New York"]

    weather_data = []
    for city in cities:
        weather_data.append({"city": city, "temperature": fetch_weather(city)})
    
    save_to_file(weather_data, "01_weather_data.json")


if __name__ == "__main__":
    main()