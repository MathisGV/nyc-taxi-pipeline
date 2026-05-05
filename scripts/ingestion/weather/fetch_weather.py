from scripts.ingestion.weather.config import load_weather_config, validate_weather_config
from scripts.ingestion.weather.openweather_client import (
    OpenWeatherClientError,
    fetch_current_weather,
)


def main() -> None:
    config = load_weather_config()
    validate_weather_config(config)
    try:
        payload = fetch_current_weather(config)
    except OpenWeatherClientError as exc:
        raise SystemExit(f"Failed to fetch weather data: {exc}") from exc

    weather_main = "unknown"
    weather_list = payload.get("weather")
    if isinstance(weather_list, list) and weather_list:
        first_weather = weather_list[0]
        if isinstance(first_weather, dict):
            weather_main = str(first_weather.get("main", "unknown"))

    print(
        "weather payload fetched "
        f"(city={config.openweather_city}, condition={weather_main})"
    )


if __name__ == "__main__":
    main()
