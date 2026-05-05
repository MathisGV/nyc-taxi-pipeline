import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WeatherConfig:
    openweather_api_key: str
    openweather_city: str
    openweather_units: str
    weather_raw_path: Path
    weather_poll_interval_seconds: int


def load_weather_config() -> WeatherConfig:
    return WeatherConfig(
        openweather_api_key=os.getenv("OPENWEATHER_API_KEY", "").strip(),
        openweather_city=os.getenv("OPENWEATHER_CITY", "New York").strip(),
        openweather_units=os.getenv("OPENWEATHER_UNITS", "metric").strip(),
        weather_raw_path=Path(
            os.getenv("WEATHER_RAW_PATH", "data_lake/weather/raw").strip()
        ),
        weather_poll_interval_seconds=int(
            os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "3600").strip()
        ),
    )


def validate_weather_config(config: WeatherConfig) -> None:
    if not config.openweather_api_key:
        raise ValueError("OPENWEATHER_API_KEY is required")
    if config.weather_poll_interval_seconds <= 0:
        raise ValueError("WEATHER_POLL_INTERVAL_SECONDS must be > 0")
