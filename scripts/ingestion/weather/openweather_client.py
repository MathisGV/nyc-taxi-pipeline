from __future__ import annotations

import time
from typing import Any

import requests

from scripts.ingestion.weather.config import WeatherConfig


class OpenWeatherClientError(Exception):
    """Raised when OpenWeather request fails after retries."""


def fetch_current_weather(
    config: WeatherConfig,
    timeout_seconds: int = 15,
    max_retries: int = 3,
    retry_delay_seconds: int = 2,
) -> dict[str, Any]:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": config.openweather_city,
        "appid": config.openweather_api_key,
        "units": config.openweather_units,
    }

    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout_seconds)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise OpenWeatherClientError("OpenWeather payload is not a JSON object")
            return payload
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(retry_delay_seconds)
                continue
            break

    raise OpenWeatherClientError(
        f"OpenWeather request failed after {max_retries} attempts"
    ) from last_error
