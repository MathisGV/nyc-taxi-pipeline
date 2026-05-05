from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.ingestion.weather.config import WeatherConfig


def persist_weather_snapshot(
    payload: dict[str, Any], config: WeatherConfig
) -> Path:
    now = datetime.now(timezone.utc)
    output_dir = (
        config.weather_raw_path
        / f"year={now:%Y}"
        / f"month={now:%m}"
        / f"day={now:%d}"
        / f"hour={now:%H}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"weather_{now:%Y%m%dT%H%M%SZ}.json"

    enriched_payload = {
        "ingested_at": now.isoformat(),
        "source": "openweathermap_current_weather",
        "city": config.openweather_city,
        "data": payload,
    }
    output_file.write_text(
        json.dumps(enriched_payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    return output_file
