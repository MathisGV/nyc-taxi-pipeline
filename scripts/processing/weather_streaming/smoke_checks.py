from __future__ import annotations

import importlib
import os
from pathlib import Path


REQUIRED_ENV_VARS = [
    "OPENWEATHER_API_KEY",
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
]


def check_python_dependencies() -> None:
    for module_name in ("pyspark",):
        importlib.import_module(module_name)
    print("OK dependency check")


def check_environment() -> None:
    missing = [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Missing required env vars: {', '.join(missing)}")
    print("OK environment check")


def check_paths() -> None:
    input_path = Path(os.getenv("WEATHER_STREAM_INPUT_PATH", "data_lake/weather/raw"))
    checkpoint_path = Path(
        os.getenv(
            "WEATHER_STREAM_CHECKPOINT_PATH",
            "data_lake/weather/checkpoints/stream_weather",
        )
    )
    input_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    print(f"OK path check (input={input_path}, checkpoint={checkpoint_path})")


def main() -> None:
    check_python_dependencies()
    check_environment()
    check_paths()
    print("Weather streaming smoke checks passed")


if __name__ == "__main__":
    main()
