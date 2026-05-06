import argparse
import logging
import time

from scripts.ingestion.weather.config import load_weather_config, validate_weather_config
from scripts.ingestion.weather.openweather_client import (
    OpenWeatherClientError,
    fetch_current_weather,
)
from scripts.ingestion.weather.snapshot_writer import persist_weather_snapshot

LOGGER = logging.getLogger(__name__)


def run_once() -> None:
    config = load_weather_config()
    validate_weather_config(config)
    try:
        payload = fetch_current_weather(config)
    except OpenWeatherClientError as exc:
        raise SystemExit(f"Failed to fetch weather data: {exc}") from exc

    output_path = persist_weather_snapshot(payload, config)

    weather_main = "unknown"
    weather_list = payload.get("weather")
    if isinstance(weather_list, list) and weather_list:
        first_weather = weather_list[0]
        if isinstance(first_weather, dict):
            weather_main = str(first_weather.get("main", "unknown"))

    print(
        "weather payload fetched and saved "
        f"(city={config.openweather_city}, condition={weather_main}, "
        f"path={output_path})"
    )
    LOGGER.info(
        "weather snapshot persisted city=%s condition=%s path=%s",
        config.openweather_city,
        weather_main,
        output_path,
    )


def run_loop(interval_seconds: int, max_runs: int | None = None) -> None:
    LOGGER.info(
        "starting weather loop interval_seconds=%s max_runs=%s",
        interval_seconds,
        max_runs,
    )
    run_count = 0
    while True:
        try:
            run_once()
        except Exception as exc:
            LOGGER.exception("run_once failed: %s", exc)
            if max_runs is not None:
                raise
        run_count += 1
        if max_runs is not None and run_count >= max_runs:
            LOGGER.info("weather loop finished runs=%s", run_count)
            return
        time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch OpenWeather payload and persist timestamped snapshots."
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--once",
        action="store_true",
        help="Run one ingestion cycle and exit (default).",
    )
    mode_group.add_argument(
        "--loop",
        action="store_true",
        help="Run ingestion in an infinite loop.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Polling interval in seconds when --loop is used.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Maximum number of runs in loop mode. If omitted, loop is infinite.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = parse_args()
    config = load_weather_config()
    validate_weather_config(config)

    if args.loop:
        interval_seconds = (
            args.interval_seconds
            if args.interval_seconds is not None
            else config.weather_poll_interval_seconds
        )
        if interval_seconds <= 0:
            raise SystemExit("--interval-seconds must be > 0")
        if args.max_runs is not None and args.max_runs <= 0:
            raise SystemExit("--max-runs must be > 0")
        run_loop(interval_seconds, max_runs=args.max_runs)
        return

    run_once()


if __name__ == "__main__":
    main()
