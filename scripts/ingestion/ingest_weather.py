import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

import requests
from minio.error import S3Error
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common import get_minio_client, ensure_bucket, get_logger

load_dotenv()

logger = get_logger(__name__)

OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
MINIO_BUCKET = "weather-raw"
LOCAL_TMP_DIR = Path("/tmp/weather")

NYC_COORDS = {
    "lat": 40.7128,
    "lon": -74.0060
}


def fetch_weather(api_key: str) -> dict:
    params = {
        "lat": NYC_COORDS["lat"],
        "lon": NYC_COORDS["lon"],
        "appid": api_key,
        "units": "metric"
    }
    response = requests.get(OPENWEATHER_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_locally(data: dict, timestamp: datetime) -> Path:
    filename = f"weather_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    path = LOCAL_TMP_DIR / timestamp.strftime("%Y/%m/%d") / filename
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Saved locally: {path}")
    return path


def upload_to_minio(client, local_path: Path, timestamp: datetime) -> str:
    object_path = f"nyc/{timestamp.strftime('%Y/%m/%d')}/{local_path.name}"
    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_path,
        file_path=str(local_path),
        content_type="application/json"
    )
    logger.info(f"Uploaded: {object_path}")
    return object_path


def collect_once(api_key: str, client) -> None:
    timestamp = datetime.now(timezone.utc)

    try:
        data = fetch_weather(api_key)
        data["_ingested_at"] = timestamp.isoformat()
        local_path = save_locally(data, timestamp)
        upload_to_minio(client, local_path, timestamp)
        local_path.unlink()
    except requests.HTTPError as e:
        logger.error(f"HTTP error fetching weather: {e}")
        raise
    except S3Error as e:
        logger.error(f"MinIO error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def run_loop(api_key: str, interval_seconds: int, max_iterations: int | None) -> None:
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)
    LOCAL_TMP_DIR.mkdir(parents=True, exist_ok=True)

    iterations = 0
    while True:
        logger.info(f"Collecting weather data (iteration {iterations + 1})")

        try:
            collect_once(api_key, client)
        except Exception:
            logger.warning("Collection failed, retrying next cycle")

        iterations += 1
        if max_iterations and iterations >= max_iterations:
            logger.info("Max iterations reached, stopping")
            break

        logger.info(f"Sleeping {interval_seconds}s until next collection")
        time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest NYC weather data into MinIO")
    parser.add_argument("--interval", type=int, default=3600)
    parser.add_argument("--max-iterations", type=int, default=None)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    api_key = os.getenv("OPENWEATHER_API_KEY")

    if not api_key:
        logger.error("OPENWEATHER_API_KEY is not set")
        sys.exit(1)

    if args.once:
        client = get_minio_client()
        ensure_bucket(client, MINIO_BUCKET)
        LOCAL_TMP_DIR.mkdir(parents=True, exist_ok=True)
        collect_once(api_key, client)
    else:
        run_loop(api_key, args.interval, args.max_iterations)