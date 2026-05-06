import os
import sys
import argparse
from pathlib import Path

import requests
from minio.error import S3Error
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import get_minio_client, ensure_bucket, get_logger

load_dotenv()

logger = get_logger(__name__)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MINIO_BUCKET = "taxi-raw"
LOCAL_TMP_DIR = Path("/tmp/taxi")


def build_file_list(start_year: int, start_month: int, end_year: int, end_month: int) -> list[dict]:
    files = []
    year, month = start_year, start_month

    while (year, month) <= (end_year, end_month):
        period = f"{year}-{month:02d}"
        files.append({
            "period": period,
            "filename": f"yellow_tripdata_{period}.parquet",
            "url": f"{TLC_BASE_URL}/yellow_tripdata_{period}.parquet",
            "object_path": f"yellow_taxi/{year}/{month:02d}/yellow_tripdata_{period}.parquet"
        })
        month += 1
        if month > 12:
            month = 1
            year += 1

    return files


def download_file(url: str, dest: Path) -> Path:
    logger.info(f"Downloading {url}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    logger.info(f"Saved to {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def upload_to_minio(client, local_path: Path, object_path: str) -> None:
    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_path,
        file_path=str(local_path),
        content_type="application/octet-stream"
    )
    logger.info(f"Uploaded: {object_path}")


def object_exists(client, object_path: str) -> bool:
    try:
        client.stat_object(MINIO_BUCKET, object_path)
        return True
    except S3Error:
        return False


def ingest(start_year: int, start_month: int, end_year: int, end_month: int, force: bool = False) -> None:
    client = get_minio_client()
    ensure_bucket(client, MINIO_BUCKET)
    LOCAL_TMP_DIR.mkdir(parents=True, exist_ok=True)

    files = build_file_list(start_year, start_month, end_year, end_month)
    logger.info(f"Files to ingest: {len(files)}")

    success, skipped, failed = 0, 0, 0

    for file in files:
        local_path = LOCAL_TMP_DIR / file["filename"]

        if not force and object_exists(client, file["object_path"]):
            logger.info(f"Already exists, skipping: {file['object_path']}")
            skipped += 1
            continue

        try:
            download_file(file["url"], local_path)
            upload_to_minio(client, local_path, file["object_path"])
            local_path.unlink()
            success += 1
        except requests.HTTPError as e:
            logger.error(f"HTTP error for {file['period']}: {e}")
            failed += 1
        except S3Error as e:
            logger.error(f"MinIO error for {file['period']}: {e}")
            failed += 1
        except Exception as e:
            logger.error(f"Unexpected error for {file['period']}: {e}")
            failed += 1

    logger.info(f"Ingestion complete - success: {success}, skipped: {skipped}, failed: {failed}")

    if failed > 0:
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest NYC Yellow Taxi data into MinIO")
    parser.add_argument("--start-year", type=int, default=2025)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-year", type=int, default=2026)
    parser.add_argument("--end-month", type=int, default=2)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        force=args.force
    )