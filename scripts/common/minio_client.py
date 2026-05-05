import os
from minio import Minio
from dotenv import load_dotenv

load_dotenv()


def get_minio_client() -> Minio:
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )


def ensure_bucket(client: Minio, bucket: str) -> None:
    from minio.error import S3Error
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)