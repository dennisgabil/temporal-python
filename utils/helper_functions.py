import re
import csv
import io
import boto3
import logging
from dotenv import load_dotenv
from botocore.exceptions import BotoCoreError, ClientError

from fastapi import FastAPI, File, HTTPException, UploadFile, status
from typing import Optional
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Set
load_dotenv()

ALLOWED_EXTENSIONS: Set[str] = {".csv"}
S3_BUCKET = os.getenv("S3_BUCKET").strip()
AWS_REGION = os.getenv("AWS_REGION", "").strip() or None

def _has_allowed_extension(filename: str) -> bool:
    filename_lower = (filename or "").lower().strip()
    return any(filename_lower.endswith(ext) for ext in ALLOWED_EXTENSIONS)


def _sanitize_filename(name: str) -> str:
    name = name.strip().replace("\\", "/").split("/")[-1]  # drop any path
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name or "uploaded.csv"


async def _read_file_object(file: UploadFile) -> io.BytesIO:
    contents = await file.read()
    buffer = io.StringIO(contents.decode('utf-8'))
    reader = csv.DictReader(buffer)
    REQUIRED_COLUMNS = {"fname","lname","dob","address"}
    if not REQUIRED_COLUMNS.issubset(set(reader.fieldnames or [])):
        raise HTTPException(status_code=400, detail=f"Missing columns. Required: {REQUIRED_COLUMNS}")
    return contents


def _looks_like_csv(sample_text: str) -> bool:
    sample_text = sample_text.strip()
    if not sample_text:
        return False
    try:
        csv.Sniffer().sniff(sample_text[:4096])
        return True
    except Exception:
        return False


def _build_s3_key(filename: str, now: Optional[datetime] = None) -> str:
    """
    Required format:
    date/hours/<file_object>

    Example:
    2026-04-17/10/revenue.csv
    """
    now = datetime.now(ZoneInfo("Asia/Kolkata"))

    date_part = now.strftime("%Y-%m-%d")
    hour_part = now.strftime("%H")

    s3_key = f"{date_part}/{hour_part}/{filename}"

    safe_name = _sanitize_filename(filename)
    return f"{date_part}/{hour_part}/{safe_name}"


def _get_s3_client():
    if AWS_REGION:
        return boto3.client("s3", region_name=AWS_REGION)
    return boto3.client("s3")


def _upload_bytes_to_s3(bucket: str, key: str, data: bytes, content_type: str) -> None:
    """
    Upload using upload_fileobj with an in-memory stream (BytesIO).
    """
    s3 = _get_s3_client()
    buffer = io.BytesIO(data)
    buffer.seek(0)

    extra_args = {"ContentType": content_type or "text/csv"}

    try:
        s3.upload_fileobj(buffer, bucket, key, ExtraArgs=extra_args)
    except (ClientError, BotoCoreError) as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to upload to S3: {str(e)}",
        ) from e```File: utils/s3_utils.pyCode:```pythonimport boto3


def upload_bytes_to_s3(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    """
    Upload raw bytes to S3.
    AWS credentials/region should be provided via standard boto3 resolution:
    env vars, shared config, IAM role, etc.
    """
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )