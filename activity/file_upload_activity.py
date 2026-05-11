import boto3
import logging
import os
from temporalio import activity
from typing import Dict
from exceptions import FileUploadError
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

@activity.defn
def upload_file_to_s3(data: Dict) -> None:
    original = data.get("original_file_path")
    enriched = data.get("enriched_file_path")
    s3_key = data.get("s3_key")
    put_amount_on_hold_workflow = data.get("put_amount_on_hold_workflow", None)

    if s3_key and put_amount_on_hold_workflow:
        new_s3_key = s3_key.replace("telemetry-amount-hold", "freezed-amount-on-account")
    else:
        new_s3_key = s3_key.replace("telemetry", "cif-codes")

    try:
        load_dotenv(override=True)
        bucket = os.getenv("S3_BUCKET")
        s3 = boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )
        base_dir = os.getcwd()
        temp_file_path = os.path.join(base_dir, enriched)
        s3.upload_file(
            temp_file_path,
            bucket,
            new_s3_key,
            ExtraArgs={"ContentType": "text/csv"}
        )
        logger.info(f"File uploaded to s3://{bucket}/{new_s3_key}")
    except Exception as exc:
        logger.exception("File upload failed")
        raise FileUploadError(str(exc)) from exc