import logging
import os
from temporalio import activity
from exceptions import FileValidationError
from fastapi import UploadFile

logger = logging.getLogger(__name__)


@activity.defn
def validate_csv_file(file_path: str) -> None:
    logger.info("Validating file", extra={"file_path": file_path})

    try:
        if not file_path:
            raise FileValidationError(f"File not found: {file_path}")

        if not file_path.lower().endswith(".csv"):
            raise FileValidationError("Only CSV files are supported")

    except Exception:
        logger.exception("File validation failed")
        raise

    logger.info("File validation successful")
