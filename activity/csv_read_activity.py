import logging
import pandas as pd
from temporalio import activity
from typing import List, Dict
from exceptions import CsvReadError

logger = logging.getLogger(__name__)


@activity.defn
def read_csv(file_path: str) -> List[Dict]:
    logger.info("Reading CSV file", extra={"file_path": file_path})

    try:
        df = pd.read_csv(file_path)

        required_columns = {"first_name", "last_name", "dob", "address"}
        missing = required_columns - set(df.columns)

        if missing:
            raise CsvReadError(f"Missing required columns: {missing}")

        records = df.to_dict(orient="records")
        logger.info(
            "CSV read successfully",
            extra={"record_count": len(records)},
        )
        return records

    except CsvReadError:
        logger.exception("CSV validation error")
        raise
    except Exception as exc:
        logger.exception("Unexpected CSV read error")
        raise CsvReadError(str(exc)) from exc


@activity.defn
def read_amount_on_hold_csv(file_path: str) -> List[Dict]:
    logger.info("Reading CSV file", extra={"file_path": file_path})

    try:
        df = pd.read_csv(file_path)

        required_columns = {"first_name", "last_name", "dob", "address", "cif_code", "comments", "hold_amount"}
        missing = required_columns - set(df.columns)

        if missing:
            raise CsvReadError(f"Missing required columns: {missing}")

        records = df.to_dict(orient="records")
        logger.info(
            "CSV read successfully",
            extra={"record_count": len(records)},
        )
        return records

    except CsvReadError:
        logger.exception("CSV validation error")
        raise
    except Exception as exc:
        logger.exception("Unexpected CSV read error")
        raise CsvReadError(str(exc)) from exc
