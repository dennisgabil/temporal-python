import logging
import pandas as pd
from temporalio import activity
from exceptions import CsvWriteError

logger = logging.getLogger(__name__)


@activity.defn
def write_amount_hold_csv(file_path: str, records: list) -> str:
    logger.info("Writing amount hold result CSV")

    try:
        output_path = f"{file_path.rsplit('/', 1)[0]}/amount_hold_users.csv"
        pd.DataFrame(records).to_csv(output_path, index=False)
        return output_path

    except Exception as exc:
        logger.exception("Failed to write amount hold CSV")
        raise CsvWriteError(str(exc)) from exc
