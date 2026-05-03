import logging
import pandas as pd
from temporalio import activity
from typing import Dict
from exceptions import CsvWriteError

logger = logging.getLogger(__name__)


@activity.defn
def write_enriched_csv(data: Dict) -> str:
    file_path = data.get("file_path")
    records = data.get("enriched_data")

    logger.info("Writing enriched CSV", extra={"file_path": file_path})

    try:
        enriched_file_path = file_path.replace(".csv", "_enriched.csv")
        df = pd.DataFrame(records)
        df.to_csv(enriched_file_path, index=False)

        logger.info(
            "Enriched CSV written",
            extra={"output_file": enriched_file_path},
        )
        return enriched_file_path

    except Exception as exc:
        logger.exception("CSV write failed")
        raise CsvWriteError(str(exc)) from exc
