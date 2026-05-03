import asyncio
import logging
import concurrent.futures
from temporalio.client import Client
from temporalio.worker import Worker

# Workflows
from workflow.revenue_file_workflow import RevenueFileWorkflow
from workflow.hold_account_amount_workflow import HoldAccountWithPenaltyWorkflow

# Activities (shared + specific)
from activity.file_validation_activity import validate_csv_file
from activity.csv_read_activity import read_csv
from activity.postgres_lookup_activity import enrich_with_cif_codes, enrich_with_cif_codes_sqlite
from activity.csv_enrich_activity import write_enriched_csv
from activity.file_upload_activity import upload_file_to_s3

from activity.read_masked_cif_csv_activity import process_masked_cif_data, process_masked_cif_data_sqlite
from activity.hold_amount_activity import hold_account_amount, hold_account_amount_sqlite
from activity.write_amount_hold_csv_activity import write_amount_hold_csv
from activity.fetch_file_from_s3_bucket import fetch_file_from_s3
from activity.csv_read_activity import read_amount_on_hold_csv
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Starting Temporal worker")
    temporal_host_path = os.getenv("TEMPORAL_HOST_PATH", "localhost:7233")

    client = await Client.connect(temporal_host_path)
#    client = await Client.connect(
#     temporal_host_path,
#     namespace="cba-temporal.pz8tx",
#     api_key=os.getenv("TEMPORAL_API_KEY"),
#     tls=True,
# )

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        worker = Worker(
            client,
            task_queue="revenue-file-queue",
            workflows=[
                RevenueFileWorkflow,
                HoldAccountWithPenaltyWorkflow,
            ],
            activities=[
                # Common
                validate_csv_file,
                upload_file_to_s3,
                fetch_file_from_s3,

                # Extracting users workflow
                read_csv,
                enrich_with_cif_codes,
                enrich_with_cif_codes_sqlite,
                write_enriched_csv,

                # Hold account workflow
                read_amount_on_hold_csv,
                process_masked_cif_data,
                process_masked_cif_data_sqlite,
                hold_account_amount,
                hold_account_amount_sqlite,
                write_amount_hold_csv,
            ],
            activity_executor=executor,
        )

        logger.info("Worker started and polling task queue")
        await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
