from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
import os

with workflow.unsafe.imports_passed_through():
    from activity.file_validation_activity import validate_csv_file
    from activity.read_masked_cif_csv_activity import process_masked_cif_data, process_masked_cif_data_sqlite
    from activity.hold_amount_activity import hold_account_amount, hold_account_amount_sqlite
    from activity.write_amount_hold_csv_activity import write_amount_hold_csv
    from activity.fetch_file_from_s3_bucket import fetch_file_from_s3
    from activity.file_validation_activity import validate_csv_file
    from activity.csv_read_activity import read_amount_on_hold_csv
    from activity.csv_enrich_activity import write_enriched_csv
    from activity.file_upload_activity import upload_file_to_s3
    from activity.ml_scoring_activity import ml_score_records

default_retry_policy = RetryPolicy(
    initial_interval = timedelta(seconds=2),
    backoff_coefficient = 2.0,
    maximum_interval = timedelta(seconds=30),
    maximum_attempts = 5
)

@workflow.defn
class HoldAccountWithPenaltyWorkflow:

    @workflow.run
    async def run(self, s3_key: str) -> str:

         # State (checkpoint)
        state = {
            "step":"start",
            "file_path":None,
            "raw_data":None,
            "proessed_user_data":None,
            "ml_scored_data":None,
            "processed_file_path":None,
            "enrichment_source":"sqlite"
        }

        # STEP 1: Fetch File
        if state["step"] == "start":
            state['file_path'] = await workflow.execute_activity(
                fetch_file_from_s3,
                "hold_amount_with_cif_codes.csv",
                schedule_to_close_timeout=timedelta(seconds=30),
                retry_policy=default_retry_policy
            )
            state['step'] = "fetched"

        # STEP 2: Validation
        if state["step"] == "fetched":
            await workflow.execute_activity(
                validate_csv_file,
                state['file_path'],
                schedule_to_close_timeout=timedelta(seconds=30),
                retry_policy=default_retry_policy
            )
            state['step'] = "validated"

        if state['step'] == "validated":
            state['raw_data'] = await workflow.execute_activity(
                read_amount_on_hold_csv,
                state['file_path'],
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=default_retry_policy
            )
            state['step'] = "read"

        if state['step'] == "read":
            state['proessed_user_data'] = await workflow.execute_activity(
                process_masked_cif_data_sqlite,
                state['raw_data'],
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=default_retry_policy
            )
            state['step'] = "proessed"

        # STEP 5: ML risk scoring + action recommendations
        if state['step'] == "proessed":
            state['ml_scored_data'] = await workflow.execute_activity(
                ml_score_records,
                state['proessed_user_data'],
                schedule_to_close_timeout=timedelta(minutes=2),
                retry_policy=default_retry_policy
            )
            state['step'] = "ml_scored"

        # STEP 6: Write enriched CSV (now includes ML columns)
        if state['step'] == "ml_scored":
            state['processed_file_path'] = await workflow.execute_activity(
                write_enriched_csv,
                {"file_path": state['file_path'], "enriched_data": state['ml_scored_data']},
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=default_retry_policy
            )
            state['step'] = "write"

        if state['step'] == "write":
            await workflow.execute_activity(
                upload_file_to_s3,
                {
                    "original_file_path": state['file_path'],
                    "enriched_file_path": state['processed_file_path'],
                    "s3_key":s3_key,
                    "put_amount_on_hold_workflow": True
                },
                schedule_to_close_timeout=timedelta(seconds=30),
            )
            state['step'] = "completed"

        return "Hold operation completed"