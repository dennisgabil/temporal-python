from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
# from fastapi import UploadFile
import io
import os

with workflow.unsafe.imports_passed_through():
    from activity.file_validation_activity import validate_csv_file
    from activity.csv_read_activity import read_csv
    from activity.postgres_lookup_activity import enrich_with_cif_codes, enrich_with_cif_codes_sqlite
    from activity.csv_enrich_activity import write_enriched_csv
    from activity.file_upload_activity import upload_file_to_s3
    from activity.fetch_file_from_s3_bucket import fetch_file_from_s3

default_retry_policy = RetryPolicy(
    initial_interval = timedelta(seconds=2),
    backoff_coefficient = 2.0,
    maximum_interval = timedelta(seconds=30),
    maximum_attempts = 5
)

@workflow.defn
class RevenueFileWorkflow:
    @workflow.run
    async def run(self, s3_key: str) -> str:

        # State (checkpoint)
        state = {
            "step":"start",
            "file_path":None,
            "raw_data":None,
            "enriched_data":None,
            "enriched_file_path":None,
            "enrichment_source":"sqlite"
        }

        # STEP 1: Fetch File
        if state["step"] == "start":
            # try:
            state["file_path"] = await workflow.execute_activity(
                fetch_file_from_s3,
                "users_info.csv",
                schedule_to_close_timeout=timedelta(seconds=30),
                retry_policy=default_retry_policy
            )
            state['step'] = "fetched"
            # except Exception as e:
            #     decision = await self.handle_failure("fetch",str(e))
            #     if decision == "retry":
            #         return await self.run(s3_key)

        # STEP 2: Validation
        if state["step"] == "fetched":
            # try:
            await workflow.execute_activity(
                validate_csv_file,
                state["file_path"],
                schedule_to_close_timeout=timedelta(seconds=30),
                retry_policy=default_retry_policy
            )
            state['step'] = "validated"
            # except Exception as e:
            #     decision = await self.handle_failure("validation",str(e))
            #     if decision == "retry":
            #         state['step'] = "fetched"
            #     else:
            #         raise e

        # STEP 3: Read CSV
        if state["step"] == "validated":
            state["raw_data"] = await workflow.execute_activity(
                read_csv,
                state["file_path"],
                schedule_to_close_timeout=timedelta(minutes=1),
                retry_policy=default_retry_policy
            )
            state['step'] = "read"

        # STEP 4: Enrichment
        if state["step"] == "read":
            state["enriched_data"] = await workflow.execute_activity(
            enrich_with_cif_codes_sqlite,
            state["raw_data"],
            schedule_to_close_timeout=timedelta(minutes=5),
            retry_policy=default_retry_policy
        )
            state['step'] = "write"

        # STEP 5: Write File
        if state["step"] == "write":
            state["enriched_file_path"] = await workflow.execute_activity(
            write_enriched_csv,
            {"file_path": state['file_path'], "enriched_data": state["enriched_data"]},
            schedule_to_close_timeout=timedelta(minutes=1),
            retry_policy=default_retry_policy
        )
            state['step'] = "upload"

        # STEP 6: Upload File
        if state["step"] == "upload":
            await workflow.execute_activity(
            upload_file_to_s3,
            {
                "original_file_path": state["file_path"],
                "enriched_file_path": state["enriched_file_path"],
                "s3_key":s3_key
            },
            schedule_to_close_timeout=timedelta(seconds=30),
            retry_policy=default_retry_policy
        )
            state['step'] = "completed"

    # async def handle_failure(self,step:str,error:str)->str:
    #     decision = await workflow.execute_activity(
    #         agent_decision_activity,
    #         {"step":step,"error":error},
    #         schedule_to_close_timeout=timedelta(seconds=30),
    #     )

    #     return decision.get("action","fail")
 