import sys
import asyncio
import logging
from temporalio.client import Client
# from workflow.revenue_file_workflow import RevenueFileWorkflow
from workflow.revenue_file_workflow import RevenueFileWorkflow
from workflow.hold_account_amount_workflow import HoldAccountWithPenaltyWorkflow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

SUPPORTED_ACTIONS = {
    "extracting_users": RevenueFileWorkflow.run,
    "hold_account_with_penality_amount": HoldAccountWithPenaltyWorkflow.run,
}


async def main(action: str, file_path: str) -> None:
    logger.info(
        "Starting workflow execution",
        extra={"action": action, "file_path": file_path},
    )

    if action not in SUPPORTED_ACTIONS:
        raise ValueError(
            f"Unsupported action '{action}'. "
            f"Supported actions: {list(SUPPORTED_ACTIONS.keys())}"
        )

    client = await Client.connect("localhost:7233")

    workflow_run_method = SUPPORTED_ACTIONS[action]

    await client.execute_workflow(
        workflow_run_method,
        file_path,
        id=f"{action}-{file_path.replace('/', '-')}",
        task_queue="revenue-file-queue",
    )

    logger.info(
        "Workflow execution triggered successfully",
        extra={"action": action, "file_path": file_path},
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python run_workflow_event.py <action> <file_path>"
        )

    asyncio.run(main(sys.argv[1], sys.argv[2]))

import logging
 