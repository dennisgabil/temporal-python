# tools.py

from temporalio.client import Client
from datetime import datetime, timezone

client = None

async def get_client():
    global client
    if not client:
        client = await Client.connect("localhost:7233")
    return client


# 🔹 Tool 1: List workflows
async def get_workflows_tool():
    print("aaaaa")
    client = await Client.connect("localhost:7233")
    result = []

    async for wf in client.list_workflows():
        result.append({
        "workflow_id": wf.id,
        "run_id": wf.run_id,
        "status": wf.status.name
        })

    return result

def calculate_duration(start, end):
    if start and end:
        return int((end - start).total_seconds() * 1000)
    return None

def convert_time(ts):
    return datetime.fromtimestamp(
    ts.seconds + ts.nanos / 1e9,
    tz=timezone.utc
    )   
# 🔹 Tool 2: Workflow dashboard
async def get_workflow_dashboard_tool(workflow_id: str, run_id: str):
    client = await get_client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    desc = await handle.describe()

    return {
    "workflow_id": workflow_id,
    "run_id": run_id,
    "status": desc.status.name,
    "start_time": str(desc.start_time),
    "end_time": str(desc.close_time)
    }


# 🔹 Tool 3: Activities (simple)
async def get_activities_tool(workflow_id: str, run_id: str):
    from temporalio.api.enums.v1 import EventType

    client = await get_client()
    handle = client.get_workflow_handle(workflow_id=workflow_id, run_id=run_id)
    desc = await handle.describe()

    activities = {}

    workflow_start = desc.start_time
    workflow_end = desc.close_time

    async for event in handle.fetch_history_events():
        print('event',event)
        event_type = EventType.Name(event.event_type).replace("EVENT_TYPE_","")
        event_time = event.event_time
        print('event_type',event_type)
        # 🔹 Scheduled
        if event_type == "ACTIVITY_TASK_SCHEDULED":
            attr = event.activity_task_scheduled_event_attributes

            activities[event.event_id] = {
            "activity_name": attr.activity_type.name,
            "attempts": []
            }

        # 🔹 Started (new attempt)
        elif event_type == "ACTIVITY_TASK_STARTED":
            attr = event.activity_task_started_event_attributes
            scheduled_id = attr.scheduled_event_id

            if scheduled_id in activities:
                activities[scheduled_id]["attempts"].append({
                "attempt": len(activities[scheduled_id]["attempts"]) + 1,
                "status": "RUNNING",
                "start_time": event_time,
                "end_time": None,
                "error": None
                })

        # 🔹 Completed
        elif event_type == "ACTIVITY_TASK_COMPLETED":
            attr = event.activity_task_completed_event_attributes
            scheduled_id = attr.scheduled_event_id

            if scheduled_id in activities:
                attempt = activities[scheduled_id]["attempts"][-1]
                attempt["status"] = "SUCCESS"
                attempt["end_time"] = event_time

        # 🔹 Failed
        elif event_type == "ACTIVITY_TASK_FAILED":
            attr = event.activity_task_failed_event_attributes
            scheduled_id = attr.scheduled_event_id

            if scheduled_id in activities:
                attempt = activities[scheduled_id]["attempts"][-1]
                attempt["status"] = "FAILED"
                attempt["end_time"] = event_time

            try:
                attempt["error"] = attr.failure.message
            except:
                attempt["error"] = "Unknown error"

    # 🔥 Transform to required format
    final_activities = []
    print("activities",activities)
    for act in activities.values():
        attempts = act["attempts"]

        # format timestamps + duration
        for a in attempts:
            if a["start_time"]:
                start_dt = convert_time(a["start_time"])
                a["start_time"] = start_dt.isoformat().replace("+00:00", "Z")
            if a["end_time"]:
                end_dt = convert_time(a["end_time"])
                a["end_time"] = end_dt.isoformat().replace("+00:00", "Z")

            a["duration_ms"] = calculate_duration(
                datetime.fromisoformat(a["start_time"].replace("Z", "")) if a["start_time"] else None,
                datetime.fromisoformat(a["end_time"].replace("Z", "")) if a["end_time"] else None
            )

        # 🔹 If only 1 attempt → flat structure
        if len(attempts) == 1:
            a = attempts[0]
            final_activities.append({
            "activity_name": act["activity_name"],
            "status": a["status"],
            "attempt": a["attempt"],
            "start_time": a["start_time"],
            "end_time": a["end_time"],
            "duration_ms": a["duration_ms"]
            })

        # 🔹 If retries → nested attempts
        else:
            final_activities.append({
            "activity_name": act["activity_name"],
            "status": attempts[-1]["status"],
            "attempts": attempts
            })

    return {
        # "workflow_id": workflow_id,
        # "status": desc.status.name,
        # "start_time": workflow_start.isoformat() + "Z" if workflow_start else None,
        # "end_time": workflow_end.isoformat() + "Z" if workflow_end else None,
        # "total_duration_ms": calculate_duration(workflow_start, workflow_end),
        # "result": None, # (optional: extract from workflow result if needed)
        "activities": final_activities
    }

# from langchain.tools import tool

# @tool
# def retry_tool(input:str)->str:
#     return "retry"

# @tool
# def fail_tool(input:str)->str:
#     return "fail"

# @tool
# def switch_source_tool(input:str)->str:
#     return "switch_source"