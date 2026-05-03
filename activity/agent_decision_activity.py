import re
import asyncio
from activity.agent_tools import (
get_workflows_tool,
get_workflow_dashboard_tool,
get_activities_tool
)


def run_async(func, *args):
    return asyncio.run(func(*args))


# def extract_ids(query: str):
#     """
#     Extract workflow_id and run_id from query
#     Expected format: workflow_id,run_id
#     """
#     match = re.search(r'(\w+)\s*,\s*(\w+)', query)
#     if match:
#         return match.group(1), match.group(2)
#     return None, None
def extract_ids(query: str):
    """
    Handles:
    - extracting_users-users_info.csv,019dcb22-670b-774e-b60a-8c150621fde4
    - workflow_id=... run_id=...
    """

    # 🔹 1. Explicit format (BEST)
    wf_match = re.search(r'workflow[_\s]?id[:=]?\s*([\w\-.]+)', query, re.I)
    run_match = re.search(r'run[_\s]?id[:=]?\s*([\w\-]+)', query, re.I)

    if wf_match and run_match:
        return wf_match.group(1), run_match.group(1)

    # 🔹 2. Comma-separated format (YOUR CASE)
    comma_match = re.search(r'([\w\-.]+)\s*,\s*([0-9a-fA-F\-]{10,})', query)
    if comma_match:
        return comma_match.group(1), comma_match.group(2)

    # 🔹 3. Fallback (UUID detection)
    run_match = re.search(r'\b[0-9a-fA-F\-]{10,}\b', query)
    run_id = run_match.group(0) if run_match else None

    # try to extract workflow_id (everything before run_id)
    if run_id:
        parts = query.split(run_id)
        possible_wf = parts[0].strip().split()[-1]
    return possible_wf, run_id

    return None, None


async def agent_handler(query: str):
    q = query.lower()
    print("q",q)
    # 🔹 1. Failed  workflows
    if "failed workflows" in q or ("failed" in q and "workflows" in q):
        data = await get_workflows_tool()
        return [wf for wf in data if wf["status"] == "FAILED"]
    
    elif "completed workflows" in q or ("completed" in q and "workflows" in q): 

        data = await get_workflows_tool()

        return [wf for wf in data if wf["status"] == "COMPLETED"]


    
    # 🔹 2. All  workflows
    elif "all workflows" in q or "list workflows" or "workflows" in q or ("completed" in q and "workflows" in q):
        return await get_workflows_tool()

    # 🔹 3. Dashboard
    elif "dashboard" in q:
        workflow_id, run_id = extract_ids(query)
        if not workflow_id:
            return {"error": "Provide workflow_id,run_id"}
        return await get_workflow_dashboard_tool(workflow_id, run_id)

    # 🔹 4. Activities
    elif "activities" in q:
        workflow_id, run_id = extract_ids(query)
        print('asasa',workflow_id, run_id)
        if not workflow_id:
            return {"error": "Provide workflow_id,run_id"}
        return await get_activities_tool(workflow_id, run_id)

    else:
        return {"message": "Query not supported"}