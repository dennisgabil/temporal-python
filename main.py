import logging
import os  
from fastapi import FastAPI, UploadFile, File, HTTPException
from utils.csv_utils import validate_csv_columns
from utils.s3_utils import upload_bytes_to_s3
from utils.time_utils import ist_hour_prefix
from utils.file_utils import safe_filename, read_uploadfile_bytes
from workflow.revenue_file_workflow import RevenueFileWorkflow
from workflow.hold_account_amount_workflow import HoldAccountWithPenaltyWorkflow
from dotenv import load_dotenv
from temporalio.client import Client
from datetime import datetime,timezone
from temporalio.api.enums.v1 import EventType
from activity.agent_decision_activity import agent_handler
from pydantic import BaseModel
import pandas as pd
import json

logger = logging.getLogger(__name__)
load_dotenv()

SUPPORTED_ACTIONS = {
    "extracting_users": RevenueFileWorkflow.run,
    "hold_account_with_penality_amount": HoldAccountWithPenaltyWorkflow.run,
}

S3_BUCKET = os.getenv("S3_BUCKET")

app = FastAPI(title="Garnishi Workflow", version="1.0.0")

from fastapi.middleware.cors import CORSMiddleware

origins = [
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:3001",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # or ["*"] for testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

temporal_client = None

# temporal_host_path = os.getenv("TEMPORAL_HOST_PATH", "localhost:7233")
temporal_host_path = "localhost:7233"

@app.on_event("startup")
async def startup_event():
    global temporal_client
    temporal_client = await Client.connect(temporal_host_path)
    if not temporal_client:
        raise Exception("Temporal client is not turning up.")
    else:
        logger.info("Temporal client is up.")


@app.post("/load-revenue-file")
async def load_revenue_file(file: UploadFile = File(...)):
    try:
        if not file.filename:
            raise HTTPException(status_code=400, detail="File name is missing.")

        filename = safe_filename(file.filename)

        if not filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only .csv files are allowed.")

        # 2) Validate required columns
        content_bytes = await read_uploadfile_bytes(file)
        validate_csv_columns(content_bytes)

        hour_prefix = ist_hour_prefix()
        s3_key = f"{hour_prefix}/telemetry/{filename}"
        # upload_bytes_to_s3(
        #     bucket=S3_BUCKET,
        #     key=s3_key,
        #     data=content_bytes,
        #     content_type="text/csv",
        # )
        workflow_run_method = SUPPORTED_ACTIONS["extracting_users"]
        workflow_run = await temporal_client.execute_workflow(
            workflow_run_method,
            s3_key,
            id=f"extracting_users-{file.filename.replace('/', '-')}",
            task_queue="revenue-file-queue",
        )
        new_s3_key = s3_key.replace("telemetry", "cif-codes")
        base_dir = os.getcwd()
        CSV_PATH = os.path.join(base_dir,"users_info_enriched.csv")
        print('CSV_PATH',CSV_PATH)
        if not os.path.exists(CSV_PATH):
            raise HTTPException(status_code=404, detail="CSV file not found")

        df = pd.read_csv(CSV_PATH)
        enriched_data = json.loads(df.to_json(orient="records"))
        return {
        "message": "File processed successfully. Processed file will be available in S3 after workflow completion.",
        "key": new_s3_key,
        "enriched_data":enriched_data
               }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload to S3: {str(e)}")


@app.post("/put-amount-on-hold")
async def put_amount_on_hold(file: UploadFile = File(...)):
        try:
            if not file.filename:
                raise HTTPException(status_code=400, detail="File name is missing.")

            filename = safe_filename(file.filename)
            if not filename.lower().endswith(".csv"):
                raise HTTPException(status_code=400, detail="Only .csv files are allowed.")
            # 2) Validate required columns
            content_bytes = await read_uploadfile_bytes(file)
            validate_csv_columns(content_bytes, telemetery_amount_put_on_hold=True)
            hour_prefix = ist_hour_prefix()
            s3_key = f"{hour_prefix}/telemetry-amount-hold/{filename}"
            # upload_bytes_to_s3(
            #     bucket=S3_BUCKET,
            #     key=s3_key,
            #     data=content_bytes,
            #     content_type="text/csv",
            # )
            workflow_run_method = SUPPORTED_ACTIONS["hold_account_with_penality_amount"]
            workflow_run = await temporal_client.execute_workflow(
                workflow_run_method,
                s3_key,
                id=f"extracting_users-{file.filename.replace('/', '-')}",
                task_queue="revenue-file-queue",
            )
            new_s3_key = s3_key.replace("telemetry-amount-hold", "freezed-amount-on-account")
            base_dir = os.getcwd()
            CSV_PATH = os.path.join(base_dir,"hold_amount_with_cif_codes.csv")
            if not os.path.exists(CSV_PATH):
                raise HTTPException(status_code=404, detail="CSV file not found")

            df = pd.read_csv(CSV_PATH)
            enriched_data = json.loads(df.to_json(orient="records"))

            from activity.ml_scoring_activity import ml_score_records
            enriched_data = ml_score_records(enriched_data)

            return {
            "message": "File processed successfully. Processed file will be available in S3 after workflow completion.",
            "key": new_s3_key,
            "enriched_data":enriched_data
                }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error while processing: {str(e)}")


class PaymentStatusRequest(BaseModel):
    cif_code: str

@app.post("/check-current-payment-status")
async def check_current_payment_status(request: PaymentStatusRequest):
    try:
        base_dir = os.getcwd()
        CSV_PATH = os.path.join(base_dir, "hold_amount_with_cif_codes.csv")
        if not os.path.exists(CSV_PATH):
            raise HTTPException(status_code=404, detail="CSV file not found")

        df = pd.read_csv(CSV_PATH)
        row = df[df["cif_code"] == request.cif_code]
        if row.empty:
            raise HTTPException(status_code=404, detail="CIF code not found")

        payment_status = str(row.iloc[0]["payment_status"])
        return {"cif_code": request.cif_code, "payment_status": "paid"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/train-risk-model")
async def train_risk_model():
    try:
        import joblib, boto3
        from ml.train_model import train_binary_model
        from ml.feature_engineering import build_feature_frame

        base_dir = os.getcwd()
        CSV_PATH = os.path.join(base_dir, "hold_amount_with_cif_codes.csv")
        if not os.path.exists(CSV_PATH):
            raise HTTPException(status_code=404, detail="CSV file not found")

        df = pd.read_csv(CSV_PATH)

        # Derive label: unpaid = 1 (high risk), paid = 0 (low risk)
        df["label_risk"] = df["payment_status"].str.lower().apply(
            lambda x: 1 if "unpaid" in str(x) else 0
        )

        # Train
        model, metrics = train_binary_model(df, label_col="label_risk")

        # Save model locally
        local_model_path = os.path.join(base_dir, "models", "risk_model_v1.joblib")
        os.makedirs(os.path.dirname(local_model_path), exist_ok=True)
        joblib.dump(model, local_model_path)

        # Generate predictions
        X = build_feature_frame(df)
        predictions = model.predict(X)
        probabilities = model.predict_proba(X)[:, 1]

        pred_df = pd.DataFrame({
            "first_name":      df["first_name"],
            "last_name":       df["last_name"],
            "cif_code":        df["cif_code"],
            "product":         df["product"],
            "hold_amount":     df["hold_amount"],
            "payment_status":  df["payment_status"],
            "customer_status": df["customer_status"],
            "risk_score":      probabilities.round(4),
            "risk_label":      ["HIGH RISK" if p == 1 else "LOW RISK" for p in predictions],
        })

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        predictions_filename = f"predictions_{timestamp}.csv"
        predictions_path = os.path.join(base_dir, "Prediction", predictions_filename)
        os.makedirs(os.path.dirname(predictions_path), exist_ok=True)
        pred_df.to_csv(predictions_path, index=False)

        # Upload model + predictions to S3 (mlfortemporal bucket)
        s3 = boto3.client(
            "s3",
            region_name=os.getenv("AWS_DEFAULT_REGION"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )
        ml_bucket = os.getenv("MODEL_BUCKET", "mlfortemporal")
        model_s3_key = os.getenv("RISK_MODEL_S3_KEY", "models/risk_model_v1.joblib")
        predictions_s3_key = f"Prediction/{predictions_filename}"

        s3.upload_file(local_model_path, ml_bucket, model_s3_key)
        s3.upload_file(predictions_path, ml_bucket, predictions_s3_key)

        return {
            "message": "Model trained, predictions generated, and files uploaded to S3",
            "metrics": metrics,
            "s3_model":       f"s3://{ml_bucket}/{model_s3_key}",
            "s3_predictions": f"s3://{ml_bucket}/{predictions_s3_key}",
            "high_risk_count": int((predictions == 1).sum()),
            "low_risk_count":  int((predictions == 0).sum()),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflows")
async def list_workflows():
    client = await Client.connect(temporal_host_path)

    workflows = []

    async for wf in client.list_workflows():
        workflows.append({
        "workflow_id": wf.id,
        "run_id": wf.run_id,
        "type": wf.workflow_type,
        "status": wf.status.name,
        "start_time": str(wf.start_time)
        })

    return {"data": workflows}

def calculate_duration(start, end):
    if start and end:
        return int((end - start).total_seconds() * 1000)
    return None

def convert_time(ts):
    return datetime.fromtimestamp(
    ts.seconds + ts.nanos / 1e9,
    tz=timezone.utc
    )   

@app.get("/workflow/{workflow_id}/{run_id}dashboard")
async def workflow_dashboard(workflow_id: str,run_id:str):
    client = await Client.connect(temporal_host_path)
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
        "workflow_id": workflow_id,
        "status": desc.status.name,
        "start_time": workflow_start.isoformat() + "Z" if workflow_start else None,
        "end_time": workflow_end.isoformat() + "Z" if workflow_end else None,
        "total_duration_ms": calculate_duration(workflow_start, workflow_end),
        "result": None, # (optional: extract from workflow result if needed)
        "activities": final_activities
    }

class Query(BaseModel):
    question:str

@app.post("/agent")
async def run_agent(query: Query):
    try:
        result = await agent_handler(query.question)
        return {"data": result}
    except Exception as e:
        return {"error": str(e)}
    
AGENTS = [
{
"agent_id": "get_workflow_agent",
"name": "Workflow Agent",
"description": "Fetches all workflows from Temporal",
"type": "rule-based"
},
{
"agent_id": "get_activity_agent",
"name": "Activity Agent",
"description": "Fetches activity details for workflow",
"type": "rule-based"
},
{
"agent_id": "read_csv_agent",
"name": "Read CSV Agent",
"description": "Builds workflow dashboard data",
"type": "rule-based"
}
]

@app.get("/list_agents")
def get_agents():
    return {
    "total_agents": len(AGENTS),
    "agents": AGENTS
    }


 