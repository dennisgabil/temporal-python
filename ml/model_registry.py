import io
import json
from datetime import datetime
from typing import Any, Dict

import boto3
import joblib

from ml.config import MODEL_BUCKET, MODEL_PREFIX


def _s3_client():
    # Create lazily (safer for some environments)
    return boto3.client("s3")


def _model_key(model_name: str, version: str, filename: str) -> str:
    return f"{MODEL_PREFIX}/{model_name}/{version}/{filename}"


def save_model_to_s3(model: Any, model_name: str, version: str, metadata: Dict[str, Any]) -> Dict[str, str]:
    """
    Saves model.joblib + metadata.json and updates latest pointer in S3.
    Returns S3 keys.
    """
    s3 = _s3_client()

    # Save model bytes to memory
    buf = io.BytesIO()
    joblib.dump(model, buf)
    buf.seek(0)

    model_key = _model_key(model_name, version, "model.joblib")
    s3.put_object(Bucket=MODEL_BUCKET, Key=model_key, Body=buf.getvalue())

    # Ensure metadata is JSON serializable
    safe_meta = {**metadata}
    safe_meta.update({
        "model_name": model_name,
        "version": version,
        "saved_at": datetime.utcnow().isoformat()
    })

    meta_key = _model_key(model_name, version, "metadata.json")
    s3.put_object(Bucket=MODEL_BUCKET, Key=meta_key, Body=json.dumps(safe_meta, default=str).encode("utf-8"))

    # Update latest pointer
    latest_key = f"{MODEL_PREFIX}/{model_name}/latest.json"
    s3.put_object(Bucket=MODEL_BUCKET, Key=latest_key, Body=json.dumps({"version": version}).encode("utf-8"))

    return {"model_key": model_key, "metadata_key": meta_key, "latest_key": latest_key}


def load_model_from_s3(model_name: str, version: str = "latest") -> Any:
    """
    Loads model from S3. If version='latest', reads latest.json pointer.
    Returns the loaded sklearn pipeline/model object.
    """
    s3 = _s3_client()

    if version == "latest":
        latest_key = f"{MODEL_PREFIX}/{model_name}/latest.json"
        latest_obj = s3.get_object(Bucket=MODEL_BUCKET, Key=latest_key)
        version = json.loads(latest_obj["Body"].read().decode("utf-8"))["version"]

    model_key = _model_key(model_name, version, "model.joblib")
    obj = s3.get_object(Bucket=MODEL_BUCKET, Key=model_key)
    return joblib.load(io.BytesIO(obj["Body"].read()))