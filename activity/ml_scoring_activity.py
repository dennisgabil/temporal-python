import logging
import os
import joblib
import pandas as pd
from typing import List, Dict
from temporalio import activity

from ml.feature_engineering import build_feature_frame
from ml.recommendations import recommend_action

logger = logging.getLogger(__name__)

LOCAL_MODEL_PATH = os.getenv("LOCAL_MODEL_PATH", "./risk_model.joblib")


def _map_to_ml_features(df: pd.DataFrame) -> pd.DataFrame:
    """Map available CSV fields to ML feature schema before scoring."""
    df = df.copy()
    if "product" in df.columns and "product_type" not in df.columns:
        df["product_type"] = df["product"].astype(str)
    if "hold_amount" in df.columns and "outstanding_amount" not in df.columns:
        df["outstanding_amount"] = pd.to_numeric(df["hold_amount"], errors="coerce").fillna(0.0)
    if "payment_status" in df.columns and "num_defaults" not in df.columns:
        df["num_defaults"] = df["payment_status"].str.lower().apply(
            lambda x: 1 if "unpaid" in str(x) else 0
        )
    return df


def _get_or_train_model(df: pd.DataFrame):
    """Load model from disk; train and save inline if none exists."""
    if os.path.exists(LOCAL_MODEL_PATH):
        logger.info("Loading saved model from %s", LOCAL_MODEL_PATH)
        return joblib.load(LOCAL_MODEL_PATH)

    logger.info("No saved model found — training inline on current data")
    from ml.train_model import train_binary_model

    train_df = df.copy()
    if "payment_status" in train_df.columns:
        train_df["label_risk"] = train_df["payment_status"].str.lower().apply(
            lambda x: 1 if "unpaid" in str(x) else 0
        )
    else:
        train_df["label_risk"] = 0

    if train_df["label_risk"].nunique() < 2:
        synthetic = train_df.iloc[[0]].copy()
        synthetic["label_risk"] = 1 - int(train_df["label_risk"].iloc[0])
        train_df = pd.concat([train_df, synthetic], ignore_index=True)

    model, metrics = train_binary_model(train_df, label_col="label_risk")
    logger.info("Inline model trained — metrics: %s", metrics)
    joblib.dump(model, LOCAL_MODEL_PATH)
    return model


@activity.defn
def ml_score_records(records: List[Dict]) -> List[Dict]:
    """
    Score records with risk model.
    Uses heartbeating to checkpoint progress every 10 records.
    On attempt 1: simulates a failure after 40 records to demonstrate Temporal retries.
    On attempt 2+: resumes from the last heartbeat checkpoint.
    """
    info = activity.info()
    attempt = info.attempt
    heartbeat_details = info.heartbeat_details

    start_idx = 0
    scored = []

    print(f"[ML SCORING] Attempt {attempt} started | heartbeat_details present: {bool(heartbeat_details)}", flush=True)

    if heartbeat_details:
        checkpoint = heartbeat_details[0]
        start_idx = int(checkpoint.get("processed_count", 0)) if isinstance(checkpoint, dict) else 0
        scored = checkpoint.get("partial_results", []) if isinstance(checkpoint, dict) else []
        print(f"[ML SCORING] Attempt {attempt}: resuming from record {start_idx + 1}, recovered {len(scored)} records", flush=True)
        logger.info("Attempt %d: resuming from record %d (recovered %d already scored records)", attempt, start_idx + 1, len(scored))
    else:
        print(f"[ML SCORING] Attempt {attempt}: starting from record 1 of {len(records)}", flush=True)
        logger.info("Attempt %d: starting from record 1 of %d", attempt, len(records))

    # Score all records upfront so we have probabilities ready
    df = pd.DataFrame(records)
    df = _map_to_ml_features(df)

    try:
        model = _get_or_train_model(df)
        feat_df = build_feature_frame(df)
        proba = model.predict_proba(feat_df)[:, 1]
        risk_scores = (proba * 100).round(1).tolist()
    except Exception as exc:
        logger.warning("ML scoring failed (%s) — defaulting all scores to 50", exc)
        risk_scores = [50.0] * len(records)

    # Process records from where we left off
    for i in range(start_idx, len(records)):
        scored.append({**records[i]})

        # Heartbeat every 10 records to checkpoint progress
        if (i + 1) % 10 == 0:
            activity.heartbeat({"processed_count": i + 1, "partial_results": scored})
            print(f"[ML SCORING] Attempt {attempt}: heartbeat — processed {i + 1}/{len(records)} records", flush=True)
            logger.info("Attempt %d: heartbeat — processed %d/%d records", attempt, i + 1, len(records))

        # Simulate failure at record 40 on first attempt only
        if attempt == 1 and i == 39:
            activity.heartbeat({"processed_count": i + 1, "partial_results": scored})
            print(f"[ML SCORING] Attempt {attempt}: SIMULATED FAILURE at record {i + 1} — Temporal will retry from record {i + 2}", flush=True)
            raise Exception(
                f"Simulated failure after record {i + 1} — "
                f"Temporal will retry and resume from record {i + 2}"
            )

    print(f"[ML SCORING] Attempt {attempt}: COMPLETE — {len(scored)}/{len(records)} records scored", flush=True)
    logger.info("Attempt %d: ML scoring complete — %d/%d records scored", attempt, len(scored), len(records))
    return scored
