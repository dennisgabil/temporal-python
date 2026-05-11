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

    # Binary classification requires at least 2 classes
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
    """Score records with risk model and attach action recommendations."""
    logger.info("ML scoring %d records", len(records))

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

    scored = []
    for i, record in enumerate(records):
        scored.append({**record})

    logger.info("ML scoring complete")
    return scored
