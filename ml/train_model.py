
import pandas as pd
from typing import Dict, Any, Tuple

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score

from ml.schema import NUMERIC_FEATURES, CATEGORICAL_FEATURES
from ml.feature_engineering import build_feature_frame


def train_binary_model(df: pd.DataFrame, label_col: str = "label_risk") -> Tuple[Pipeline, Dict[str, Any]]:
    """
    Train a binary classification model.

    Args:
        df: Raw input dataframe containing label + raw fields.
        label_col: Name of the label/target column in df.

    Returns:
        model: sklearn Pipeline (preprocess + classifier)
        metrics: training metrics dictionary
    """
    if df is None or not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    if df.empty:
        raise ValueError("Training data is empty. Cannot train model.")

    # Normalize column names (handles hidden spaces)
    df.columns = df.columns.str.strip()

    if label_col not in df.columns:
        raise KeyError(
            f"Label column '{label_col}' not found. Available columns: {list(df.columns)}"
        )

    # Build feature frame (your feature engineering step)
    # IMPORTANT: build_feature_frame should return a DataFrame with engineered feature columns
    feat_df = build_feature_frame(df)

    if feat_df is None or not isinstance(feat_df, pd.DataFrame) or feat_df.empty:
        raise ValueError("build_feature_frame(df) returned empty/invalid DataFrame. Cannot train.")

    feat_df.columns = feat_df.columns.str.strip()

    # Ensure required feature columns exist
    missing_num = [c for c in NUMERIC_FEATURES if c not in feat_df.columns]
    missing_cat = [c for c in CATEGORICAL_FEATURES if c not in feat_df.columns]
    if missing_num or missing_cat:
        raise KeyError(
            "Missing engineered features required by schema. "
            f"Missing numeric: {missing_num} | Missing categorical: {missing_cat} | "
            f"Available: {list(feat_df.columns)}"
        )

    # Prepare X / y
    y = df[label_col]

    # Ensure binary labels (at least two classes)
    uniq = pd.Series(y).dropna().unique()
    if len(uniq) < 2:
        raise ValueError(
            f"Label column '{label_col}' must contain at least 2 classes to train. Found: {uniq}"
        )

    X = feat_df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]

    # Preprocess
    preprocess = ColumnTransformer(
        transformers=[
            ("num", Pipeline(steps=[
                ("imputer", SimpleImputer(strategy="median")),
            ]), NUMERIC_FEATURES),

            ("cat", Pipeline(steps=[
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("ohe", OneHotEncoder(handle_unknown="ignore")),
            ]), CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False
    )

    # Classifier
    clf = LogisticRegression(max_iter=400, class_weight="balanced")

    # Full pipeline
    model = Pipeline(steps=[
        ("prep", preprocess),
        ("clf", clf),
    ])

    # Fit
    model.fit(X, y)

    # Quick evaluation on train (OK for PoC; replace with proper split/time-split for prod)
    # predict_proba can fail if y not binary/valid; we already validated >=2 classes
    p = model.predict_proba(X)[:, 1]

    metrics: Dict[str, Any] = {
        "roc_auc_train": float(roc_auc_score(y, p)),
        "pr_auc_train": float(average_precision_score(y, p)),
        "label": label_col,
        "n_rows": int(len(df)),
        "n_features": int(X.shape[1]),
    }

    return model, metrics
