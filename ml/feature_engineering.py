import pandas as pd
from ml.schema import NUMERIC_FEATURES, CATEGORICAL_FEATURES

def build_feature_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Derive age from dob
    if "dob" in df.columns:
        df["age"] = (pd.Timestamp("today") - pd.to_datetime(df["dob"], errors="coerce")).dt.days // 365
    else:
        df["age"] = 30

    # Fill missing numeric features
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    # Fill missing categorical features
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            df[col] = "UNKNOWN"
        df[col] = df[col].astype(str).fillna("UNKNOWN")

    return df[NUMERIC_FEATURES + CATEGORICAL_FEATURES]
