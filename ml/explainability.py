import numpy as np
import pandas as pd
from typing import Dict, List, Any

from ml.schema import NUMERIC_FEATURES, CATEGORICAL_FEATURES

def explain_logistic(model_pipeline, X_raw: pd.DataFrame, top_k: int = 5) -> List[Dict[str, Any]]:
    """
    Generate approximate explanations:
    - For numeric: coefficient * value
    - For categorical: use OHE feature contributions
    """
    prep = model_pipeline.named_steps["prep"]
    clf = model_pipeline.named_steps["clf"]

    X_trans = prep.transform(X_raw)
    coefs = clf.coef_[0]

    # Get feature names after preprocess
    feature_names = []
    # numeric names
    feature_names.extend(NUMERIC_FEATURES)
    # categorical names after one hot
    ohe = prep.named_transformers_["cat"].named_steps["ohe"]
    cat_names = ohe.get_feature_names_out(CATEGORICAL_FEATURES).tolist()
    feature_names.extend(cat_names)

    contributions = X_trans.multiply(coefs) if hasattr(X_trans, "multiply") else X_trans * coefs
    contributions = np.asarray(contributions)

    explanations = []
    for i in range(contributions.shape[0]):
        row = contributions[i]
        idx = np.argsort(np.abs(row))[::-1][:top_k]
        top = [{"feature": feature_names[j], "impact": float(row[j])} for j in idx]
        explanations.append({"top_factors": top})

    return explanations
