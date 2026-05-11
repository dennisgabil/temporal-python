from typing import List

NUMERIC_FEATURES: List[str] = [
    "hold_amount",
    "age",
]

CATEGORICAL_FEATURES: List[str] = [
    "customer_status",
    "product",
]

LABEL_RISK = "label_risk"

ID_COLUMNS: List[str] = [
    "cif_code",
    "first_name",
    "last_name",
]