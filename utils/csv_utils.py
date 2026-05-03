import io
import pandas as pd
from fastapi import HTTPException

RAW_REQUIRED_COLUMNS = {"first_name", "last_name", "dob", "address"}
HOLD_REQUIRED_COLUMNS = {"first_name", "last_name", "dob", "address", "cif_code", "comments"}


def validate_csv_columns(content_bytes: bytes, telemetery_amount_put_on_hold: bool= False) -> None:
    """
    Validates that CSV has exactly the required column names present
    (at minimum). Extra columns are allowed.
    """
    try:
        # Read only header + few rows (cheap) but also fine for full read
        df = pd.read_csv(io.BytesIO(content_bytes), dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid CSV file: {str(e)}")

    cols = set(map(str.strip, df.columns.astype(str)))
    if telemetery_amount_put_on_hold:
        missing = HOLD_REQUIRED_COLUMNS - cols
    else:
        missing = RAW_REQUIRED_COLUMNS - cols
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {sorted(missing)}. Required: {sorted(REQUIRED_COLUMNS)}",
        )
