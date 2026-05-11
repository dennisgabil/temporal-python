import os

MODEL_BUCKET = os.getenv("MODEL_BUCKET", os.getenv("S3_BUCKET", "your-bucket"))
MODEL_PREFIX = os.getenv("MODEL_PREFIX", "models")

RISK_MODEL_NAME = os.getenv("RISK_MODEL_NAME", "risk_model")
FUTURE_MODEL_NAME = os.getenv("FUTURE_MODEL_NAME", "future_default_model")

# If you want explicit version selection:
RISK_MODEL_VERSION = os.getenv("RISK_MODEL_VERSION", "latest")
FUTURE_MODEL_VERSION = os.getenv("FUTURE_MODEL_VERSION", "latest")

AI_OUTPUT_PREFIX = os.getenv("AI_OUTPUT_PREFIX", "runs")  # where to store per-run JSON results
