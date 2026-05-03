# download_from_s3.py
import boto3
import os
import logging
import tempfile
import pandas as pd
from pathlib import Path

from temporalio import activity
from dotenv import load_dotenv
import random


logger = logging.getLogger(__name__)
load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")

@activity.defn
def fetch_file_from_s3(
    file_name: str,
) -> str:
    """
    Download a file from S3 using given s3_key and store it locally.
    """

    # s3 = boto3.client("s3")

    # # Create temporary file (accessible, OS-managed temp location)
    # suffix = Path(s3_key).suffix
    # temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    # temp_file_path = temp_file.name
    # temp_file.close()  # important for Windows compatibility

    # # Download file into temp path
    # s3.download_file(S3_BUCKET, s3_key, temp_file_path)
    
    rn = random.random()
    print("rn1..........",rn)
    # if rn<0.5:
    #     raise Exception("Unable to fetch file")
    base_dir = os.getcwd()
    temp_file_path = os.path.join(base_dir,file_name)
    df = pd.read_csv(temp_file_path, dtype=str)
    return temp_file_path