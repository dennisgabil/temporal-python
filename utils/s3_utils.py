import boto3


def upload_bytes_to_s3(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    """
    Upload raw bytes to S3.
    AWS credentials/region should be provided via standard boto3 resolution:
    env vars, shared config, IAM role, etc.
    """
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType=content_type,
    )
