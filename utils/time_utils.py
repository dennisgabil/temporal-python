from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))


def ist_hour_prefix() -> str:
    """
    Returns S3 prefix in format: data/YYYY-MM-DD-HH (HH in IST 00-23).
    Example: data/2026-04-17-20
    """
    now = datetime.now(IST)

    date_part = now.strftime("%Y-%m-%d")
    hour_part = now.strftime("%H")
    return f"{date_part}/{hour_part}"
