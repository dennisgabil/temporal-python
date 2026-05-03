import re
from fastapi import UploadFile, HTTPException


_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def safe_filename(filename: str) -> str:
    """
    Sanitizes filename to prevent path traversal and unsafe chars.
    Keeps only letters, numbers, dot, underscore, hyphen.
    """
    name = filename.strip().split("/")[-1].split("\\")[-1]
    name = _FILENAME_RE.sub("_", name)
    if not name or name in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid file name.")
    return name


async def read_uploadfile_bytes(file: UploadFile) -> bytes:
    """
    Reads uploaded file bytes safely.
    """
    try:
        content = await file.read()
    finally:
        await file.close()

    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    return content
