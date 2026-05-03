import logging
import os
import pandas as pd
import psycopg2
from temporalio import activity
from typing import List, Dict
from dotenv import load_dotenv

from exceptions import DatabaseError
from utils.cif_masking import CifMasker

logger = logging.getLogger(__name__)
load_dotenv()
host = os.getenv("DB_HOST")
port = os.getenv("DB-PORT")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
db = os.getenv("DB_NAME")


@activity.defn
def enrich_with_cif_codes(records: List[Dict]) -> List[Dict]:
    logger.info(
        "Starting CIF enrichment",
        extra={"records_in": len(records)},
    )

    try:
        df = pd.DataFrame(records)
        masker = CifMasker(os.getenv("CIF_ENCRYPTION_KEY"))

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=username,
            password=password,
        )

        cif_codes = []
        comments = []
        with conn, conn.cursor() as cursor:
            for row in df.itertuples(index=False):
                cursor.execute(
                    """
                    SELECT DISTINCT u.cifid
                    FROM users u
                    JOIN addresses a ON u.id = a.user_id
                    WHERE u.first_name = %s
                      AND u.last_name  = %s
                      AND u.dob        = %s
                      AND a.address    = %s
                    """,
                    (
                        row.first_name,
                        row.last_name,
                        row.dob,
                        row.address,
                    ),
                )

                result = cursor.fetchone()
                if result:
                    cif_codes.append(masker.mask(str(result[0])))
                    comments.append("user details found.")
                else:
                    cif_codes.append(None)
                    comments.append("No user details found.")

        df["cif_code"] = cif_codes
        df["comments"] = comments

        logger.info("CIF enrichment completed successfully")
        return df.to_dict(orient="records")

    except psycopg2.Error as exc:
        logger.exception("Database error")
        raise DatabaseError("Postgres failure") from exc
    except Exception as exc:
        logger.exception("Unexpected CIF enrichment error")
        raise DatabaseError("CIF enrichment failed") from exc


@activity.defn
def enrich_with_cif_codes_sqlite(records: List[Dict]) -> List[Dict]:
    logger.info(
        "Starting CIF enrichment (SQLite)",
        extra={"records_in": len(records)},
    )

    try:
        import sqlite3  # local import to avoid changing existing imports

        df = pd.DataFrame(records)
        masker = CifMasker(os.getenv("CIF_ENCRYPTION_KEY"))

        sqlite_db_path = os.getenv("SQLITE_DB", "./app.db")
        conn = sqlite3.connect(sqlite_db_path)
        conn.execute("PRAGMA foreign_keys = ON;")

        cif_codes = []
        comments = []

        with conn:
            cursor = conn.cursor()
            for row in df.itertuples(index=False):
                cursor.execute(
                    """
                    SELECT DISTINCT u.cifid
                    FROM users u
                    JOIN addresses a ON u.id = a.user_id
                    WHERE u.first_name = ?
                      AND u.last_name  = ?
                      AND u.dob        = ?
                      AND a.address    = ?
                    """,
                    (
                        row.first_name,
                        row.last_name,
                        row.dob,
                        row.address,
                    ),
                )

                result = cursor.fetchone()
                if result:
                    cif_codes.append(masker.mask(str(result[0])))
                    comments.append("user details found.")
                else:
                    cif_codes.append(None)
                    comments.append("No user details found.")

            cursor.close()

        df["cif_code"] = cif_codes
        df["comments"] = comments

        logger.info("CIF enrichment completed successfully (SQLite)")
        return df.to_dict(orient="records")

    except Exception as exc:
        logger.exception("Unexpected CIF enrichment error (SQLite)")
        raise DatabaseError("CIF enrichment failed (SQLite)") from exc
