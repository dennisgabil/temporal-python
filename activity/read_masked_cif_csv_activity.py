import logging
import os
import pandas as pd
import psycopg2
import math
from temporalio import activity
from typing import List, Dict
from dotenv import load_dotenv
from datetime import datetime

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
def process_masked_cif_data(records: List[Dict]) -> List[Dict]:
    logger.info(
        "Starting CIF data processing",
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

        comments = []
        with conn, conn.cursor() as cursor:
            for row in df.itertuples(index=False):
                if not isinstance(row.cif_code, str):
                    if math.isnan(row.cif_code):
                        logger.info("There is invalid or no cif_code is present. Hence skipping operation.")
                        comments.append("unavailable cif_code")
                        continue
                    else:
                        comment["Invalid cif_code"]
                unmask_cif_code = masker.unmask(row.cif_code)
                cursor.execute(
                    """
                    SELECT DISTINCT u.cifid
                    FROM users u
                    JOIN addresses a ON u.id = a.user_id
                    WHERE u.first_name = %s
                      AND u.last_name  = %s
                      AND u.dob        = %s
                      AND a.address    = %s
                      AND u.cifid   = %s
                    """,
                    (
                        row.first_name,
                        row.last_name,
                        row.dob,
                        row.address,
                        unmask_cif_code
                    ),
                )

                result = cursor.fetchone()
                if result:
                    ## put amount hold from statement table.
                    cursor.execute("""SELECT usable_balance, hold_balance
                    from accounts
                    where cifid = %s """,
                    (result))
                    account_statement_result = cursor.fetchone()
                    if account_statement_result:
                        usable_balance, hold_balance = account_statement_result
                        usable_balance-=row.hold_amount
                        hold_balance+=row.hold_amount

                        cursor.execute("""
                        UPDATE accounts set 
                        usable_balance = %s, 
                        hold_balance = %s
                        where cifid = %s""",
                        (usable_balance,
                        hold_balance,
                        unmask_cif_code))
                        conn.commit()
                        row_count = cursor.rowcount
                        comments.append(f"AUD: {row.hold_amount} has been freezed.")
                    else:
                        comments.append("No user account found.")
                else:
                    cif_codes.append(None)
                    comments.append("No user details found.")

        df["comments"] = comments

        logger.info("CIF enrichment completed successfully")
        return df.to_dict(orient="records")

    except psycopg2.Error as exc:
        logger.exception("Database error")
        raise DatabaseError("Postgres failure") from exc
    except Exception as exc:
        logger.exception(f"Unexpected CIF enrichment error: {exc}")
        raise Exception("CIF enrichment failed")

@activity.defn
def process_masked_cif_data_sqlite(records: List[Dict]) -> List[Dict]:
    logger.info(
        "Starting CIF data processing",
        extra={"records_in": len(records)},
    )

    try:
        import sqlite3
        df = pd.DataFrame(records)
        masker = CifMasker(os.getenv("CIF_ENCRYPTION_KEY"))

        sqlite_db_path = os.getenv("SQLITE_DB", "./app.db")
        conn = sqlite3.connect(sqlite_db_path)
        conn.execute("PRAGMA foreign_keys = ON;")

        comments = []
        with conn:
            cursor = conn.cursor()
            for row in df.itertuples(index=False):
                if not isinstance(row.cif_code, str):
                    if math.isnan(row.cif_code):
                        logger.info("There is invalid or no cif_code is present. Hence skipping operation.")
                        comments.append("unavailable cif_code")
                        continue
                    else:
                        comment["Invalid cif_code"]
                unmask_cif_code = masker.unmask(row.cif_code)
                formatted_dob = row.dob
                cursor.execute(
                    """
                    SELECT DISTINCT u.cifid
                    FROM users u
                    JOIN addresses a ON u.id = a.user_id
                    WHERE u.first_name = ?
                      AND u.last_name  = ?
                      AND u.dob        = ?
                      AND a.address    = ?
                      AND u.cifid      = ?
                    """,
                    (
                        row.first_name,
                        row.last_name,
                        formatted_dob,
                        row.address,
                        unmask_cif_code
                    ),
                )
    
                user_match = cursor.fetchone()
                if not user_match:
                    comments.append("No user details found.")
                    continue

                cursor.execute(
                    """
                    SELECT usable_balance, hold_balance
                    FROM accounts
                    WHERE cifid = ?
                    """,
                    (unmask_cif_code,),
                )
                account_statement_result = cursor.fetchone()

                if not account_statement_result:
                    comments.append("No user account found.")
                    continue

                usable_balance, hold_balance = account_statement_result
                usable_balance = float(usable_balance or 0.0)
                hold_balance = float(hold_balance or 0.0)

                hold_amount = float(getattr(row, "hold_amount", 0.0) or 0.0)
                if hold_amount <= 0:
                    comments.append("invalid hold_amount")
                    continue

                if usable_balance >= hold_amount:
                    cursor.execute(
                        """
                        UPDATE accounts
                        SET usable_balance = usable_balance - ?,
                            hold_balance   = hold_balance + ?
                        WHERE cifid = ?
                        """,
                        (hold_amount, hold_amount, unmask_cif_code),
                    )
                    comments.append(f"AUD: {hold_amount} has been freezed.")
                else:
                    comments.append("insufficient balance")

            cursor.close()

        df["comments"] = comments

        logger.info("CIF enrichment completed successfully")
        return df.to_dict(orient="records")

    except psycopg2.Error as exc:
        logger.exception("Database error")
        raise DatabaseError("Postgres failure") from exc
    except Exception as exc:
        logger.exception(f"Unexpected CIF enrichment error: {exc}")
        raise Exception("CIF enrichment failed")
