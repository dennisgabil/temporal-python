import logging
import os
from typing import List, Dict

import pandas as pd
import psycopg2
from temporalio import activity

from exceptions import DatabaseError
from utils.cif_masking import CifMasker

logger = logging.getLogger(__name__)


@activity.defn
def hold_account_amount(records: List[Dict]) -> List[Dict]:
    logger.info("Starting account hold operation")

    try:
        masker = CifMasker(os.getenv("CIF_ENCRYPTION_KEY"))
        df = pd.DataFrame(records)

        conn = psycopg2.connect(
            host=os.getenv("HOST"),
            dbname=os.getenv("DB"),
            user=os.getenv("USERNAME"),
            password=os.getenv("PASSWORD"),
        )

        comments: List[str] = []

        with conn, conn.cursor() as cursor:
            for row in df.itertuples(index=False):
                cif = masker.unmask(row.masked_cif)

                cursor.execute(
                    "SELECT balance FROM accounts WHERE cif_code = %s",
                    (cif,),
                )
                result = cursor.fetchone()

                if not result:
                    comments.append("account not found")
                    continue

                balance = result[0]

                if balance >= row.hold_amount:
                    cursor.execute(
                        """
                        UPDATE accounts
                        SET balance = balance - %s
                        WHERE cif_code = %s
                        """,
                        (row.hold_amount, cif),
                    )
                    comments.append("amount put on hold")
                else:
                    comments.append("insufficient balance")

        df["comment"] = comments
        logger.info("Hold operation completed")

        return df.to_dict(orient="records")

    except Exception as exc:
        logger.exception("Account hold failed")
        raise DatabaseError(str(exc)) from exc


@activity.defn
def hold_account_amount_sqlite(records: List[Dict]) -> List[Dict]:
    logger.info("Starting account hold operation (SQLite)")

    try:
        import sqlite3

        masker = CifMasker(os.getenv("CIF_ENCRYPTION_KEY"))
        df = pd.DataFrame(records)

        sqlite_db_path = os.getenv("SQLITE_DB", "./app.db")
        conn = sqlite3.connect(sqlite_db_path)
        conn.execute("PRAGMA foreign_keys = ON;")

        comments: List[str] = []

        with conn:
            cursor = conn.cursor()
            for row in df.itertuples(index=False):
                cif = masker.unmask(row.masked_cif)

                cursor.execute(
                    "SELECT usable_balance, hold_balance FROM accounts WHERE cifid = ?",
                    (cif,),
                )
                result = cursor.fetchone()

                if not result:
                    comments.append("account not found")
                    continue

                usable_balance, hold_balance = result
                usable_balance = float(usable_balance or 0.0)
                hold_balance = float(hold_balance or 0.0)

                hold_amount = float(row.hold_amount)

                if usable_balance >= hold_amount:
                    cursor.execute(
                        """
                        UPDATE accounts
                        SET usable_balance = usable_balance - ?,
                            hold_balance = hold_balance + ?
                        WHERE cifid = ?
                        """,
                        (hold_amount, hold_amount, cif),
                    )
                    comments.append("amount put on hold")
                else:
                    comments.append("insufficient balance")

            cursor.close()

        df["comment"] = comments
        logger.info("Hold operation completed (SQLite)")

        return df.to_dict(orient="records")

    except Exception as exc:
        logger.exception("Account hold failed (SQLite)")
        raise DatabaseError(str(exc)) from exc
