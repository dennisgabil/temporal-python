import sqlite3
import pandas as pd
from typing import List, Dict


def fetch_50_users_from_sqlite(
    sqlite_db_path: str = "app.db"
):
    """
    Fetches 50 users with first_name, last_name, dob, and address
    from the SQLite database.

    Tables used:
      - users
      - addresses
    """

    query = """
    SELECT
        u.first_name,
        u.last_name,
        u.dob,
        a.address
    FROM users u
    JOIN addresses a
        ON u.id = a.user_id
    WHERE a.is_current = 1
    LIMIT 50;
    """

    conn = sqlite3.connect(sqlite_db_path)

    df = pd.read_sql_query(query, conn)
    df.to_csv("users_info.csv", index=None)


fetch_50_users_from_sqlite()
 