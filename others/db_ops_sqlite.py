import os
import random
import sqlite3
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

# Single SQLite DB file (set in .env or use default)
# SQLITE_DB=./app.db
SQLITE_DB = os.getenv("SQLITE_DB", "./app.db")

TOTAL_RECORDS = int(os.getenv("TOTAL_RECORDS", "10000"))
RESET_DB = os.getenv("RESET_DB", "true").lower() in ("1", "true", "yes", "y")


def get_sqlite_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def create_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cifid TEXT UNIQUE NOT NULL,
            first_name TEXT,
            last_name TEXT,
            dob TEXT, -- ISO date string: YYYY-MM-DD
            created_at TEXT DEFAULT (datetime('now'))
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            address TEXT NOT NULL,
            is_current INTEGER DEFAULT 1, -- 1=True, 0=False
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cifid TEXT NOT NULL,
            usable_balance REAL DEFAULT 0.0,
            hold_balance REAL DEFAULT 0.0,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (cifid) REFERENCES users(cifid) ON DELETE CASCADE
        );
        """
    )

    conn.commit()
    cur.close()


def reset_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM accounts;")
    cur.execute("DELETE FROM addresses;")
    cur.execute("DELETE FROM users;")
    cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('users', 'addresses', 'accounts');")
    conn.commit()
    cur.close()


def random_date(start: date, end: date) -> str:
    delta = (end - start).days
    d = start + timedelta(days=random.randint(0, delta))
    return d.isoformat()


def seed_data(conn: sqlite3.Connection, n: int) -> None:
    first_names = [
        "Oliver", "Jack", "Noah", "William", "James", "Henry", "Lucas", "Thomas", "Ethan", "Leo",
        "Charlotte", "Amelia", "Olivia", "Mia", "Ava", "Emily", "Sofia", "Chloe", "Ruby", "Ella",
        "Grace", "Harper", "Isla", "Matilda", "Zoe", "Liam", "Mason", "Samuel", "Oscar", "Benjamin",
    ]
    last_names = [
        "Smith", "Jones", "Williams", "Brown", "Wilson", "Taylor", "Anderson", "Johnson", "White", "Thompson",
        "Martin", "Harris", "Walker", "Kelly", "Ryan", "Robinson", "Hall", "Evans", "Wright", "Campbell",
        "Mitchell", "Clarke", "Moore", "Young", "King", "Scott", "Cooper", "Murphy", "Edwards", "Baker",
    ]

    # Street name parts and suffixes
    street_bases = [
        "George", "Collins", "Elizabeth", "King", "Oxford", "Victoria", "Crown", "Bridge", "Park", "High",
        "Church", "River", "Ocean", "Grove", "Station", "Garden", "Harbour", "Bay", "Forest", "Hill",
        "Pacific", "Princes", "Lygon", "Chapel", "Pitt", "Castlereagh", "Flinders", "Bourke", "Spencer", "Swanston",
    ]
    street_types = ["Street", "Road", "Avenue", "Drive", "Crescent", "Lane", "Court", "Parade", "Terrace", "Place", "Way"]

    # NSW (suburb/locality, state, country)
    nsw_localities = [
        "Bondi Beach", "Manly", "Parramatta", "Chatswood", "Surry Hills", "Newtown", "Blacktown",
        "Ryde", "Mosman", "Randwick", "Coogee", "Glebe", "Strathfield", "Burwood", "Bankstown",
        "Liverpool", "Penrith", "Hurstville", "Kogarah", "Cronulla", "Dee Why", "Neutral Bay",
        "North Sydney", "Redfern", "Paddington", "Balmain", "Marrickville", "Ultimo", "Darlinghurst",
        "Wollongong", "Newcastle",
    ]

    # Victoria (suburb/locality, state, country)
    vic_localities = [
        "Melbourne", "Southbank", "Docklands", "Carlton", "Fitzroy", "Richmond", "St Kilda",
        "Footscray", "Brunswick", "Collingwood", "South Yarra", "Prahran", "Toorak", "Hawthorn",
        "Kew", "Camberwell", "Ivanhoe", "Essendon", "Moonee Ponds", "Coburg", "Preston",
        "Northcote", "Port Melbourne", "Albert Park", "Williamstown", "Yarraville", "Sunshine",
        "Geelong", "Ballarat", "Bendigo",
    ]

    all_places = [(loc, "NSW", "Australia") for loc in nsw_localities] + [(loc, "VIC", "Australia") for loc in vic_localities]

    cur = conn.cursor()
    cur.execute("BEGIN;")

    # 1) Insert users
    user_rows = []
    for i in range(1, n + 1):
        cifid = f"CIF{i:06d}"
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        dob = random_date(date(1960, 1, 1), date(2005, 12, 31))
        user_rows.append((cifid, fn, ln, dob))

    cur.executemany(
        "INSERT INTO users (cifid, first_name, last_name, dob) VALUES (?, ?, ?, ?);",
        user_rows
    )

    # Map: cifid -> user_id
    cur.execute("SELECT id, cifid FROM users;")
    id_by_cif = {cif: uid for (uid, cif) in cur.fetchall()}

    # 2) Insert addresses (pin-pointed: house number, street name, suburb/city, state, country)
    address_rows = []
    for cifid, _, _, _ in user_rows:
        user_id = id_by_cif[cifid]
        locality, state, country = random.choice(all_places)

        house_no = random.randint(1, 999)
        street = random.choice(street_bases)
        st_type = random.choice(street_types)

        # Example: "221 George Street, Parramatta, NSW, Australia"
        addr = f"{house_no} {street} {st_type}, {locality}, {state}, {country}"
        address_rows.append((user_id, addr, 1))

    cur.executemany(
        "INSERT INTO addresses (user_id, address, is_current) VALUES (?, ?, ?);",
        address_rows
    )

    # 3) Insert accounts (1 per user)
    account_rows = []
    for cifid, _, _, _ in user_rows:
        usable = round(random.uniform(0, 250000), 2)
        hold = round(random.uniform(0, 25000), 2)
        account_rows.append((cifid, usable, hold))

    cur.executemany(
        "INSERT INTO accounts (cifid, usable_balance, hold_balance) VALUES (?, ?, ?);",
        account_rows
    )

    conn.commit()
    cur.close()


def main():
    conn = get_sqlite_conn(SQLITE_DB)
    create_tables(conn)

    if RESET_DB:
        reset_tables(conn)

    seed_data(conn, TOTAL_RECORDS)
    conn.close()


if __name__ == "__main__":
    main()
 