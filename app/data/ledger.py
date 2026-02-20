import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/sales.db")
DB_PATH.parent.mkdir(exist_ok=True)

TABLE_NAME = "sales_ledger"


def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    with get_conn() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                date TEXT,
                account TEXT,
                ASIN TEXT,
                sales REAL,
                net_sales REAL
            )
            """
        )


def load_ledger() -> pd.DataFrame:
    init_db()
    with get_conn() as conn:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

    if df.empty:
        return pd.DataFrame(
            columns=["date", "account", "ASIN", "sales", "net_sales"]
        )

    df["date"] = pd.to_datetime(df["date"])
    return df


def save_ledger(df: pd.DataFrame):
    init_db()
    with get_conn() as conn:
        # 🔒 Replace whole table (safe & simple for your use case)
        conn.execute(f"DELETE FROM {TABLE_NAME}")
        df.copy().assign(
            date=lambda x: x["date"].astype(str)
        ).to_sql(
            TABLE_NAME,
            conn,
            if_exists="append",
            index=False,
        )
