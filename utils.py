# utils.py

import os
import sqlite3
from datetime import datetime, timedelta
import pandas as pd


class DBQuery:

    def __init__(self, sqlite_path: str):
        self.sqlite_path = sqlite_path

    def query(self, sql: str) -> list[sqlite3.Row]:
        if not os.path.exists(self.sqlite_path):
            raise FileNotFoundError(f"SQLite database '{self.sqlite_path}' not found.")

        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(sql)
        rows = cursor.fetchall()
        conn.close()
        return rows

    def print_query(self, sql: str) -> None:
        rows = self.query(sql)
        if not rows:
            print("No results returned.")
            return

        headers = rows[0].keys()
        print("\t".join(headers))
        print("-" * 80)
        for row in rows:
            print("\t".join(str(row[h]) for h in headers))


class DataLoader:
    def __init__(self, csv_dir: str, sqlite_path: str = "data/maiven.db"):
        self.csv_dir = csv_dir
        self.sqlite_path = sqlite_path
        os.makedirs(os.path.dirname(self.sqlite_path), exist_ok=True)

    def _read_csv(self, filename: str) -> pd.DataFrame:
        path = os.path.join(self.csv_dir, filename)
        df = pd.read_csv(path)
        return df

    def _clean_company(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def _clean_policy(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    def load_and_save(self) -> None:
        df_company = self._read_csv("company_data.csv")
        df_policy = self._read_csv("random_policies.csv")

        df_company = self._clean_company(df_company)
        df_policy = self._clean_policy(df_policy)

        conn = sqlite3.connect(self.sqlite_path)
        df_company.to_sql("company", conn, if_exists="replace", index=False)
        df_policy.to_sql("policy", conn, if_exists="replace", index=False)
        conn.close()

        print(f"Written 'company' and 'policy' tables to '{self.sqlite_path}'.")


def build_active_policies_sql() -> str:
    """
    Constructs the SQL for retrieving active policies updated in the last 100 days,
    along with the average days-since-update over the past year per geography.
    """
    today = datetime.utcnow().date()
    cutoff_100 = today - timedelta(days=100)
    cutoff_365 = today - timedelta(days=365)

    return f"""
    WITH recent_policies AS (
      SELECT
        p.policy_id,
        p.geography,
        p.updated_date,
        p.active,
        c.operating_jurisdiction AS jurisdiction
      FROM policy AS p
      JOIN company AS c
        ON p.geography = c.operating_jurisdiction
      WHERE p.active = 1
        AND date(p.updated_date) >= date('{cutoff_100}')
    ),
    avg_past_year AS (
      SELECT
        geography,
        AVG(julianday('now') - julianday(date(updated_date))) AS avg_days_since_update
      FROM policy
      WHERE active = 1
        AND date(updated_date) >= date('{cutoff_365}')
      GROUP BY geography
    )
    SELECT
      rp.policy_id,
      rp.geography,
      rp.updated_date,
      rp.active,
      apy.avg_days_since_update
    FROM recent_policies rp
    LEFT JOIN avg_past_year apy
      ON rp.geography = apy.geography
    ORDER BY rp.geography, rp.updated_date DESC;
    """
