import os
import csv
import json
import logging
import sqlite3
import time
from typing import Dict

from tqdm import tqdm
from utils.models import Company, Policy

def configure_logging():
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fh = logging.FileHandler("logs/etl_errors.log", mode="a")
    fh.setLevel(logging.WARNING)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    ch.setFormatter(fmt)
    fh.setFormatter(fmt)
    root.handlers.clear()
    root.addHandler(ch)
    root.addHandler(fh)

logger = logging.getLogger(__name__)

def ensure_db(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS policy;")
    c.execute("DROP TABLE IF EXISTS company;")
    c.execute(
        """
        CREATE TABLE company (
          company_id INTEGER PRIMARY KEY,
          name TEXT,
          operating_jurisdiction TEXT,
          sector TEXT,
          last_login TIMESTAMP
        );
    """
    )
    c.execute(
        """
        CREATE TABLE policy (
          policy_id TEXT PRIMARY KEY,
          name TEXT,
          geography TEXT,
          sector TEXT,
          published_date DATE,
          updated_date TIMESTAMP,
          active BOOLEAN,
          description TEXT,
          topics TEXT,
          source_url TEXT
        );
    """
    )
    conn.commit()
    conn.close()

def insert_companies(db_path: str, csv_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    total = sum(1 for _ in open(csv_path)) - 1
    success = 0
    errors_by_col: Dict[str, int] = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, total=total, desc="Companies"):
            try:
                comp = Company(**row)
            except Exception as e:
                msg = str(e)
                for field in [
                    "company_id",
                    "name",
                    "operating_jurisdiction",
                    "sector",
                    "last_login",
                ]:
                    if field in msg:
                        errors_by_col[field] = errors_by_col.get(field, 0) + 1
                logger.warning("Company row skipped: %s", e)
                continue

            cur.execute(
                """
                INSERT OR IGNORE INTO company
                  (company_id,name,operating_jurisdiction,sector,last_login)
                VALUES (?,?,?,?,?)
            """,
                (
                    comp.company_id,
                    comp.name,
                    comp.operating_jurisdiction,
                    comp.sector,
                    comp.last_login.isoformat(),
                ),
            )
            success += 1

    conn.commit()
    conn.close()
    return total, success, errors_by_col

def insert_policies(db_path: str, csv_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    total = sum(1 for _ in open(csv_path)) - 1
    success = 0
    errors_by_col: Dict[str, int] = {}
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, total=total, desc="Policies"):
            try:
                pol = Policy(**row)
            except Exception as e:
                msg = str(e)
                for field in [
                    "id",
                    "name",
                    "geography",
                    "sectors",
                    "published_date",
                    "updated_date",
                    "status",
                    "description",
                    "topics",
                    "source_url",
                ]:
                    if field in msg:
                        errors_by_col[field] = errors_by_col.get(field, 0) + 1
                logger.warning("Policy row skipped: %s", e)
                continue

            cur.execute(
                """
                INSERT OR IGNORE INTO policy
                  (policy_id,name,geography,sector,published_date,
                   updated_date,active,description,topics,source_url)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
                (
                    pol.policy_id,
                    pol.name,
                    pol.geography,
                    pol.sector,
                    pol.published_date.isoformat(),
                    pol.updated_date.isoformat(),
                    pol.active,
                    pol.description,
                    json.dumps(pol.topics),
                    str(pol.source_url),
                ),
            )
            success += 1

    conn.commit()
    conn.close()
    return total, success, errors_by_col

def color_code(rate: float) -> str:
    if rate < 0.05:
        return "GREEN"
    if rate <= 0.20:
        return "ORANGE"
    return "RED"

def print_summary(table: str, total: int, success: int, errors: Dict[str, int]):
    print(f"\n=== {table} Summary ===")
    print(f"Total rows read   : {total}")
    print(f"Successfully saved: {success}")
    if errors:
        print("Column error rates:")
        for col, cnt in errors.items():
            rate = cnt / total
            print(f"  - {col:20s}: {cnt} errors / {rate:.1%} → {color_code(rate)}")
    else:
        print("No column‐level errors.")

def main():
    configure_logging()
    start = time.time()
    db = "data/maiven.db"

    ensure_db(db)

    total_c, succ_c, err_c = insert_companies(db, "data/company_data.csv")
    total_p, succ_p, err_p = insert_policies(db, "data/random_policies.csv")

    elapsed = time.time() - start
    print(f"\nETL completed in {elapsed:.2f}s")
    print_summary("Companies", total_c, succ_c, err_c)
    print_summary("Policies", total_p, succ_p, err_p)


def register_views(db_path: str, sql_file: str = "create_views.sql"):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    with open(sql_file, "r") as f:
        cur.executescript(f.read())
    conn.commit()
    conn.close()

def fetch_relevant(db_path: str):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("SELECT * FROM relevant_policies;")
    rows = cur.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    main()
    #db = "data/maiven.db"
    #ensure_db(db)           # your existing table‐creation
    #register_views(db)      # registers the relevant_policies view

    #d = fetch_relevant(db)
    #print(d)
