import os
import csv
import json
import sqlite3
import logging
from typing import Dict

from tqdm import tqdm
from utils.company import Company
from utils.policy import Policy
from dataclasses import fields

logger = logging.getLogger(__name__)

def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def ensure_db(db_path: str, ddl_sql_path: str):
    """Create (or recreate) company & policy tables from SQL file."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(load_sql(ddl_sql_path))
    conn.close()

def insert_companies(db_path: str, csv_path: str) -> (int, int, Dict[str,int]):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    total = sum(1 for _ in open(csv_path)) - 1
    success = 0
    errors_by_col: Dict[str,int] = {}

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, total=total, desc="Companies"):
            try:
                comp = Company(**row)
            except Exception as e:
                msg = str(e)
                for field in ("company_id","name",
                              "operating_jurisdiction",
                              "sector","last_login"):
                    if field in msg:
                        errors_by_col[field] = errors_by_col.get(field,0) + 1
                logger.warning("Company row skipped: %s", e)
                continue

            cur.execute(
                """
                INSERT OR IGNORE INTO company
                  (company_id, name, operating_jurisdiction, sector, last_login)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    comp.company_id,
                    comp.name,
                    comp.operating_jurisdiction,
                    comp.sector,
                    comp.last_login.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            success += 1

    conn.commit()
    conn.close()
    return total, success, errors_by_col


def insert_policies(db_path: str, csv_path: str) -> (int, int, Dict[str, int]):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    total = sum(1 for _ in open(csv_path)) - 1
    success = 0
    errors_by_col: Dict[str,int] = {}

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in tqdm(reader, total=total, desc="Policies"):
            try:
                pol = Policy(**row)
            except Exception as e:
                msg = str(e)
                expected = {f.name for f in fields(Policy)}
                for fld in expected:
                    if fld in msg:
                        errors_by_col[fld] = errors_by_col.get(fld, 0) + 1
                logger.warning("Policy row skipped: %s", e)
                continue

            cur.execute(
                """
                INSERT OR IGNORE INTO policy
                  (policy_id, name, geography, sector, published_date,
                   updated_date, active, description, topics, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pol.policy_id,
                    pol.name,
                    pol.geography,
                    pol.sector,
                    pol.published_date.strftime("%Y-%m-%d"),
                    pol.updated_date.strftime("%Y-%m-%d %H:%M:%S"),
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

def register_views(db_path: str, view_sql_path: str):
    """Read a .sql file and execute it to create views."""
    conn = sqlite3.connect(db_path)
    conn.executescript(load_sql(view_sql_path))
    conn.close()

def fetch_relevant(db_path: str):
    """Return all rows from the relevant_policies view."""
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute("SELECT * FROM relevant_policies;")
    rows = cur.fetchall()
    conn.close()
    return rows