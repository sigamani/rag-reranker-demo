import os
import csv, sqlite3, json
from typing import Dict, Tuple
from tqdm import tqdm
from pydantic import ValidationError

from utils.company import Company
from utils.policy import Policy

import logging

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)


def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def ensure_db(db_path: str, ddl_sql_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(load_sql(ddl_sql_path))
    conn.close()


def insert_companies(db_path: str, csv_path: str) -> Tuple[int, int, Dict[str, int]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    total, success = 0, 0
    errors_by_col: Dict[str, int] = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        total = len(rows)
        success = 0

        for row in tqdm(rows, total=total, desc="Companies"):
            try:
                comp = Company(**row)
            except Exception as e:
                msg = str(e)
                for field in (
                    "company_id",
                    "name",
                    "operating_jurisdiction",
                    "sector",
                    "last_login",
                ):
                    if field in msg:
                        errors_by_col[field] = errors_by_col.get(field, 0) + 1
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


def insert_policies(db_path: str, csv_path: str) -> Tuple[int, int, Dict[str, int]]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    total = 0
    success = 0
    errors_by_col: Dict[str, int] = {}

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        # wrap reader in tqdm without a total
        for row in tqdm(reader, desc="Policies"):
            total += 1
            data = {
                "policy_id": row["id"],
                "name": row["name"],
                "geography": row["geography"],
                "sectors": row["sectors"],
                "published_date": row["published_date"],
                "updated_date": row["updated_date"],
                "status": row["status"],
                "description": row["description"],
                "topics": row["topics"],
                "source_url": row["source_url"],
            }
            try:
                pol = Policy(**data)
            except ValidationError as e:
                for err in e.errors():
                    loc = err["loc"][0]
                    errors_by_col[loc] = errors_by_col.get(loc, 0) + 1
                logger.warning("Policy row skipped: %s", e)
                continue

            cur.execute(
                """
                INSERT INTO policy
                  (id, name, geography, sectors, published_date,
                   updated_date, status, description, topics, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pol.policy_id,
                    pol.name,
                    pol.geography,
                    pol.sectors,
                    pol.published_date.strftime("%Y-%m-%d"),
                    pol.updated_date.strftime("%Y-%m-%d %H:%M:%S"),
                    pol.status,
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
    conn = sqlite3.connect(db_path)
    conn.executescript(load_sql(view_sql_path))
    conn.close()


def fetch_relevant(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM relevant_policies;")
    rows = cur.fetchall()
    conn.close()
    return rows
