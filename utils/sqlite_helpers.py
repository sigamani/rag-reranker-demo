import os
import csv, sqlite3, json
from typing import Dict, Tuple, List
from tqdm import tqdm
from utils.models import Company, Policy


def load_sql(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def create_db_connection(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def execute_sql_script(
    conn: sqlite3.Connection, sql_content: str
) -> sqlite3.Connection:
    conn.executescript(sql_content)
    return conn


def ensure_db(db_path: str, ddl_sql_path: str) -> sqlite3.Connection:
    conn = create_db_connection(db_path)
    sql_content = load_sql(ddl_sql_path)
    return execute_sql_script(conn, sql_content)


def parse_company_row(row: dict) -> Company:
    return Company(**row)


def insert_company_record(cur: sqlite3.Cursor, comp: Company) -> bool:
    cur.execute(
        "INSERT OR IGNORE INTO company (company_id, name, operating_jurisdiction, sector, last_login) VALUES (?, ?, ?, ?, ?)",
        (
            comp.company_id,
            comp.name,
            comp.operating_jurisdiction,
            comp.sector,
            comp.last_login.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    return True


def track_error(
    errors_by_col: Dict[str, int], error_msg: str, fields: List[str]
) -> Dict[str, int]:
    for field in fields:
        if field in error_msg:
            errors_by_col[field] = errors_by_col.get(field, 0) + 1
    return errors_by_col


def load_csv_rows(csv_path: str) -> List[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def insert_companies(db_path: str, csv_path: str) -> Tuple[int, int, Dict[str, int]]:
    conn = create_db_connection(db_path)
    cur = conn.cursor()
    rows = load_csv_rows(csv_path)

    total, success = len(rows), 0
    errors_by_col: Dict[str, int] = {}
    company_fields = [
        "company_id",
        "name",
        "operating_jurisdiction",
        "sector",
        "last_login",
    ]

    for row in tqdm(rows, desc="Companies"):
        try:
            comp = parse_company_row(row)
            insert_company_record(cur, comp)
            success += 1
        except Exception as e:
            errors_by_col = track_error(errors_by_col, str(e), company_fields)

    conn.commit()
    conn.close()
    return total, success, errors_by_col


def parse_policy_row(row: dict) -> Policy:
    return Policy(**row)


def insert_policy_record(cur: sqlite3.Cursor, pol: Policy) -> bool:
    cur.execute(
        "INSERT INTO policy (policy_id, name, geography, sectors, published_date, updated_date, status, description, topics, source_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    return True


def insert_policies(db_path: str, csv_path: str) -> Tuple[int, int, Dict[str, int]]:
    conn = create_db_connection(db_path)
    cur = conn.cursor()
    rows = load_csv_rows(csv_path)

    total, success = len(rows), 0
    errors_by_col: Dict[str, int] = {}
    policy_fields = [
        "policy_id",
        "name",
        "geography",
        "sectors",
        "published_date",
        "updated_date",
        "status",
        "description",
        "topics",
        "source_url",
    ]

    for row in tqdm(rows, desc="Policies"):
        try:
            pol = parse_policy_row(row)
            insert_policy_record(cur, pol)
            success += 1
        except Exception as e:
            errors_by_col = track_error(errors_by_col, str(e), policy_fields)

    conn.commit()
    conn.close()
    return total, success, errors_by_col


def register_views(db_path: str, view_sql_path: str) -> sqlite3.Connection:
    conn = create_db_connection(db_path)
    sql_content = load_sql(view_sql_path)
    return execute_sql_script(conn, sql_content)


def fetch_relevant(db_path: str) -> List[tuple]:
    conn = create_db_connection(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM relevant_policies;")
    rows = cur.fetchall()
    conn.close()
    return rows
