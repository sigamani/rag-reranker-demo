#!/usr/bin/env python3
import time
import logging

from utils.sqlite_helpers import (
    ensure_db,
    insert_companies,
    insert_policies,
    register_views,
    fetch_relevant,
)

logger = logging.getLogger(__name__)

def color_code(rate: float) -> str:
    if rate < 0.05:   return "GREEN"
    if rate <= 0.20:  return "ORANGE"
    return "RED"

def print_summary(name, total, success, errors):
    pct = f"{(success/total*100):.1f}%" if total else "N/A"
    print(f"{name}: {success}/{total} succeeded ({pct})")
    for col, cnt in errors.items():
        rate = cnt / total
        print(f"  • {col}: {cnt} errors ({rate:.1%}) → {color_code(rate)}")

def main():
    # configure_logging()
    start = time.time()
    db_path = "data/maiven.db"

    ensure_db(db_path, ddl_sql_path="sql/create_tables.sql")

    total_c, succ_c, err_c = insert_companies(db_path, "data/company_data.csv")
    total_p, succ_p, err_p = insert_policies(db_path, "data/random_policies.csv")

    register_views(db_path, view_sql_path="sql/create_views.sql")

    elapsed = time.time() - start
    print(f"\nETL finished in {elapsed:.2f}s\n")
    print_summary("Companies", total_c, succ_c, err_c)
    print_summary("Policies", total_p, succ_p, err_p)

    print("\nRelevant policies by company:")
    for row in fetch_relevant(db_path):
        print(row)

if __name__ == "__main__":
    main()