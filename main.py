#!/usr/bin/env python3
import time

from utils.sqlite_helpers import (
    ensure_db,
    insert_companies,
    insert_policies,
    register_views,
    fetch_relevant,
)

# Constants
ERROR_THRESHOLDS = {"GREEN": 0.05, "ORANGE": 0.20}
PATHS = {
    "db": "data/maiven.db",
    "ddl": "sql/create_tables.sql",
    "views": "sql/create_views.sql",
    "companies": "data/company_data.csv",
    "policies": "data/random_policies.csv"
}


def get_error_rate_color(rate: float) -> str:
    if rate < ERROR_THRESHOLDS["GREEN"]:
        return "GREEN"
    if rate <= ERROR_THRESHOLDS["ORANGE"]:
        return "ORANGE"
    return "RED"


def calculate_success_rate(success: int, total: int) -> str:
    return f"{(success / total * 100):.1f}%" if total else "N/A"


def print_error_summary(errors: dict, total: int) -> None:
    for col, cnt in errors.items():
        rate = cnt / total
        color = get_error_rate_color(rate)
        print(f"  • {col}: {cnt} errors ({rate:.1%}) → {color}")


def print_summary(name: str, total: int, success: int, errors: dict) -> None:
    pct = calculate_success_rate(success, total)
    print(f"{name}: {success}/{total} succeeded ({pct})")
    print_error_summary(errors, total)


def setup_database(db_path: str) -> None:
    ensure_db(db_path, PATHS["ddl"])
    register_views(db_path, PATHS["views"])


def run_etl(db_path: str) -> tuple:
    company_results = insert_companies(db_path, PATHS["companies"])
    policy_results = insert_policies(db_path, PATHS["policies"])
    return company_results, policy_results


def print_table_header() -> None:
    headers = ['company_id', 'policy_id', 'geography', 'updated_date', 'avg_days']
    print(f"{headers[0]:<12} {headers[1]:<30} {headers[2]:<15} {headers[3]:<20} {headers[4]}")


def print_table_rows(rows: list) -> None:
    for company_id, policy_id, geography, updated_date, avg_days in rows:
        print(f"{company_id:<12} {policy_id:<30} {geography:<15} {updated_date:<20} {avg_days:.1f}")


def display_results(db_path: str) -> None:
    rows = fetch_relevant(db_path)
    if rows:
        print("\nRelevant policies by company:")
        print_table_header()
        print_table_rows(rows)
    else:
        print("No relevant policies found.")


def main():
    start = time.time()

    setup_database(PATHS["db"])
    company_results, policy_results = run_etl(PATHS["db"])

    elapsed = time.time() - start
    print(f"\nETL finished in {elapsed:.2f}s\n")

    print_summary("Companies", *company_results)
    print_summary("Policies", *policy_results)

    display_results(PATHS["db"])


if __name__ == "__main__":
    main()