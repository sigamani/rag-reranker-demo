# main.py

import argparse
from utils import DataLoader, DBQuery, build_active_policies_sql


def main():
    parser = argparse.ArgumentParser(
        description="DataLoader: load CSVs into DB; DBQuery: run SQL on DB"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subparser for loading CSVs into SQLite
    load_parser = subparsers.add_parser(
        "load", help="Load CSV files into DB"
    )
    load_parser.add_argument(
        "--csv-dir",
        type=str,
        default="data",
        help="Directory containing csv files",
    )
    load_parser.add_argument(
        "--db-path",
        type=str,
        default="data/maiven.db",
        help="Output DB database file path",
    )

    # Subparser for running a query against DB
    query_parser = subparsers.add_parser(
        "query", help="Run SQL query against DB"
    )
    query_parser.add_argument(
        "--db-path",
        type=str,
        default="data/maiven.db",
        help="DB database file path",
    )
    query_parser.add_argument(
        "sql",
        type=str,
        nargs="?",
        help="The SQL query string to execute",
    )
    query_parser.add_argument(
        "--active-policies",
        action="store_true",
        help="Run the built-in active-policies query",
    )

    args = parser.parse_args()

    if args.command == "load":
        loader = DataLoader(
                 csv_dir=args.csv_dir, 
                 sqlite_path=args.db_path
                 )
        loader.load_and_save()

    elif args.command == "query":
        if not args.active_policies and not args.sql:
            parser.error(
                "For 'query', provide SQL command or --active-policies."
            )
        if args.active_policies and args.sql:
            parser.error(
                "Cannot specify both SQL command and --active-policies."
            )

        querier = DBQuery(sqlite_path=args.db_path)
        if args.active_policies:
            sql = build_active_policies_sql()
        else:
            sql = args.sql

        querier.print_query(sql)


if __name__ == "__main__":
    main()
