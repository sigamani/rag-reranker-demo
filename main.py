#!/usr/bin/env python3

import psycopg2
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text


def clean_data(file_path: str) -> pd.DataFrame:
    """
    Reads a CSV file into a pandas DataFrame.
    """
    df = pd.read_csv(file_path)
    return df


def load_to_db(df: pd.DataFrame, table_name: str, engine) -> None:
    """
    Loads the given DataFrame into the specified Postgres table.
    """
    df.to_sql(
        table_name, engine, if_exists="replace", index=False
    )  # pandas.DataFrame.to_sql leverages SQLAlchemy  [oai_citation:7‡pandas.pydata.org](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html?utm_source=chatgpt.com)


def export_tables_to_sqlite(
    pg_user: str, pg_pass: str, pg_host: str, pg_db: str, sqlite_file: str = "maiven.db"
) -> None:
    """
    Connects to the Postgres database and exports the 'company' and 'policy'
    tables into a SQLite file at `sqlite_file`.
    """
    # 1. Create a SQLAlchemy engine for Postgres
    pg_url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}/{pg_db}"
    pg_engine = create_engine(
        pg_url
    )  # SQLAlchemy engine connects to Postgres  [oai_citation:8‡medium.com](https://medium.com/%40alestamm/importing-data-from-a-postgresql-database-to-a-pandas-dataframe-5f4bffcd8bb2?utm_source=chatgpt.com) [oai_citation:9‡dev.to](https://dev.to/varungujarathi9/exporting-a-large-postgresql-table-using-python-4non?utm_source=chatgpt.com)

    # 2. Read 'company' and 'policy' tables into pandas DataFrames
    with pg_engine.connect() as conn:
        company_df = pd.read_sql(
            text("SELECT * FROM company"), conn
        )  # pd.read_sql reads table via SQLAlchemy engine  [oai_citation:10‡medium.com](https://medium.com/%40alestamm/importing-data-from-a-postgresql-database-to-a-pandas-dataframe-5f4bffcd8bb2?utm_source=chatgpt.com) [oai_citation:11‡stackoverflow.com](https://stackoverflow.com/questions/6148421/how-to-convert-a-postgres-database-to-sqlite?utm_source=chatgpt.com)
        policy_df = pd.read_sql(text("SELECT * FROM policy"), conn)

    # 3. Open (or create) the SQLite database file using sqlite3
    sqlite_conn = sqlite3.connect(
        sqlite_file
    )  # sqlite3 shipped with Python  [oai_citation:12‡theleftjoin.com](https://theleftjoin.com/how-to-write-a-pandas-dataframe-to-an-sqlite-table/?utm_source=chatgpt.com) [oai_citation:13‡serverfault.com](https://serverfault.com/questions/274355/how-to-convert-a-postgres-database-to-sqlite?utm_source=chatgpt.com)

    # 4. Write each DataFrame into SQLite (replacing if tables exist)
    company_df.to_sql(
        "company", sqlite_conn, if_exists="replace", index=False
    )  # DataFrame → SQLite table  [oai_citation:14‡theleftjoin.com](https://theleftjoin.com/how-to-write-a-pandas-dataframe-to-an-sqlite-table/?utm_source=chatgpt.com) [oai_citation:15‡pandas.pydata.org](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html?utm_source=chatgpt.com)
    policy_df.to_sql("policy", sqlite_conn, if_exists="replace", index=False)

    sqlite_conn.close()
    print(f"Exported 'company' and 'policy' tables into '{sqlite_file}'.")


def query_active_policies(sqlite_file: str = "maiven.db") -> None:
    """
    Opens the SQLite file, runs the SQL that:
      * Finds all active policies whose geography matches a customer's operating_jurisdiction
        and whose updated_date is within the last 100 days.
      * For each such policy, computes avg days-since-last-update over the past year for all policies in that geography.
    Prints the resulting rows.
    """
    conn = sqlite3.connect(sqlite_file)
    conn.row_factory = (
        sqlite3.Row
    )  # Enable name‐based column access  [oai_citation:16‡theleftjoin.com](https://theleftjoin.com/how-to-write-a-pandas-dataframe-to-an-sqlite-table/?utm_source=chatgpt.com) [oai_citation:17‡atcoordinates.info](https://atcoordinates.info/2017/07/24/copying-tables-from-sqlite-to-postgresql/?utm_source=chatgpt.com)

    # 1. Compute date boundaries (YYYY-MM-DD) using Python’s datetime
    today = (
        datetime.utcnow().date()
    )  # Current UTC date  [oai_citation:18‡dev.to](https://dev.to/varungujarathi9/exporting-a-large-postgresql-table-using-python-4non?utm_source=chatgpt.com)
    cutoff_100 = today - timedelta(
        days=100
    )  # 100 days ago  [oai_citation:19‡dev.to](https://dev.to/varungujarathi9/exporting-a-large-postgresql-table-using-python-4non?utm_source=chatgpt.com)
    cutoff_365 = today - timedelta(days=365)  # 365 days ago

    # 2. Define the SQL query with two CTEs: recent_policies and avg_past_year
    sql = f"""
    WITH recent_policies AS (
      SELECT
        p.*,
        c.operating_jurisdiction AS jurisdiction
      FROM policy p
      JOIN company c
        ON p.geography = c.operating_jurisdiction
      WHERE p.active = 1
        AND date(p.updated_date) >= date('{cutoff_100}')
    ),
    avg_past_year AS (
      SELECT
        geography,
        AVG(julianday(date('now')) - julianday(date(updated_date))) AS avg_days_since_update
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
    # SQLite’s date() strips timestamps to 'YYYY-MM-DD'; julianday() computes Julian day number  [oai_citation:20‡atcoordinates.info](https://atcoordinates.info/2017/07/24/copying-tables-from-sqlite-to-postgresql/?utm_source=chatgpt.com) [oai_citation:21‡serverfault.com](https://serverfault.com/questions/274355/how-to-convert-a-postgres-database-to-sqlite?utm_source=chatgpt.com)

    cursor = conn.execute(sql)
    rows = cursor.fetchall()

    if not rows:
        print("No matching active policies found.")
    else:
        # Print header row
        headers = rows[0].keys()
        print("\t".join(headers))
        print("-" * 80)
        for row in rows:
            print("\t".join(str(row[h]) for h in headers))

    conn.close()


def main() -> None:
    # Database connection parameters
    db_params = {
        "host": "localhost",
        "database": "maiven",
        "user": "michaelsigamani",
        "password": "policy",
    }

    # 1. Ensure Postgres database exists (connect & optionally CREATE DATABASE)
    conn = psycopg2.connect(
        host=db_params["host"],
        database=db_params["database"],
        user=db_params["user"],
        password=db_params["password"],
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # If you need to create the database from scratch, uncomment:
    # cur.execute("CREATE DATABASE maiven")
    conn.commit()
    cur.close()
    conn.close()

    # 2. Create SQLAlchemy engine for PostgreSQL
    engine = create_engine(
        f'postgresql://{db_params["user"]}:{db_params["password"]}@'
        f'{db_params["host"]}/{db_params["database"]}'
    )

    # 3. Define CSV file paths
    csv_files = {
        "company": "data/company_data.csv",
        "policy": "data/random_policies.csv",
    }

    # 4. Basic ETL: clean and load each table into Postgres
    for table_name, file_path in csv_files.items():
        print(f"Contents of '{table_name}' CSV file:")
        df = clean_data(file_path)
        print(df.head(2))
        print()
        load_to_db(df, table_name, engine)
        print(f"Table '{table_name}' loaded successfully.\n")

    # 5. Export Postgres tables to SQLite
    export_tables_to_sqlite(
        pg_user=db_params["user"],
        pg_pass=db_params["password"],
        pg_host=db_params["host"],
        pg_db=db_params["database"],
        sqlite_file="maiven.db",
    )

    # 6. Run the active‐policies query on SQLite and print results
    print("\n=== Active Policies Matching Customer Jurisdiction (Last 100 Days) ===\n")
    query_active_policies("maiven.db")


if __name__ == "__main__":
    main()
