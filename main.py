#!/usr/bin/env python3

import psycopg2
import pandas as pd
from sqlalchemy import create_engine


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
    df.to_sql(table_name, engine, if_exists='replace', index=False)


def main() -> None:
    # Database connection parameters
    db_params = {
        "host": "localhost",
        "database": "maiven",
        "user": "michaelsigamani",
        "password": "policy"
    }

    # Create the database if it doesn’t already exist
    conn = psycopg2.connect(
        host=db_params["host"],
        database=db_params["database"],
        user=db_params["user"],
        password=db_params["password"]
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    # If you need to create the database from scratch, uncomment:
    # cur.execute("CREATE DATABASE maiven")
    conn.commit()
    cur.close()
    conn.close()

    # Create SQLAlchemy engine for PostgreSQL
    engine = create_engine(
        f'postgresql://{db_params["user"]}:{db_params["password"]}@'
        f'{db_params["host"]}/{db_params["database"]}'
    )

    # Define CSV file paths
    csv_files = {
        "company": "data/company_data.csv",
        "policy": "data/random_policies.csv",
    }

    # Basic ETL: clean and load each table
    for table_name, file_path in csv_files.items():
        print(f"Contents of '{table_name}' CSV file:")
        df = clean_data(file_path)
        print(df.head(2))
        print()
        load_to_db(df, table_name, engine)
        print(f"Table '{table_name}' loaded successfully.\n")


if __name__ == "__main__":
    main()
