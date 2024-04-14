import gc
import os
import sys

import pandas as pd
from sqlalchemy import create_engine


def write_data_postgres(dataframe: pd.DataFrame) -> bool:

    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse",
        "dbms_table": "nyc_raw"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect() as connection:
            print("Connection successful! Processing parquet file")
            dataframe.to_sql(db_config["dbms_table"], connection, index=False, if_exists='append')

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return False

    return True


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:

    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        parquet_df = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')

        clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df):
            return

        del parquet_df
        gc.collect()


if __name__ == '__main__':
    sys.exit(main())
