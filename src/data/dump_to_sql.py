import gc
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from minio import Minio


def extract_data_from_minio():
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket = "nyc-taxi-data"
    folder_path = "../../data/raw/"

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    objects = client.list_objects(bucket)
    for obj in objects:
        file_name = obj.object_name
        file_path = os.path.join(folder_path, file_name)
        client.fget_object(bucket, file_name, file_path)
        print(f"Téléchargé {file_name} de Minio vers {file_path}")


def write_data_postgres(dataframe: pd.DataFrame, table_name: str) -> bool:
    """
    Dumps a Dataframe to the DBMS engine

    Parameters:
        - dataframe (pd.DataFrame) : The dataframe to dump into the DBMS engine
        - table_name (str) : The name of the table to dump the data into

    Returns:
        - bool : True if the connection to the DBMS and the dump to the DBMS is successful, False if either
        execution is failed
    """
    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    try:
        engine = create_engine(db_config["database_url"])
        with engine.connect() as connection:
            success = True
            print(f"Connection successful! Processing parquet file into {table_name}")
            dataframe.to_sql(table_name, connection, index=False, if_exists='replace')

    except Exception as e:
        success = False
        print(f"Error connecting to the database: {e}")
        return success

    return success


def clean_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Take a Dataframe and rewrite it columns into a lowercase format.
    Parameters:
        - dataframe (pd.DataFrame) : The dataframe columns to change

    Returns:
        - pd.Dataframe : The changed Dataframe into lowercase format
    """
    dataframe.columns = map(str.lower, dataframe.columns)
    return dataframe


def create_and_populate_dimension_tables(engine):
    query_date = """
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id SERIAL PRIMARY KEY,
        date DATE,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        weekday INTEGER
    );
    INSERT INTO dim_date (date, year, month, day, weekday)
    SELECT DISTINCT
        DATE(pickup_datetime) AS date,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        EXTRACT(DAY FROM pickup_datetime) AS day,
        EXTRACT(DOW FROM pickup_datetime) AS weekday
    FROM nyc_raw;
    """
    query_location = """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id SERIAL PRIMARY KEY,
        latitude FLOAT,
        longitude FLOAT,
        location_name VARCHAR(255)
    );
    INSERT INTO dim_location (latitude, longitude, location_name)
    SELECT DISTINCT
        pickup_latitude AS latitude,
        pickup_longitude AS longitude,
        'Pickup' AS location_name
    FROM nyc_raw
    UNION
    SELECT DISTINCT
        dropoff_latitude AS latitude,
        dropoff_longitude AS longitude,
        'Dropoff' AS location_name
    FROM nyc_raw;
    """
    query_passenger = """
    CREATE TABLE IF NOT EXISTS dim_passenger (
        passenger_id SERIAL PRIMARY KEY,
        passenger_count INTEGER
    );
    INSERT INTO dim_passenger (passenger_count)
    SELECT DISTINCT passenger_count
    FROM nyc_raw;
    """

    with engine.connect() as connection:
        connection.execute(query_date)
        connection.execute(query_location)
        connection.execute(query_passenger)


def create_and_populate_fact_table(engine):
    query_fact = """
    CREATE TABLE IF NOT EXISTS fact_yellow_taxi (
        trip_id SERIAL PRIMARY KEY,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance FLOAT,
        fare_amount FLOAT,
        tip_amount FLOAT,
        total_amount FLOAT,
        pickup_location_id INTEGER,
        dropoff_location_id INTEGER,
        date_id INTEGER,
        passenger_id INTEGER
    );

    INSERT INTO fact_yellow_taxi (
        pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
        fare_amount, tip_amount, total_amount, pickup_location_id,
        dropoff_location_id, date_id, passenger_id
    )
    SELECT
        ytd.pickup_datetime,
        ytd.dropoff_datetime,
        ytd.passenger_count,
        ytd.trip_distance,
        ytd.fare_amount,
        ytd.tip_amount,
        ytd.total_amount,
        dl_pickup.location_id,
        dl_dropoff.location_id,
        dd.date_id,
        dp.passenger_id
    FROM
        nyc_raw ytd
        JOIN dim_location dl_pickup ON ytd.pickup_latitude = dl_pickup.latitude AND ytd.pickup_longitude = dl_pickup.longitude
        JOIN dim_location dl_dropoff ON ytd.dropoff_latitude = dl_dropoff.latitude AND ytd.dropoff_longitude = dl_dropoff.longitude
        JOIN dim_date dd ON DATE(ytd.pickup_datetime) = dd.date
        JOIN dim_passenger dp ON ytd.passenger_count = dp.passenger_count;
    """
    with engine.connect() as connection:
        connection.execute(query_fact)


def main() -> None:
    extract_data_from_minio()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, '..', '..', 'data', 'raw')

    parquet_files = [f for f in os.listdir(folder_path) if
                     f.lower().endswith('.parquet') and os.path.isfile(os.path.join(folder_path, f))]

    for parquet_file in parquet_files:
        parquet_df: pd.DataFrame = pd.read_parquet(os.path.join(folder_path, parquet_file), engine='pyarrow')
        clean_column_name(parquet_df)
        if not write_data_postgres(parquet_df, 'nyc_raw'):
            del parquet_df
            gc.collect()
            return

        del parquet_df
        gc.collect()

    db_config = {
        "dbms_engine": "postgresql",
        "dbms_username": "postgres",
        "dbms_password": "admin",
        "dbms_ip": "localhost",
        "dbms_port": "15432",
        "dbms_database": "nyc_warehouse"
    }

    db_config["database_url"] = (
        f"{db_config['dbms_engine']}://{db_config['dbms_username']}:{db_config['dbms_password']}@"
        f"{db_config['dbms_ip']}:{db_config['dbms_port']}/{db_config['dbms_database']}"
    )
    engine = create_engine(db_config["database_url"])

    create_and_populate_dimension_tables(engine)
    create_and_populate_fact_table(engine)


if __name__ == '__main__':
    sys.exit(main())
