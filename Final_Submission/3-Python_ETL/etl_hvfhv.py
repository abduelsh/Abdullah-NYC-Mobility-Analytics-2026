
from pathlib import Path
import logging
import os
import getpass

import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values


# ==========================================
# 1. CONFIGURATION
# ==========================================

SOURCE_FILE = Path(
    r"C:\Users\abdue\Downloads\Reference_Data\fhvhv_tripdata_2025-12.parquet"
)

PROJECT_DIR = Path.home() / "Documents" / "NYC_Mobility_Project"
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

BATCH_SIZE = 50_000
TABLE_NAME = "stg_hvfhv_trips"


# ==========================================
# 2. LOGGING
# ==========================================

logging.basicConfig(
    filename=LOG_DIR / "etl_hvfhv.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


# ==========================================
# 3. SOURCE COLUMNS
# ==========================================

COLUMNS = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag",
    "cbd_congestion_fee",
]

NUMERIC_COLUMNS = [
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "cbd_congestion_fee",
]


# ==========================================
# 4. DATABASE CONNECTION
# ==========================================

def connect_db():

    password = os.environ.get("PGPASSWORD")

    if not password:
        password = getpass.getpass(
            "PostgreSQL password for postgres: "
        )

    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="qeema_nyc",
        user="postgres",
        password=password,
    )


# ==========================================
# 5. CREATE STAGING TABLE
# ==========================================

def create_table(conn):

    definitions = []

    for col in COLUMNS:

        if col in {
            "request_datetime",
            "on_scene_datetime",
            "pickup_datetime",
            "dropoff_datetime",
        }:
            sql_type = "TIMESTAMP"

        elif col in {
            "PULocationID",
            "DOLocationID",
            "trip_time",
        }:
            sql_type = "INTEGER"

        elif col in NUMERIC_COLUMNS:
            sql_type = "DOUBLE PRECISION"

        else:
            sql_type = "TEXT"

        definitions.append(f'"{col}" {sql_type}')

    definitions += [
        '"source_file" TEXT NOT NULL',
        '"source_row_group" INTEGER NOT NULL',
        '"quality_flags" TEXT',
    ]

    with conn.cursor() as cur:

        cur.execute(
            f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} '
            f'({", ".join(definitions)})'
        )

    conn.commit()


# ==========================================
# 6. MAIN ETL PROCESS
# ==========================================

def main():

    if not SOURCE_FILE.is_file():
        raise FileNotFoundError(
            f"Parquet file not found: {SOURCE_FILE}"
        )

    parquet = pq.ParquetFile(SOURCE_FILE)

    logging.info(
        "Started file=%s rows=%s row_groups=%s",
        SOURCE_FILE.name,
        parquet.metadata.num_rows,
        parquet.num_row_groups,
    )

    print("Connecting to PostgreSQL...")

    conn = connect_db()

    total_read = 0
    total_loaded = 0
    total_rejected = 0

    try:

        create_table(conn)

        # Remove this file's previous partial load.
        # This makes a complete rerun safe for this source file.

        with conn.cursor() as cur:

            cur.execute(
                f"DELETE FROM {TABLE_NAME} WHERE source_file = %s",
                (SOURCE_FILE.name,)
            )

        conn.commit()

        print("Previous partial load cleared.")
        print("Starting ETL processing...")

        # Process one Parquet row group at a time.

        for group_index in range(parquet.num_row_groups):

            arrow_table = parquet.read_row_group(
                group_index,
                columns=COLUMNS
            )

            for start in range(
                0,
                arrow_table.num_rows,
                BATCH_SIZE
            ):

                df = arrow_table.slice(
                    start,
                    BATCH_SIZE
                ).to_pandas()

                total_read += len(df)

                # Convert numeric columns.

                for col in NUMERIC_COLUMNS:

                    df[col] = pd.to_numeric(
                        df[col],
                        errors="coerce"
                    )

                # Calculate actual trip duration.

                duration = (
                    df["dropoff_datetime"]
                    - df["pickup_datetime"]
                ).dt.total_seconds()

                # Create quality flags.

                flags = pd.Series(
                    "",
                    index=df.index,
                    dtype="object"
                )

                flags = flags.mask(
                    df["pickup_datetime"].isna()
                    | df["dropoff_datetime"].isna(),
                    flags + "missing_timestamp;"
                )

                flags = flags.mask(
                    duration.isna() | (duration <= 0),
                    flags + "invalid_duration;"
                )

                flags = flags.mask(
                    df["trip_time"].isna()
                    | (df["trip_time"] <= 0),
                    flags + "invalid_trip_time;"
                )

                flags = flags.mask(
                    df["trip_miles"].isna()
                    | (df["trip_miles"] < 0),
                    flags + "invalid_distance;"
                )

                flags = flags.mask(
                    df["PULocationID"].isna()
                    | df["DOLocationID"].isna(),
                    flags + "missing_location;"
                )

                # Negative fares are flagged, not automatically rejected.

                flags = flags.mask(
                    df["base_passenger_fare"] < 0,
                    flags + "negative_base_fare;"
                )

                # Identify records that must be rejected.

                reject = (
                    df["pickup_datetime"].isna()
                    | df["dropoff_datetime"].isna()
                    | duration.isna()
                    | (duration <= 0)
                    | df["trip_time"].isna()
                    | (df["trip_time"] <= 0)
                    | df["trip_miles"].isna()
                    | (df["trip_miles"] < 0)
                    | df["PULocationID"].isna()
                    | df["DOLocationID"].isna()
                )

                rejected_count = int(reject.sum())

                total_rejected += rejected_count

                clean = df.loc[~reject].copy()

                clean["source_file"] = SOURCE_FILE.name

                clean["source_row_group"] = group_index

                clean["quality_flags"] = (
                    flags.loc[~reject].replace("", None)
                )

                if clean.empty:
                    continue

                # Prepare database insertion.

                insert_columns = COLUMNS + [
                    "source_file",
                    "source_row_group",
                    "quality_flags",
                ]

                rows = [
                    tuple(
                        None if pd.isna(v) else v
                        for v in row
                    )
                    for row in clean[insert_columns].itertuples(
                        index=False,
                        name=None
                    )
                ]

                quoted_cols = ", ".join(
                    f'"{c}"' for c in insert_columns
                )

                values_sql = (
                    f"INSERT INTO {TABLE_NAME} ({quoted_cols}) "
                    f"VALUES %s"
                )

                # Faster bulk insertion.

                with conn.cursor() as cur:

                    execute_values(
                        cur,
                        values_sql,
                        rows,
                        page_size=5000
                    )

                conn.commit()

                total_loaded += len(rows)

                logging.info(
                    "Group=%s batch_start=%s read=%s loaded=%s rejected=%s",
                    group_index,
                    start,
                    len(df),
                    len(clean),
                    rejected_count,
                )

                print(
                    f"Processed: {total_read:,} | "
                    f"Loaded: {total_loaded:,} | "
                    f"Rejected: {total_rejected:,}"
                )

        # Final summary.

        logging.info(
            "Finished file=%s read=%s loaded=%s rejected=%s",
            SOURCE_FILE.name,
            total_read,
            total_loaded,
            total_rejected,
        )

        print("\nETL COMPLETED SUCCESSFULLY")
        print("Rows read:", total_read)
        print("Rows loaded:", total_loaded)
        print("Rows rejected:", total_rejected)
        print("Log file:", LOG_DIR / "etl_hvfhv.log")

    except Exception:

        conn.rollback()

        logging.exception("ETL failed")

        raise

    finally:

        conn.close()


# ==========================================
# 7. RUN ETL
# ==========================================

if __name__ == "__main__":

    main()
