
import os
import sys
import getpass
import logging
import traceback
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# CONFIGURATION
# ============================================================

SOURCE_DIR = Path(r"C:\Users\abdue\Downloads\Reference_Data")
PROJECT_DIR = Path(r"C:\Users\abdue\Documents\NYC_Mobility_Project")
LOG_DIR = PROJECT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "etl_hvfhv.log"

BATCH_SIZE = 10000

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "qeema_nyc",
    "user": "postgres",
}

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

TIMESTAMP_COLUMNS = [
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
]

NUMERIC_COLUMNS = [
    "trip_miles",
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

LOCATION_COLUMNS = ["PULocationID", "DOLocationID"]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)


# ============================================================
# DATABASE CONNECTION
# ============================================================

def connect_db():
    password = os.environ.get("PGPASSWORD")

    if not password:
        password = getpass.getpass("Enter PostgreSQL password: ")

    config = DB_CONFIG.copy()
    config["password"] = password

    return psycopg2.connect(**config)


# ============================================================
# DATA CLEANING
# ============================================================

def prepare_dataframe(df):
    df = df.copy()

    for col in TIMESTAMP_COLUMNS:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["trip_time"] = pd.to_numeric(
        df["trip_time"], errors="coerce"
    )

    for col in LOCATION_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    flags = pd.Series("", index=df.index, dtype="object")

    flags.loc[df["base_passenger_fare"] < 0] += "negative_base_fare;"
    flags.loc[df["trip_miles"] == 0] += "zero_distance;"

    calculated_duration = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds()

    flags.loc[
        (calculated_duration - df["trip_time"]).abs() > 300
    ] += "duration_mismatch;"

    invalid = (
        df["pickup_datetime"].isna()
        | df["dropoff_datetime"].isna()
        | (df["dropoff_datetime"] <= df["pickup_datetime"])
        | df["trip_time"].isna()
        | (df["trip_time"] <= 0)
        | df["trip_miles"].isna()
        | (df["trip_miles"] < 0)
        | df["PULocationID"].isna()
        | df["DOLocationID"].isna()
    )

    accepted = df.loc[~invalid].copy()
    accepted["quality_flags"] = flags.loc[~invalid]

    rejected = int(invalid.sum())

    accepted = accepted.astype(object)
    accepted = accepted.where(pd.notna(accepted), None)

    return accepted, rejected


# ============================================================
# STAGING LOAD
# ============================================================

def load_staging(connection, parquet_path, source_name):
    logging.info("Starting staging load: %s", source_name)

    parquet_file = pq.ParquetFile(parquet_path)

    total_read = 0
    total_loaded = 0
    total_rejected = 0

    cursor = connection.cursor()

    # Clear any partial staging data from an earlier attempt.
    cursor.execute(
        """
        DELETE FROM public.stg_hvfhv_trips
        WHERE source_file = %s
        """,
        (source_name,),
    )
    connection.commit()

    insert_sql = f"""
        INSERT INTO public.stg_hvfhv_trips (
            {", ".join('"' + col + '"' for col in COLUMNS)},
            quality_flags,
            source_file,
            source_row_group
        )
        VALUES %s
    """

    for row_group_index in range(parquet_file.num_row_groups):

        batches = parquet_file.iter_batches(
            batch_size=BATCH_SIZE,
            row_groups=[row_group_index],
            columns=COLUMNS,
        )

        for batch in batches:
            df = batch.to_pandas()

            if df.empty:
                continue

            total_read += len(df)

            accepted, rejected = prepare_dataframe(df)
            total_rejected += rejected

            if not accepted.empty:
                accepted["source_file"] = source_name
                accepted["source_row_group"] = row_group_index

                records = list(
                    accepted[
                        COLUMNS
                        + [
                            "quality_flags",
                            "source_file",
                            "source_row_group",
                        ]
                    ].itertuples(index=False, name=None)
                )

                try:
                    execute_values(
                        cursor,
                        insert_sql,
                        records,
                        page_size=1000,
                    )
                    connection.commit()

                except Exception:
                    connection.rollback()
                    raise

                total_loaded += len(records)

            print(
                f"{source_name} | "
                f"Read {total_read:,} | "
                f"Loaded {total_loaded:,} | "
                f"Rejected {total_rejected:,}",
                flush=True,
            )

    cursor.close()

    logging.info(
        "STAGING COMPLETE: read=%s loaded=%s rejected=%s",
        total_read,
        total_loaded,
        total_rejected,
    )

    return total_read, total_loaded, total_rejected


# ============================================================
# DIMENSION UPDATES
# ============================================================

def update_dimensions(connection, source_name):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO public.dim_license (hvfhs_license_num)
            SELECT DISTINCT hvfhs_license_num
            FROM public.stg_hvfhv_trips
            WHERE source_file = %s
              AND hvfhs_license_num IS NOT NULL
            ON CONFLICT (hvfhs_license_num) DO NOTHING
            """,
            (source_name,),
        )

        cursor.execute(
            """
            INSERT INTO public.dim_provider (dispatching_base_num)
            SELECT DISTINCT dispatching_base_num
            FROM public.stg_hvfhv_trips
            WHERE source_file = %s
              AND dispatching_base_num IS NOT NULL
            ON CONFLICT (dispatching_base_num) DO NOTHING
            """,
            (source_name,),
        )

        cursor.execute(
            """
            INSERT INTO public.dim_date (
                date_key, full_date, year, quarter, month,
                month_name, day_of_month, day_of_week,
                day_name, is_weekend
            )
            SELECT DISTINCT
                TO_CHAR(d.dt, 'YYYYMMDD')::INTEGER,
                d.dt::DATE,
                EXTRACT(YEAR FROM d.dt)::INTEGER,
                EXTRACT(QUARTER FROM d.dt)::INTEGER,
                EXTRACT(MONTH FROM d.dt)::INTEGER,
                TRIM(TO_CHAR(d.dt, 'Month')),
                EXTRACT(DAY FROM d.dt)::INTEGER,
                EXTRACT(DOW FROM d.dt)::INTEGER,
                TRIM(TO_CHAR(d.dt, 'Day')),
                EXTRACT(DOW FROM d.dt) IN (0, 6)
            FROM (
                SELECT pickup_datetime::DATE AS dt
                FROM public.stg_hvfhv_trips
                WHERE source_file = %s

                UNION

                SELECT dropoff_datetime::DATE AS dt
                FROM public.stg_hvfhv_trips
                WHERE source_file = %s
            ) d
            WHERE d.dt IS NOT NULL
            ON CONFLICT (date_key) DO NOTHING
            """,
            (source_name, source_name),
        )

        connection.commit()
        logging.info("Dimensions updated for %s", source_name)

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()


# ============================================================
# FACT LOAD
# ============================================================

def load_fact(connection, source_name):
    cursor = connection.cursor()

    try:
        # Remove any earlier fact rows for this file.
        cursor.execute(
            """
            DELETE FROM public.fact_trips
            WHERE source_file = %s
            """,
            (source_name,),
        )
        connection.commit()

        cursor.execute(
            """
            INSERT INTO public.fact_trips (
                pickup_date_key,
                dropoff_date_key,
                pickup_location_key,
                dropoff_location_key,
                license_key,
                provider_key,
                pickup_datetime,
                dropoff_datetime,
                trip_miles,
                trip_time,
                base_passenger_fare,
                tolls,
                tips,
                driver_pay,
                congestion_surcharge,
                source_file
            )
            SELECT
                TO_CHAR(s.pickup_datetime, 'YYYYMMDD')::INTEGER,
                TO_CHAR(s.dropoff_datetime, 'YYYYMMDD')::INTEGER,
                pu.location_key,
                do_loc.location_key,
                lic.license_key,
                prov.provider_key,
                s.pickup_datetime,
                s.dropoff_datetime,
                s.trip_miles,
                s.trip_time,
                s.base_passenger_fare,
                s.tolls,
                s.tips,
                s.driver_pay,
                s.congestion_surcharge,
                s.source_file
            FROM public.stg_hvfhv_trips s
            INNER JOIN public.dim_location pu
                ON s."PULocationID" = pu.location_id
            INNER JOIN public.dim_location do_loc
                ON s."DOLocationID" = do_loc.location_id
            INNER JOIN public.dim_license lic
                ON s.hvfhs_license_num = lic.hvfhs_license_num
            LEFT JOIN public.dim_provider prov
                ON s.dispatching_base_num = prov.dispatching_base_num
            INNER JOIN public.dim_date pickup_date
                ON pickup_date.date_key =
                   TO_CHAR(s.pickup_datetime, 'YYYYMMDD')::INTEGER
            INNER JOIN public.dim_date dropoff_date
                ON dropoff_date.date_key =
                   TO_CHAR(s.dropoff_datetime, 'YYYYMMDD')::INTEGER
            WHERE s.source_file = %s
            """,
            (source_name,),
        )

        inserted = cursor.rowcount
        connection.commit()

        logging.info(
            "Fact load completed for %s: %s rows",
            source_name,
            inserted,
        )

        return inserted

    except Exception:
        connection.rollback()
        logging.exception("Fact load failed for %s", source_name)
        raise

    finally:
        cursor.close()


# ============================================================
# COUNTS AND COMPLETION TRACKING
# ============================================================

def get_counts(connection, source_name):
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM public.stg_hvfhv_trips
        WHERE source_file = %s
        """,
        (source_name,),
    )
    staging_count = cursor.fetchone()[0]

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM public.fact_trips
        WHERE source_file = %s
        """,
        (source_name,),
    )
    fact_count = cursor.fetchone()[0]

    cursor.close()

    return staging_count, fact_count


def is_completed(connection, source_name, expected_rows):
    staging_count, fact_count = get_counts(
        connection, source_name
    )

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT expected_source_rows, staging_rows, fact_rows
        FROM public.etl_file_status
        WHERE source_file = %s
          AND status = 'COMPLETED'
        """,
        (source_name,),
    )

    status = cursor.fetchone()
    cursor.close()

    return (
        status is not None
        and status[0] == expected_rows
        and status[1] == staging_count
        and status[2] == fact_count
        and staging_count == fact_count
    )


def mark_completed(
    connection,
    source_name,
    expected_rows,
    staging_rows,
    fact_rows,
):
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO public.etl_file_status (
                source_file,
                expected_source_rows,
                staging_rows,
                fact_rows,
                status,
                completed_at
            )
            VALUES (%s, %s, %s, %s, 'COMPLETED', CURRENT_TIMESTAMP)
            ON CONFLICT (source_file) DO UPDATE SET
                expected_source_rows = EXCLUDED.expected_source_rows,
                staging_rows = EXCLUDED.staging_rows,
                fact_rows = EXCLUDED.fact_rows,
                status = 'COMPLETED',
                completed_at = CURRENT_TIMESTAMP
            """,
            (
                source_name,
                expected_rows,
                staging_rows,
                fact_rows,
            ),
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()


# ============================================================
# PROCESS ONE MONTH
# ============================================================

def process_file(connection, parquet_path):
    source_name = parquet_path.name

    parquet_file = pq.ParquetFile(parquet_path)
    expected_rows = parquet_file.metadata.num_rows

    print("\n" + "=" * 65)
    print("PROCESSING:", source_name)
    print(f"Expected source rows: {expected_rows:,}")
    print("=" * 65)

    if is_completed(connection, source_name, expected_rows):
        print("SKIPPED: This file is already completed and verified.")
        logging.info("Skipping completed file: %s", source_name)
        return

    # Invalidate any old completion record before reprocessing.
    cursor = connection.cursor()
    cursor.execute(
        """
        DELETE FROM public.etl_file_status
        WHERE source_file = %s
        """,
        (source_name,),
    )
    connection.commit()
    cursor.close()

    print("\nSTEP 1: Loading staging table...")

    read_count, loaded_count, rejected_count = load_staging(
        connection,
        parquet_path,
        source_name,
    )

    if read_count != expected_rows:
        raise RuntimeError(
            f"Source read mismatch: expected {expected_rows}, "
            f"read {read_count}"
        )

    print("\nSTEP 2: Updating dimensions...")
    update_dimensions(connection, source_name)

    print("\nSTEP 3: Loading fact table...")
    inserted = load_fact(connection, source_name)

    staging_count, fact_count = get_counts(
        connection, source_name
    )

    print("\nMONTH VALIDATION")
    print(f"Source rows:   {read_count:,}")
    print(f"Rejected:      {rejected_count:,}")
    print(f"Staging rows:  {staging_count:,}")
    print(f"Fact rows:     {fact_count:,}")

    if staging_count != fact_count or fact_count != inserted:
        raise RuntimeError(
            f"Count mismatch for {source_name}: "
            f"staging={staging_count}, "
            f"fact={fact_count}, inserted={inserted}"
        )

    mark_completed(
        connection,
        source_name,
        expected_rows,
        staging_count,
        fact_count,
    )

    print("\nCOMPLETED AND VERIFIED:", source_name)

    logging.info(
        "COMPLETED %s | source=%s staging=%s fact=%s rejected=%s",
        source_name,
        read_count,
        staging_count,
        fact_count,
        rejected_count,
    )


# ============================================================
# MAIN ETL
# ============================================================

def main():
    print("=" * 65)
    print("NYC MOBILITY ANALYTICS - FULL ETL")
    print("=" * 65)

    connection = None

    try:
        connection = connect_db()
        connection.autocommit = False

        files = sorted(
            SOURCE_DIR.glob("fhvhv_tripdata_*.parquet")
        )

        if not files:
            raise FileNotFoundError(
                f"No Parquet files found in {SOURCE_DIR}"
            )

        print(f"Monthly files found: {len(files)}")

        for parquet_path in files:
            try:
                process_file(connection, parquet_path)

            except Exception:
                connection.rollback()

                logging.exception(
                    "ETL STOPPED on file: %s",
                    parquet_path.name,
                )

                print("\nETL STOPPED.")
                print("Failed file:", parquet_path.name)
                print("See the log for the full error:")
                print(LOG_FILE)

                raise

        print("\n" + "=" * 65)
        print("ALL MONTHLY FILES PROCESSED")
        print("=" * 65)

        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*) AS completed_files,
                SUM(staging_rows) AS total_staging_rows,
                SUM(fact_rows) AS total_fact_rows
            FROM public.etl_file_status
            WHERE status = 'COMPLETED'
            """
        )

        result = cursor.fetchone()
        cursor.close()

        print("Completed files:", result[0])
        print("Total staging rows:", f"{result[1] or 0:,}")
        print("Total fact rows:", f"{result[2] or 0:,}")

    except Exception as error:
        print("\nFATAL ERROR:")
        print(repr(error))
        logging.exception("Full ETL failed")

    finally:
        if connection is not None:
            connection.close()

        print("\nETL PROCESS FINISHED")
        print("Log file:", LOG_FILE)


if __name__ == "__main__":
    main()
