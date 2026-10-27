import os
import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = Path("/opt/airflow/project")
STAGING_DIR = PROJECT_ROOT / "data" / "airflow_staging"

sys.path.insert(0, str(PROJECT_ROOT))



def extract_data():
    """Extract the primary sales dataset to a staging file."""
    os.chdir(PROJECT_ROOT)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    from airflow.models import Variable

    from src.datamart.ingestion.ecommerce_reader import (
        read_ecommerce_data,
    )

    input_path = Variable.get("etl_input_path")
    batch_size = Variable.get(
        "etl_batch_size", default_var="5000"
    )

    dataframe = read_ecommerce_data(
        file_path=input_path,
        batch_size=int(batch_size),
    )

    if dataframe.empty:
        raise ValueError("The source dataset is empty.")

    output_path = STAGING_DIR / "extracted_sales.pkl"
    dataframe.to_pickle(output_path)

    print(f"Source file: {input_path}")
    print(f"Extracted records: {len(dataframe):,}")
    print(f"Batch size: {batch_size}")
    print(f"Staging file: {output_path}")


def transform_data():
    """Clean, validate and calculate sales metrics."""
    os.chdir(PROJECT_ROOT)

    from src.datamart.quality.validators import validate_sales
    from src.datamart.transformations.deduplicate import (
        remove_exact_duplicates,
    )
    from src.datamart.transformations.normalize import normalize_sales
    from src.datamart.transformations.sales_metrics import (
        aggregate_daily_product_revenue,
        calculate_sales_metrics,
    )

    source_path = STAGING_DIR / "extracted_sales.pkl"

    if not source_path.exists():
        raise FileNotFoundError(
            f"Extraction output not found: {source_path}"
        )

    ecommerce = pd.read_pickle(source_path)

    deduplicated = remove_exact_duplicates(ecommerce)
    normalized = normalize_sales(deduplicated)
    validated = validate_sales(normalized)
    metrics = calculate_sales_metrics(validated)
    daily_metrics = aggregate_daily_product_revenue(metrics)

    if metrics.empty or daily_metrics.empty:
        raise ValueError("The transformation produced empty output.")

    metrics.to_pickle(STAGING_DIR / "transformed_sales.pkl")
    daily_metrics.to_pickle(STAGING_DIR / "daily_product_revenue.pkl")

    print(f"Processed sales records: {len(metrics):,}")
    print(f"Daily product revenue rows: {len(daily_metrics):,}")
    print(f"Valid records: {metrics['is_valid'].sum():,}")
    print(
        "Rejected records: "
        f"{(metrics['is_valid'] == False).sum():,}"
    )



def load_data():
    """Load both datasets atomically into the PostgreSQL warehouse."""
    os.chdir(PROJECT_ROOT)

    from airflow.sdk.bases.hook import BaseHook
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    from src.datamart.load.postgres_loader import clear_target_tables, load_dataframe

    sales_path = STAGING_DIR / "transformed_sales.pkl"
    daily_path = STAGING_DIR / "daily_product_revenue.pkl"

    for path in (sales_path, daily_path):
        if not path.exists():
            raise FileNotFoundError(
                f"Transformation output not found: {path}"
            )

    metrics = pd.read_pickle(sales_path)
    daily_metrics = pd.read_pickle(daily_path)

    if metrics.empty or daily_metrics.empty:
        raise ValueError("Cannot load empty datasets into the warehouse.")

    airflow_connection = BaseHook.get_connection("warehouse_postgres")

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        username=airflow_connection.login,
        password=airflow_connection.password,
        host=airflow_connection.host,
        port=airflow_connection.port,
        database=airflow_connection.schema,
    )

    engine = create_engine(database_url, pool_pre_ping=True)

    try:
        with engine.begin() as connection:
            clear_target_tables(connection)

            sales_rows = load_dataframe(
                metrics,
                table_name="sales_records",
                if_exists="append",
                engine=connection,
            )

            daily_rows = load_dataframe(
                daily_metrics,
                table_name="daily_product_revenue",
                if_exists="append",
                engine=connection,
            )

        print(f"Sales records loaded: {sales_rows:,}")
        print(f"Daily product revenue rows loaded: {daily_rows:,}")
        print("Warehouse load completed successfully.")

    finally:
        engine.dispose()


with DAG(
    dag_id="etl_datamart",
    description="Daily sales ETL: extraction, transformation and load",
    start_date=pendulum.datetime(2026, 10, 27, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["etl", "postgresql", "datamart"],
) as dag:

    extract_task = PythonOperator(
    task_id="extract_data",
    python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task
