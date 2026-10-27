from src.datamart.ingestion.ecommerce_reader import read_ecommerce_data
from src.datamart.ingestion.online_retail_reader import read_online_retail_data
from src.datamart.transformations.deduplicate import remove_exact_duplicates
from src.datamart.transformations.normalize import normalize_sales
from src.datamart.quality.validators import validate_sales
from src.datamart.transformations.sales_metrics import calculate_sales_metrics
from src.datamart.transformations.sales_metrics import aggregate_daily_product_revenue
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from src.datamart.load.postgres_loader import load_dataframe


connection = BaseHook.get_connection("warehouse_postgres")
engine = create_engine(connection.get_uri(), pool_pre_ping=True)


def explore_dataset(name, dataframe):
    """Display a basic overview of a dataset."""
    print("\n" + "-" * 50)
    print(f"Dataset: {name}")
    print("-" * 50)
    print(f"Rows: {len(dataframe)}")
    print(f"Columns: {len(dataframe.columns)}")
    print("\nColumn names:")
    print(dataframe.columns.tolist())
    print("\nData types:")
    print(dataframe.dtypes)
    print("\nMissing values per column:")
    print(dataframe.isna().sum())
    print(f"\nExact duplicate rows: {dataframe.duplicated().sum()}")
    print("\nFirst five rows:")
    print(dataframe.head())


def compare_datasets(ecommerce, online_retail):
    """Compare the business data in both source datasets."""
    first = ecommerce.copy()
    second = online_retail.copy()

    if "index" in second.columns:
        second = second.drop(columns=["index"])

    print("\n" + "-" * 50)
    print("Dataset comparison")
    print("-" * 50)

    same_columns = first.columns.tolist() == second.columns.tolist()
    same_rows = len(first) == len(second)
    same_content = first.equals(second)

    print(f"\nSame business columns: {same_columns}")
    print(f"Same number of rows: {same_rows}")
    print(f"Same content and order: {same_content}")

    print("\nExact duplicate rows:")
    print(f"data.csv: {first.duplicated().sum()}")
    print(f"online_retail.csv: {second.duplicated().sum()}")

    first_counts = first.value_counts(dropna=False)
    second_counts = second.value_counts(dropna=False)
    all_rows = first_counts.index.union(second_counts.index)

    first_counts = first_counts.reindex(all_rows, fill_value=0)
    second_counts = second_counts.reindex(all_rows, fill_value=0)

    only_first = (first_counts - second_counts).clip(lower=0).sum()
    only_second = (second_counts - first_counts).clip(lower=0).sum()

    print("\nUnmatched row occurrences:")
    print(f"Excess occurrences in data.csv: {only_first}")
    print(f"Excess occurrences in online_retail.csv: {only_second}")


def main():
    # Read both source datasets.
    ecommerce = read_ecommerce_data()
    online_retail = read_online_retail_data()

    # Explore the original datasets.
    explore_dataset("data.csv", ecommerce)
    explore_dataset("online_retail.csv", online_retail)

    # Compare the sources before processing the primary dataset.
    compare_datasets(ecommerce, online_retail)

    # Remove exact duplicate rows from the primary dataset.
    deduplicated = remove_exact_duplicates(ecommerce)

    print("\n" + "-" * 50)
    print("Deduplication summary")
    print("-" * 50)
    print(f"Original rows: {len(ecommerce):,}")
    print(f"Rows after deduplication: {len(deduplicated):,}")
    print("Removed duplicate rows: "f"{len(ecommerce) - len(deduplicated):,}")

    # Normalize the deduplicated records.
    normalized = normalize_sales(deduplicated)

    print("\nNormalization check")
    print(normalized.dtypes)
    print("Invalid dates: "f"{normalized['InvoiceDate'].isna().sum():,}")

    # Validate normalized records.
    validated = validate_sales(normalized)

    print("\nValidation summary")
    print(validated["record_type"].value_counts())
    print(f"\nValid records: {validated['is_valid'].sum():,}")
    print("Rejected records: "f"{(validated['is_valid'] == False).sum():,}")
    print("Missing customer IDs: "f"{validated['customer_missing'].sum():,}")
    print("Missing descriptions: "f"{validated['description_missing'].sum():,}")

    print("\nRejection reasons:")
    print(validated.loc[validated["is_valid"] == False, "rejection_reason",].value_counts())

    # Calculate revenue metrics.
    metrics = calculate_sales_metrics(validated)

    # Separate valid and rejected records.
    valid_records = metrics.loc[metrics["is_valid"]].copy()
    rejected_records = metrics.loc[metrics["is_valid"] == False].copy()

    print("\nRevenue metrics")
    print("Gross revenue: "f"{valid_records['gross_revenue'].sum():,.2f}")
    print("Return and adjustment amount: "f"{valid_records['return_adjustment_amount'].sum():,.2f}")
    print("Net revenue: "f"{valid_records['revenue'].sum():,.2f}")

    print("\nRecords for processing")
    print(f"Valid records: {len(valid_records):,}")
    print(f"Rejected records: {len(rejected_records):,}")


    # Aggregate valid revenue by product and UTC day.
    daily_metrics = aggregate_daily_product_revenue(metrics)

    print("\nDaily product revenue")
    print(f"Aggregated rows: {len(daily_metrics):,}")
    print(daily_metrics.head(10))

    
    # Load processed records into PostgreSQL.
    sales_rows = load_dataframe(
        metrics,
        table_name="sales_records",
        if_exists="replace",
    )

    # Load daily product revenue into PostgreSQL.
    daily_rows = load_dataframe(
        daily_metrics,
        table_name="daily_product_revenue",
        if_exists="replace",
    )

    print("\nPostgreSQL load summary")
    print(f"Sales records loaded: {sales_rows:,}")
    print(f"Daily product revenue rows loaded: {daily_rows:,}")





if __name__ == "__main__":
    main()
