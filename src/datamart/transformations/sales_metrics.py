import pandas as pd


def calculate_sales_metrics(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Calculate revenue metrics for each transaction record."""
    metrics = dataframe.copy()

    # Calculate the monetary value of each record.
    metrics["revenue"] = metrics["Quantity"] * metrics["UnitPrice"]

    # Identify valid positive-quantity sales.
    valid_sales = ((metrics["Quantity"] > 0)& (metrics["UnitPrice"] > 0) & metrics["InvoiceDate"].notna() & metrics["is_valid"])

    metrics["gross_revenue"] = 0.0
    metrics.loc[valid_sales, "gross_revenue"] = metrics.loc[valid_sales, "revenue"]

    # Identify valid returns and adjustments.
    valid_returns = ((metrics["Quantity"] <= 0) & metrics["InvoiceDate"].notna() & metrics["is_valid"])

    metrics["return_adjustment_amount"] = 0.0
    metrics.loc[valid_returns, "return_adjustment_amount"] = metrics.loc[valid_returns, "revenue"]

    # Exclude rejected records from the net revenue calculation.
    metrics["net_revenue"] = 0.0
    valid_records = metrics["is_valid"]
    metrics.loc[valid_records, "net_revenue"] = metrics.loc[valid_records, "revenue"]

    return metrics


def aggregate_daily_product_revenue(dataframe: pd.DataFrame,) -> pd.DataFrame:
    """Aggregate sales and returns by product and UTC day."""
    valid_records = dataframe.loc[dataframe["is_valid"]].copy()

    # Normalize timestamps to the UTC calendar day.
    valid_records["sale_date"] = valid_records["InvoiceDate"].dt.floor("D")

    # Aggregate sales and returns independently.
    daily_metrics = ( valid_records.groupby(["StockCode", "sale_date"], as_index=False, dropna=False,).agg(
            gross_revenue=("gross_revenue", "sum"),
            return_adjustment_amount=("return_adjustment_amount", "sum"),
            net_revenue=("net_revenue", "sum"),
        )
    )

    return daily_metrics

















