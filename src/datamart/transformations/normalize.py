import pandas as pd


SOURCE_TIMEZONE = "Europe/London"


def normalize_sales(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize sales data and convert invoice dates to UTC."""
    normalized = dataframe.copy()

    # Normalize column names.
    normalized.columns = [column.strip() for column in normalized.columns]

    # Normalize product codes.
    normalized["StockCode"] = (normalized["StockCode"].astype("string").str.strip().str.upper())

    # Normalize product descriptions.
    normalized["Description"] = (normalized["Description"].astype("string").str.strip().str.upper())

    # Parse invoice dates using the source format.
    dates = pd.to_datetime(normalized["InvoiceDate"], format="%m/%d/%Y %H:%M",errors="coerce")

    # Localize source timestamps and convert them to UTC.
    normalized["InvoiceDate"] = (dates.dt.tz_localize( SOURCE_TIMEZONE, ambiguous="NaT", nonexistent="NaT",).dt.tz_convert("UTC"))

    # Preserve missing customer IDs as nullable values.
    normalized["CustomerID"] = normalized["CustomerID"].astype("Int64")

    return normalized











