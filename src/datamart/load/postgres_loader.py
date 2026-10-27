import pandas as pd
from sqlalchemy import text, types
from sqlalchemy.engine import Connection


def load_dataframe(dataframe: pd.DataFrame, table_name: str, if_exists: str = "append", engine: Connection | None = None,) -> int:
    """Load a DataFrame into PostgreSQL and return the number of rows."""
    allowed_tables = {
        "sales_records",
        "daily_product_revenue",
    }

    if table_name not in allowed_tables:
        raise ValueError(f"Unsupported table: {table_name}")

    if if_exists not in {"fail", "append"}:
        raise ValueError("if_exists must be 'fail' or 'append'")

    if engine is None:
        raise ValueError("A database connection must be provided.")

    if dataframe.empty:
        return 0

    records = dataframe.copy()
    records = records.astype(object).where(pd.notna(records), None)

    records.to_sql(
        name=table_name,
        con=engine,
        schema="public",
        if_exists=if_exists,
        index=False,
        chunksize=5_000,
        dtype={
            "InvoiceDate": types.DateTime(timezone=True),
            "sale_date": types.DateTime(timezone=True),
        },
    )

    return len(records)


def clear_target_tables(connection: Connection) -> None:
    """Clear both target tables within the current transaction."""
    connection.execute(
        text(
            "TRUNCATE TABLE "
            "public.sales_records, public.daily_product_revenue"
        )
    )
