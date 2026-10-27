import pandas as pd

from src.datamart.transformations.sales_metrics import (
    calculate_sales_metrics,
)


def test_calculates_gross_revenue_for_positive_sales():
    dataframe = pd.DataFrame(
        {
            "Quantity": [3],
            "UnitPrice": [10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01 10:00"]),
            "is_valid": [True],
        }
    )

    result = calculate_sales_metrics(dataframe)

    assert result.loc[0, "revenue"] == 30.0
    assert result.loc[0, "gross_revenue"] == 30.0
    assert result.loc[0, "return_adjustment_amount"] == 0.0
    assert result.loc[0, "net_revenue"] == 30.0


def test_keeps_negative_quantity_as_return_adjustment():
    dataframe = pd.DataFrame(
        {
            "Quantity": [-1],
            "UnitPrice": [10.0],
            "InvoiceDate": pd.to_datetime(["2011-01-01 10:00"]),
            "is_valid": [True],
        }
    )

    result = calculate_sales_metrics(dataframe)

    assert result.loc[0, "revenue"] == -10.0
    assert result.loc[0, "gross_revenue"] == 0.0
    assert result.loc[0, "return_adjustment_amount"] == -10.0
    assert result.loc[0, "net_revenue"] == -10.0









