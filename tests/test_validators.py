import pandas as pd

from src.datamart.quality.validators import validate_sales


def create_sales_dataframe() -> pd.DataFrame:
    """Create a small dataset for validation tests."""
    return pd.DataFrame(
        {
            "InvoiceNo": ["1001", "1002", "1003", "1004"],
            "StockCode": ["A1", "A2", "A3", "A4"],
            "Description": ["Product A", None, "Product C", "Product D"],
            "Quantity": [2, -1, 3, 1],
            "InvoiceDate": pd.to_datetime(
                [
                    "2011-01-01 10:00",
                    "2011-01-01 11:00",
                    "2011-01-02 12:00",
                    "2011-01-02 13:00",
                ]
            ),
            "UnitPrice": [10.0, 5.0, 0.0, 8.0],
            "CustomerID": pd.array([101, 102, None, 104], dtype="Int64"),
            "Country": ["United Kingdom"] * 4,
        }
    )


def test_positive_quantity_is_classified_as_sale():
    dataframe = create_sales_dataframe()

    result = validate_sales(dataframe)

    assert result.loc[0, "record_type"] == "sale"


def test_non_positive_quantity_is_classified_as_return_adjustment():
    dataframe = create_sales_dataframe()

    result = validate_sales(dataframe)

    assert result.loc[1, "record_type"] == "return_adjustment"


def test_non_positive_unit_price_is_rejected():
    dataframe = create_sales_dataframe()

    result = validate_sales(dataframe)

    assert not result.loc[2, "is_valid"]
    assert (result.loc[2, "rejection_reason"]== "Unit price must be greater than zero")


def test_missing_customer_is_flagged_without_automatic_rejection():
    dataframe = create_sales_dataframe()

    # Use a valid price to isolate the missing-customer rule.
    dataframe.loc[2, "UnitPrice"] = 12.0

    result = validate_sales(dataframe)

    assert result.loc[2, "customer_missing"]
    assert result.loc[2, "is_valid"]



def test_missing_description_is_flagged_without_automatic_rejection():
    dataframe = create_sales_dataframe()

    result = validate_sales(dataframe)

    assert result.loc[1, "description_missing"]
    assert result.loc[1, "is_valid"]
