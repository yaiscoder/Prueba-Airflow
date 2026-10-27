import pandas as pd

from src.datamart.transformations.normalize import normalize_sales


def test_invoice_dates_are_converted_to_utc():
    dataframe = pd.DataFrame(
        {
            "StockCode": [" a1 "],
            "Description": [" product a "],
            "InvoiceDate": ["12/1/2010 8:26"],
            "CustomerID": [101],
        }
    )

    result = normalize_sales(dataframe)

    assert str(result["InvoiceDate"].dtype) == "datetime64[us, UTC]"
    assert result.loc[0, "InvoiceDate"] == pd.Timestamp(
        "2010-12-01 08:26:00", tz="UTC"
    )


def test_invalid_invoice_dates_become_missing():
    dataframe = pd.DataFrame(
        {
            "StockCode": ["A1"],
            "Description": ["Product A"],
            "InvoiceDate": ["invalid date"],
            "CustomerID": [101],
        }
    )

    result = normalize_sales(dataframe)

    assert result["InvoiceDate"].isna().all()











