import pandas as pd

from src.datamart.transformations.deduplicate import (
    remove_exact_duplicates,
)


def test_removes_exact_duplicate_rows():
    dataframe = pd.DataFrame(
        {
            "InvoiceNo": ["1001", "1001", "1002"],
            "StockCode": ["A1", "A1", "A2"],
            "Quantity": [2, 2, 1],
        }
    )

    result = remove_exact_duplicates(dataframe)

    assert len(result) == 2
    assert not result.duplicated().any()
