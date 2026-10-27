import pandas as pd


def validate_sales(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Classify sales records according to business rules."""

    validated = dataframe.copy()

    # Initialize record classification and rejection reasons.
    validated["record_type"] = "sale"
    validated["is_valid"] = True
    validated["rejection_reason"] = pd.NA

    # Identify returns and adjustments.
    return_mask = validated["Quantity"] <= 0
    validated.loc[return_mask, "record_type"] = "return_adjustment"

    # Reject records with non-positive unit prices.
    invalid_price = validated["UnitPrice"] <= 0
    validated.loc[invalid_price, "is_valid"] = False
    validated.loc[invalid_price, "rejection_reason"] = ("Unit price must be greater than zero")

    # Reject records with invalid invoice dates.
    invalid_date = validated["InvoiceDate"].isna()
    validated.loc[invalid_date, "is_valid"] = False

    validated.loc[invalid_date, "rejection_reason"] = (validated.loc[invalid_date, "rejection_reason"].fillna("").apply(
            lambda reason: (
                f"{reason}; Invalid invoice date"
                if reason
                else "Invalid invoice date"
            )
        )
    )

    # Flag missing customer IDs without rejecting the transaction.
    validated["customer_missing"] = validated["CustomerID"].isna()

    # Flag missing product descriptions without rejecting the transaction.
    validated["description_missing"] = validated["Description"].isna()

    return validated
