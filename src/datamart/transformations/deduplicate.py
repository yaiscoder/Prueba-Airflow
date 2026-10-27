import pandas as pd


def remove_exact_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are exact duplicates of earlier rows."""
    return dataframe.drop_duplicates().copy()
