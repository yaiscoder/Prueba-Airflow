from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_PATH = PROJECT_ROOT / "data" / "online_retail.csv"


def read_online_retail_data() -> pd.DataFrame:
    """Read the second online retail CSV dataset."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATA_PATH}")

    try:
        return pd.read_csv(DATA_PATH, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(DATA_PATH, encoding="latin-1")