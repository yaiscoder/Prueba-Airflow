from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_PATH = PROJECT_ROOT / "data" / "data.csv"


def read_ecommerce_data(file_path: str | Path | None = None, batch_size: int | None = None,) -> pd.DataFrame:
    """Read the ecommerce CSV dataset."""
    source_path = Path(file_path) if file_path else DATA_PATH

    if not source_path.exists():
        raise FileNotFoundError(f"Dataset not found: {source_path}")

    if batch_size is not None and batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    read_options = (
        {"chunksize": batch_size}
        if batch_size is not None
        else {}
    )

    for encoding in ("utf-8", "latin-1"):
        try:
            data = pd.read_csv(
                source_path,
                encoding=encoding,
                **read_options,
            )

            if batch_size is not None:
                data = pd.concat(data, ignore_index=True)

            return data

        except UnicodeDecodeError:
            if encoding == "latin-1":
                raise

    raise RuntimeError("Unable to read the CSV file.")
