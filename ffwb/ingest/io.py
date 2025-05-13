# ffwb/ingest/io.py
from __future__ import annotations

import pathlib
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------- central dtype map ----------
DTYPE_MAP: dict[str, pa.DataType] = {
    "player_id": pa.string(),
    "team_id": pa.string(),
    "league_id": pa.string(),
    "season": pa.int16(),
    "week": pa.int8(),
    "src": pa.dictionary(pa.int8(), pa.string()),  # categorical
    "proj_pts": pa.float32(),
    "actual_pts": pa.float32(),
    # …extend as new stat columns arrive
}

# ---------- helpers ----------
_DATA_ROOT = pathlib.Path.cwd() / "data"  # overridable in tests


def to_parquet(
    df: pd.DataFrame, table: str, *, partition_cols: list[str] | None = None
) -> pathlib.Path:
    """
    Write a DataFrame using unified dtypes + snappy compression.
    Returns the file/dir path written to.
    """
    if partition_cols is None:
        partition_cols = []

    # cast columns that have a declared Arrow dtype
    for col, dtype in DTYPE_MAP.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype.to_pandas_dtype())

        # ---------- build a minimal Arrow schema with only present columns ----------
    selected_fields = [
        pa.field(col, dtype) for col, dtype in DTYPE_MAP.items() if col in df.columns
    ]
    schema = pa.schema(selected_fields) if selected_fields else None

    # ---------- write partitioned Parquet ----------
    table_path = _DATA_ROOT / table
    table_path.mkdir(parents=True, exist_ok=True)

    table = (
        pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        if schema is not None
        else pa.Table.from_pandas(df, preserve_index=False)
    )
    pq.write_to_dataset(
        table,
        root_path=str(table_path),
        partition_cols=partition_cols,
        compression="snappy",
    )
    return table_path


def read_parquet(
    table: str, filters: list[tuple[str, str, Any]] | None = None
) -> pd.DataFrame:
    """
    Load a table back into pandas, applying any Arrow filters.
    Example filter: [("season", "==", 2024)]
    """
    table_path = _DATA_ROOT / table
    dataset = pq.ParquetDataset(table_path, filters=filters)
    return dataset.read_pandas().to_pandas()
