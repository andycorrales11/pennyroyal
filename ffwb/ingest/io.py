# ffwb/ingest/io.py
from __future__ import annotations

import pathlib
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------- central dtype map ----------
DTYPE_MAP: dict[str, pa.DataType] = {
    "pass_yds": pa.int32(),
    "pass_tds": pa.int8(),
    "pass_ints": pa.int8(),
    "rush_yds": pa.int32(),
    "rush_tds": pa.int8(),
    "rec_rec": pa.int8(),
    "rec_yds": pa.int32(),
    "rec_tds": pa.int8(),
    "fumbles_lost": pa.int8(),
    "actual_pts": pa.float32(),
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

    # --------- build Arrow schema (present cols + partition cols) ----------
    selected_fields = {
        col: pa.field(col, dtype)
        for col, dtype in DTYPE_MAP.items()
        if col in df.columns
    }

    # ensure partition columns are present
    for pc in partition_cols:
        if pc not in selected_fields:
            # derive Arrow dtype from the pandas column
            arrow_type = pa.from_numpy_dtype(df[pc].dtype)
            selected_fields[pc] = pa.field(pc, arrow_type)

    schema = pa.schema(list(selected_fields.values())) if selected_fields else None

    # --------- write partitioned Parquet ----------
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
