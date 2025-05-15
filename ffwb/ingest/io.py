"""Lightweight Parquet helpers with Arrow schema down‑casting."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_DATA_ROOT = Path.cwd() / "data"

# --------------------------------------------------------------------------- #
#  Project‑wide canonical dtypes (extend as needed)
# --------------------------------------------------------------------------- #
DTYPE_MAP: Dict[str, pa.DataType] = {
    "player_id": pa.string(),
    "league_id": pa.string(),
    "team_id": pa.string(),
    "season": pa.int16(),
    "week": pa.int8(),
    "position": pa.string(),
    # add more stat columns here
}


# --------------------------------------------------------------------------- #
#  Helper
# --------------------------------------------------------------------------- #
def to_parquet(
    df: pd.DataFrame,
    table: str,
    *,
    partition_cols: list[str] | None = None,
) -> Path:
    """
    Write a DataFrame to `data/{table}/` partitioned parquet.
    Only columns present in DTYPE_MAP (plus partition_cols) get included.
    """
    partition_cols = partition_cols or []

    # ---------- Arrow schema (project dtypes + partitions) ----------
    selected_fields = {}
    for col in df.columns:
        if col in DTYPE_MAP:
            arrow_type = DTYPE_MAP[col]
        else:
            pd_dtype = df[col].dtype
            if pd.api.types.is_object_dtype(pd_dtype) or pd.api.types.is_string_dtype(
                pd_dtype
            ):
                arrow_type = pa.string()
            else:
                arrow_type = pa.from_numpy_dtype(pd_dtype)
        selected_fields[col] = pa.field(col, arrow_type)

    # ensure partition cols present
    for pc in partition_cols:
        if pc not in selected_fields:  # unlikely, but guard
            selected_fields[pc] = pa.field(pc, pa.string())

    schema = pa.schema(list(selected_fields.values())) if selected_fields else None

    # ---------- write ----------
    table_path = _DATA_ROOT / table
    table_path.mkdir(parents=True, exist_ok=True)

    pa_table = (
        pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        if schema is not None
        else pa.Table.from_pandas(df, preserve_index=False)
    )

    pq.write_to_dataset(
        pa_table,
        root_path=str(table_path),
        partition_cols=partition_cols,
        compression="snappy",
    )
    return table_path
