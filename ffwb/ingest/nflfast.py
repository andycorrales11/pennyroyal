# ffwb/ingest/nflfast.py
from __future__ import annotations

from typing import List

import pandas as pd

from ._nfl_compat import import_weekly_data  # shim handles version drift
from . import ids, io

# --------------------------- columns you’ll score ----------------------------
STAT_COLS: List[str] = [
    "pass_yds",
    "pass_tds",
    "pass_ints",
    "rush_yds",
    "rush_tds",
    "rec_rec",
    "rec_yds",
    "rec_tds",
    "fumbles_lost",
]

# -----------------------------------------------------------------------------


def ingest_actual_weekly(season: int, weeks: list[int] | None = None) -> pd.DataFrame:
    """
    Pull weekly box‑score data from nfl_data_py, map it to Sleeper IDs,
    and store to `actual_weekly` (Parquet partitioned by season + week).

    Returns the DataFrame that was written.
    """
    if weeks is None:
        weeks = list(range(1, 19))

    # --------------------------- load & filter -------------------------------
    raw = import_weekly_data([season])
    raw = raw.query("week in @weeks").reset_index(drop=True)

    # -------------------- normalize the GSIS identifier ----------------------
    id_variants = ("gsis_id", "gsis_it_id", "player_id")
    gsis_col = next((c for c in id_variants if c in raw.columns), None)
    if gsis_col is None:
        raise ValueError(
            f"None of {id_variants} found in weekly stats columns: {raw.columns[:15]}"
        )
    if gsis_col != "gsis_id":
        raw = raw.rename(columns={gsis_col: "gsis_id"})

    # --------------------------- back‑fill stats -----------------------------
    for col in STAT_COLS:
        if col not in raw.columns:
            raw[col] = 0

    # --------------------------- ID cross‑walk -------------------------------
    xwalk = ids.build_xwalk(season)
    merged = raw.merge(xwalk, on="gsis_id", how="left")

    df = (
        merged[["sleeper_id", "season", "week", *STAT_COLS]]
        .rename(columns={"sleeper_id": "player_id"})
        .astype({"season": "int16", "week": "int8"})
    )

    # --------------------------- write to Parquet ----------------------------
    io.to_parquet(df, "actual_weekly", partition_cols=["season", "week"])
    return df
