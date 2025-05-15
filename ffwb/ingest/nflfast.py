# ffwb/ingest/nflfast.py
from __future__ import annotations

from typing import List

import pandas as pd

from ._nfl_compat import import_weekly_data
from . import ids, io

# ---------------------- canonical scoring columns ---------------------------
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

# mapping from 2024+ nflfast names ➜ canonical
_ALIAS_MAP = {
    "passing_yards": "pass_yds",
    "passing_tds": "pass_tds",
    "interceptions": "pass_ints",
    "rushing_yards": "rush_yds",
    "rushing_tds": "rush_tds",
    "receptions": "rec_rec",
    "receiving_yards": "rec_yds",
    "receiving_tds": "rec_tds",
    "fumbles_lost": "fumbles_lost",  # already same
}


# --------------------------------------------------------------------------- #
def ingest_actual_weekly(season: int, weeks: list[int] | None = None) -> pd.DataFrame:
    if weeks is None:
        weeks = list(range(1, 19))

    raw = import_weekly_data([season]).query("week in @weeks").reset_index(drop=True)

    # --- normalize GSIS id (code unchanged) ---
    id_variants = ("gsis_id", "gsis_it_id", "player_id")
    gsis_col = next((c for c in id_variants if c in raw.columns), None)
    if gsis_col is None:
        raise ValueError("Weekly stats file missing GSIS identifier")
    if gsis_col != "gsis_id":
        raw = raw.rename(columns={gsis_col: "gsis_id"})

    # ------------------- rename stat columns to canonical --------------------
    canon_rename = {src: tgt for src, tgt in _ALIAS_MAP.items() if src in raw.columns}
    raw = raw.rename(columns=canon_rename)

    # ensure every STAT_COL exists (zero-fill for byes/DST)
    for col in STAT_COLS:
        if col not in raw.columns:
            raw[col] = 0

    # ------------------- x-walk and export -----------------------------------
    xwalk = ids.build_xwalk(season)

    # ─── DEBUG & NORMALIZE GSIS IDs ─────────────────────────────────────────
    # Strip dashes and leading zeros so raw IDs match roster xwalk
    def normalize(s):
        return (
            s.astype("string")
            .str.replace("-", "", regex=True)
            .str.lstrip("0")
            .str.strip()
        )

    raw["gsis_id"] = normalize(raw["gsis_id"])
    xwalk["gsis_id"] = normalize(xwalk["gsis_id"])

    print("WEEKLY gsis_id sample:", raw["gsis_id"].drop_duplicates().head(10))
    print("ROSTER  gsis_id sample:", xwalk["gsis_id"].drop_duplicates().head(10))
    common = set(raw["gsis_id"].dropna()) & set(xwalk["gsis_id"].dropna())
    print("Common GSIS IDs:", len(common))

    merged = raw.merge(xwalk, on="gsis_id", how="left")

    df = (
        merged[["sleeper_id", "season", "week", *STAT_COLS]]
        .rename(columns={"sleeper_id": "player_id"})
        .astype({"season": "int16", "week": "int8"})
    )

    io.to_parquet(df, "actual_weekly", partition_cols=["season", "week"])
    return df
