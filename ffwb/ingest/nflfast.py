# ffwb/ingest/nflfast.py
from __future__ import annotations
import pandas as pd
from nfl_data_py import import_weekly_data

from . import io, ids

STAT_COLS = [
    # pick the columns you’ll use in scoring
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


def ingest_actual_weekly(season: int, weeks: list[int] | None = None) -> pd.DataFrame:
    if weeks is None:
        weeks = list(range(1, 19))

    raw = import_weekly_data([season])
    raw = raw.query("week in @weeks")

    # attach sleeper IDs
    xwalk = ids.build_xwalk(season)
    merged = raw.merge(xwalk, on="gsis_id", how="left")

    df = merged[["sleeper_id", "season", "week", *STAT_COLS]].rename(
        columns={"sleeper_id": "player_id"}
    )
    io.to_parquet(df, "actual_weekly", partition_cols=["season", "week"])
    return df
