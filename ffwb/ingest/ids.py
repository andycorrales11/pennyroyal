# ffwb/ingest/ids.py
from __future__ import annotations

import pandas as pd
from nfl_data_py import import_seasonal_rosters


def build_xwalk(season: int) -> pd.DataFrame:
    """
    Return DataFrame mapping `gsis_id` → `sleeper_id`
    (plus `full_name`, `position`).

    Handles both old and new nfl_data_py roster schemas.
    """
    roster = import_seasonal_rosters([season])

    # ---------- harmonise GSIS id ----------
    if "player_id" in roster.columns:
        # nfl_data_py’s import_seasonal_rosters puts the NFL GSIS ID in `player_id`
        roster = roster.rename(columns={"player_id": "gsis_id"})
    elif "gsis_id" in roster.columns:
        roster = roster.rename(columns={"gsis_id": "gsis_id"})
    elif "gsis_it_id" in roster.columns:
        roster = roster.rename(columns={"gsis_it_id": "gsis_id"})
    elif "gameday_id" in roster.columns:
        roster = roster.rename(columns={"gameday_id": "gsis_id"})
    else:
        raise KeyError("No GSIS id column found in seasonal roster")
    # ---------- harmonise full name ----------
    if "display_name" in roster.columns:
        roster = roster.rename(columns={"display_name": "full_name"})
    elif "player_name" in roster.columns:
        roster = roster.rename(columns={"player_name": "full_name"})
    else:
        # fall back to first + last
        roster["full_name"] = roster["first_name"].str.cat(roster["last_name"], sep=" ")

    keep_cols = ["gsis_id", "sleeper_id", "full_name", "position"]
    df = (
        roster[keep_cols]
        .dropna(subset=["gsis_id", "sleeper_id"])
        .astype({"gsis_id": "string", "sleeper_id": "string"})
        .drop_duplicates("gsis_id")
    )
    print("xwalk (gsis_id → sleeper_id) sample:", df.head(10))
    print("Total xwalk rows:", len(df))
    return df
