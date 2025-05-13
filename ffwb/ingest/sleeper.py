# ffwb/ingest/sleeper.py
from __future__ import annotations

# import os
import requests
import pandas as pd
from typing import TypedDict

from . import io  # relative import within package


BASE_URL = "https://api.sleeper.app/v1"


class LeagueMeta(TypedDict):
    league_id: str
    season: int
    name: str
    scoring_settings: dict
    rostersettings: dict


def _get(path: str) -> dict | list:
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ---------- public helpers ----------
def ingest_league(league_id: str) -> pd.DataFrame:
    """Fetch league metadata and store to `league` table."""
    raw: LeagueMeta = _get(f"league/{league_id}")  # type: ignore[assignment]
    df = pd.DataFrame(
        [
            {
                "league_id": raw["league_id"],
                "season": int(raw["season"]),
                "host": "sleeper",
                "scoring_json": raw["scoring_settings"],
            }
        ]
    )
    io.to_parquet(df, "league", partition_cols=["season"])
    return df


def ingest_teams(league_id: str) -> pd.DataFrame:
    """Teams / owners for the league."""
    raw = _get(f"league/{league_id}/users")
    df = pd.json_normalize(
        raw,
        record_path=None,
        meta=["user_id", "display_name"],
    ).rename(columns={"user_id": "team_id", "display_name": "owner"})
    df["league_id"] = league_id
    io.to_parquet(df, "team")
    return df


def ingest_rosters_weekly(league_id: str, *, weeks: list[int]) -> pd.DataFrame:
    """Fetch roster slots for each week requested."""
    frames: list[pd.DataFrame] = []
    for wk in weeks:
        raw = _get(f"league/{league_id}/rosters/{wk}")
        df = pd.json_normalize(raw)
        df["league_id"] = league_id
        df["week"] = wk
        frames.append(df)
    roster = pd.concat(frames, ignore_index=True)
    io.to_parquet(roster, "roster_weekly", partition_cols=["week"])
    return roster
