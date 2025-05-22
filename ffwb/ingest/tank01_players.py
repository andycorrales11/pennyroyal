from __future__ import annotations

import os
from typing import List, Dict

import pandas as pd
import requests
from dotenv import load_dotenv
from ffwb.ingest import io as io_utils

load_dotenv()

_URL = (
    "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/"
    "getNFLTeamRoster?teamAbv="
)
_TEAMS = [
    "BUF",
    "MIA",
    "NE",
    "NYJ",
    "BAL",
    "CIN",
    "CLE",
    "PIT",
    "HOU",
    "IND",
    "JAX",
    "TEN",
    "DEN",
    "KC",
    "LV",
    "LAC",
    "DAL",
    "NYG",
    "PHI",
    "WSH",
    "CHI",
    "DET",
    "GB",
    "MIN",
    "ATL",
    "CAR",
    "NO",
    "TB",
    "ARI",
    "LAR",
    "SEA",
    "SF",
]

_HEADERS: Dict[str, str] = {
    "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    "x-rapidapi-key": os.getenv("RAPIDAPI_TANK01_KEY", ""),
}

_KEEP = ["player_id", "full_name", "pos", "team", "tank_id"]


def ingest_player_list() -> pd.DataFrame:
    """Pull every team roster from Tank-01, cache to Parquet, and return DF."""
    if not _HEADERS["x-rapidapi-key"]:
        raise RuntimeError("Set RAPIDAPI_TANK01_KEY env var")

    rows: List[Dict] = []

    for team in _TEAMS:
        url = f"{_URL}{team}&getStats=true&fantasyPoints=true"
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        roster = data.get("body", {}).get("roster", [])
        for plyr in roster:
            rows.append(
                {
                    "player_id": str(plyr.get("playerID") or ""),
                    "sleeper_id": str(plyr.get("sleeperBotID") or ""),
                    "full_name": plyr.get("longName"),
                    "pos": plyr.get("pos"),
                    "team": team,
                }
            )

    df = (
        pd.DataFrame(rows)[_KEEP]
        .dropna(subset=["player_id"])  # remove empty IDs
        .drop_duplicates("player_id")  # prevent merge fan-out
        .reset_index(drop=True)
    )

    io_utils.to_parquet(df, "tank01_players")  # data/tank01_players/*.parquet
    return df
