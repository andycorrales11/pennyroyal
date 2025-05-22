from __future__ import annotations

import os
from typing import Dict, List

import pandas as pd
import requests
import json
import logging
from ffwb.ingest import io as io_utils
from dotenv import load_dotenv

load_dotenv()

URL = (
    "https://tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com/"
    "getNFLProjections"
)

HEADERS = {
    "x-rapidapi-host": "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com",
    "x-rapidapi-key": os.getenv("RAPIDAPI_TANK01_KEY", ""),
}

KEEP = ["player_id", "season", "week", "position", "fantasy_pts", "full_name", "team"]


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _request(week: int, season: int, weights: Dict[str, str]) -> List[dict]:
    params = {"week": week, "archiveSeason": season, **weights}
    resp = requests.get(URL, headers=HEADERS, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    try:
        pp = data["body"]["playerProjections"]
    except (KeyError, TypeError):

        logging.warning("Tank-01 unexpected response: %s", json.dumps(data)[:400])
        raise RuntimeError("Tank-01 response missing 'body→playerProjections'")

    rows: list[dict] = []
    for proj in pp.values():  # keys are Tank IDs – use sleeperBotID inside
        rows.append(
            {
                "player_id": str(proj.get("playerID")),
                "position": proj.get("pos"),
                "fantasy_pts": float(
                    proj.get("fantasyPointsDefault", {}).get("PPR", 0)
                ),
                # flatten sub-dicts
                **{
                    f"pass_{k.lower()}": float(v)
                    for k, v in proj.get("Passing", {}).items()
                },
                **{
                    f"rush_{k.lower()}": float(v)
                    for k, v in proj.get("Rushing", {}).items()
                },
                **{
                    f"rec_{k.lower()}": float(v)
                    for k, v in proj.get("Receiving", {}).items()
                },
                "fumbles_lost": float(proj.get("fumblesLost", 0)),
                "full_name": str(proj.get("longName")),
            }
        )

    return rows


# --------------------------------------------------------------------------- #
# public ingest
# --------------------------------------------------------------------------- #
def ingest_tank01(
    season: int,
    week: int,
    *,
    scoring_weights: Dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Fetch Tank-01 projections for one week, map to Sleeper IDs, store Parquet,
    and return a tidy DataFrame.
    """
    if not HEADERS["x-rapidapi-key"]:
        raise RuntimeError("Set RAPIDAPI_TANK01_KEY env var with your RapidAPI key")

    # default = half-PPR
    weights = scoring_weights or {
        "pointsPerReception": 0.5,
        "passYards": 0.04,
        "passTD": 4,
        "passInterceptions": -2,
        "rushYards": 0.1,
        "rushTD": 6,
        "receivingYards": 0.1,
        "receivingTD": 6,
        "fumbles": -2,
    }

    rows = _request(week, season, weights)
    df = pd.json_normalize(rows)

    df["season"] = season
    df["week"] = week
    df["position"] = df["position"].str.upper()

    # ------------------------------------------------------------------ #
    # attach names & teams from cached roster
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # final tidy frame
    # ------------------------------------------------------------------ #
    proj = df[[c for c in KEEP if c in df.columns]].copy()
    io_utils.to_parquet(
        proj, "projection_weekly_tank01", partition_cols=["season", "week"]
    )
    return proj
