from __future__ import annotations

import io as _io

# from typing import List

import pandas as pd
import requests

from . import io, ids

# ---------- public endpoints ----------
FANTASYPROS_URL = "https://www.fantasypros.com/nfl/adp/overall.php?csv=1"
UNDERDOG_URL = "https://underdogfantasy.com/api/tournaments/puppy_5/adp"
FFC_URL_TMPL = "https://fantasyfootballcalculator.com/api/v1/adp/standard?teams={teams}&year={year}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}


class ADPError(RuntimeError): ...


# ---------------------------- source loaders ---------------------------------
def _load_fpros_csv() -> pd.DataFrame:
    resp = requests.get(FANTASYPROS_URL, headers=HEADERS, timeout=10)
    if "text/csv" not in resp.headers.get("Content-Type", ""):
        raise ADPError("FantasyPros returned non‑CSV")

    try:
        df = pd.read_csv(_io.BytesIO(resp.content))
    except Exception as exc:
        raise ADPError("FantasyPros CSV parse failed") from exc

    if not {"Player", "Pos", "ADP"}.issubset(df.columns):
        raise ADPError("FantasyPros CSV missing columns")

    return df.rename(columns={"Player": "full_name", "Pos": "position", "ADP": "adp"})[
        ["full_name", "position", "adp"]
    ]


def _load_underdog_json() -> pd.DataFrame:
    resp = requests.get(UNDERDOG_URL, headers=HEADERS, timeout=10)
    try:
        data = resp.json()
    except ValueError as exc:
        raise ADPError("Underdog JSON decode failed") from exc

    rows = data.get("players", {}).values() if "players" in data else data["overall"]
    df = pd.DataFrame(rows).rename(
        columns={"name": "full_name", "adp": "adp", "position": "position"}
    )
    return df[["full_name", "position", "adp"]]


def _load_ffc_json(season: int, teams: int = 12) -> pd.DataFrame:
    url = FFC_URL_TMPL.format(year=season, teams=teams)
    resp = requests.get(url, headers=HEADERS, timeout=10)
    try:
        data = resp.json()
    except ValueError as exc:
        raise ADPError("FFC JSON decode failed") from exc

    # FFC JSON: {"QB":[{...}, ...], "RB":[...], ...}
    rows: list[dict] = []
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                rows.extend(v)
    elif isinstance(data, list):
        rows = data
    else:
        raise ADPError("Unexpected FFC JSON structure")

    if not rows:
        raise ADPError("FFC returned empty data")

    df = pd.DataFrame(rows)

    # Normalize column names
    df = df.rename(
        columns={
            "name": "full_name",
            "player_name": "full_name",
            "pos": "position",
            "overall": "adp",
            "average_pick": "adp",
        }
    )
    if not {"full_name", "position", "adp"}.issubset(df.columns):
        raise ADPError("FFC JSON missing expected columns")

    return df[["full_name", "position", "adp"]]


# ---------------------------- mapping helper ---------------------------------
def _map_to_players(adp_raw: pd.DataFrame, season: int) -> pd.DataFrame:
    roster = ids.build_xwalk(season)[["sleeper_id", "full_name"]]
    adp = (
        adp_raw.merge(roster, on="full_name", how="left")
        .dropna(subset=["sleeper_id"])
        .rename(columns={"sleeper_id": "player_id"})
    )
    return adp[["player_id", "position", "adp"]]


# ---------------------------- public ingest ----------------------------------
def ingest_adp(
    season: int,
    *,
    source: str = "ffc",  # "fantasypros" | "underdog" | "ffc"
    teams: int = 12,
) -> pd.DataFrame:
    if source == "fantasypros":
        adp_raw = _load_fpros_csv()
    elif source == "underdog":
        adp_raw = _load_underdog_json()
    elif source == "ffc":
        adp_raw = _load_ffc_json(season, teams)
    else:
        raise ValueError("source must be 'fantasypros', 'underdog', or 'ffc'")

    adp = _map_to_players(adp_raw, season)
    adp["season"] = season
    adp["source"] = source
    io.to_parquet(adp, "adp", partition_cols=["season", "source"])
    return adp
