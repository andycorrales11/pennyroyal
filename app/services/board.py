# app/services/board.py
from ffwb import vor
from ffwb.ingest.tank01 import ingest_tank01
import pandas as pd

from ffwb.pipeline import DATA_DIR

ROSTER = {"qb": 1, "rb": 2, "wr": 2, "te": 1}


def _load_parquet(rel: str) -> pd.DataFrame:
    path = DATA_DIR / rel
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def load_board(season: int, week: int, teams: int = 12) -> pd.DataFrame:
    """Return Draft Board dataframe ready for templating."""
    proj = ingest_tank01(season, week)  # uses your existing code
    totals = proj.rename(columns={"fantasy_pts": "fantasy_pts_season"})

    board = vor.compute_vor(totals, ROSTER, num_teams=teams)
    board = board.sort_values(["tier", "vor"], ascending=[True, False])

    # format the floats ahead of time, keeps Jinja templates simple
    board["pts"] = board["fantasy_pts_season"].round(1)
    board["vor_f"] = board["vor"].round(1)

    return board[["player_id", "full_name", "position", "pts", "vor_f", "tier"]]


def load_season_board(season: int, teams: int = 12) -> pd.DataFrame:
    totals = _load_parquet(f"totals/season={season}")
    if totals.empty:
        raise RuntimeError(
            f"Season totals not found – run `ffwb-calc-season --season {season}` first."
        )

    vor_df = vor.compute_vor(
        totals,
        roster_settings={"qb": 1, "rb": 2, "wr": 2, "te": 1},
        num_teams=teams,
    )
    return _attach_names(vor_df, season)


def _attach_names(board: pd.DataFrame, season: int) -> pd.DataFrame:
    """
    Merge Sleeper IDs → player names (via Tank-01 roster parquet if present;
    otherwise pull ids.build_xwalk fall-back).
    """
    from ffwb.ingest.tank01_players import ingest_player_list

    roster_path = DATA_DIR / "tank01_players"

    if roster_path.exists():
        names = pd.read_parquet(roster_path)[["player_id", "full_name", "team"]]
    else:
        names = ingest_player_list()[["player_id", "full_name", "team"]]

    board = board.merge(names, on="player_id", how="left")
    board["full_name"] = board["full_name"].fillna("–")
    return board
