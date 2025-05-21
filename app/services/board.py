# app/services/board.py
from ffwb import vor
from ffwb.ingest.tank01 import ingest_tank01
import pandas as pd

ROSTER = {"qb": 1, "rb": 2, "wr": 2, "te": 1}


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
