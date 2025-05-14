from __future__ import annotations

import pandas as pd


def compute_replacement(
    totals: pd.DataFrame,
    roster_settings: dict,
    *,
    num_teams: int,
) -> pd.DataFrame:
    """
    Return DF with one row per position containing the replacement
    fantasy_pts_season value.

    • totals must have columns: player_id, position, fantasy_pts_season
    • roster_settings example: {"qb": 1, "rb": 2, "wr": 2, "te": 1}
    """
    reps = []
    for pos, starters in roster_settings.items():
        k = starters * num_teams
        pool = totals.query("position == @pos").nlargest(k + 1, "fantasy_pts_season")
        # replacement = first player *not* drafted as a starter
        if len(pool) > k:
            rep_pts = pool.iloc[k]["fantasy_pts_season"]
        else:
            rep_pts = pool["fantasy_pts_season"].min()
        reps.append({"position": pos.upper(), "replacement_pts": rep_pts})
    return pd.DataFrame(reps)
