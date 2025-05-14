# ffwb/scoring.py
from __future__ import annotations

import logging
from typing import Dict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def _validate_rules(df: pd.DataFrame, rules: Dict[str, float]) -> Dict[str, float]:
    """
    Ensure every key in `rules` exists in `df`.
    • If a column is missing, create it with zeros and warn once.
    • Return a *copy* of rules containing only the cols present in df
      (so we can pass directly to DataFrame.mul).
    """
    usable_rules: Dict[str, float] = {}
    missing: list[str] = []

    for col, weight in rules.items():
        if col in df.columns:
            usable_rules[col] = weight
        else:
            missing.append(col)
            df[col] = 0  # back‑fill with zeros

    if missing:
        logger.warning(
            "score_weekly: stat columns not found in DataFrame and "
            "treated as 0 → %s",
            ", ".join(missing),
        )
    return usable_rules


def score_weekly(
    stats: pd.DataFrame,
    rules: Dict[str, float],
    *,
    drop_stat_cols: bool = False,
) -> pd.DataFrame:
    """
    Vector‑compute fantasy points for each row.

    Parameters
    ----------
    stats : DataFrame
        Must contain the raw stat columns referenced in `rules`.
        Additional columns (player_id, week, etc.) are left untouched.
    rules : dict
        Mapping {stat_column: weight}. e.g. {"pass_td": 4, "pass_yds": 0.04}
    drop_stat_cols : bool, default False
        If True, remove the individual stat columns after computing points
        (keeps DataFrame compact).

    Returns
    -------
    DataFrame  (new copy)  with an extra column  `fantasy_pts`
    """
    df = stats.copy(deep=False)  # shallow copy keeps memory low

    usable_rules = _validate_rules(df, rules)
    if not usable_rules:
        raise ValueError("No valid stat columns found to score")

    # Multiply each stat column by its weight and sum row‑wise
    df["fantasy_pts"] = df[list(usable_rules)].mul(usable_rules).sum(axis=1)

    if drop_stat_cols:
        df = df.drop(columns=list(usable_rules))

    return df


def aggregate_season(
    scored: pd.DataFrame,
    *,
    agg: str | dict[str, str] = "sum",
    points_col: str = "fantasy_pts",
) -> pd.DataFrame:
    """
    Collapse weekly rows to one row per (player_id, season).

    Parameters
    ----------
    scored : DataFrame
        Must include `player_id`, `season`, `week`, and `points_col`.
    agg : str or dict, default "sum"
        Aggregation for points column.  Use "mean" for best‑ball leagues.
        You can also pass dict ({"fantasy_pts": "mean", "rush_yds": "sum"}).
    points_col : str, default "fantasy_pts"
        Name of the points column to aggregate.

    Returns
    -------
    DataFrame
        Columns: player_id, season, <aggregated stats...>
    """
    if points_col not in scored.columns:
        raise KeyError(f"{points_col} not found in DataFrame")

    if isinstance(agg, str):
        agg_map = {points_col: agg}
    else:
        agg_map = agg

    out = (
        scored.groupby(["player_id", "season"], as_index=False)
        .agg(agg_map)
        .rename(columns={points_col: "fantasy_pts_season"})
    )
    # ensure deterministic float dtype
    out["fantasy_pts_season"] = out["fantasy_pts_season"].astype(np.float32)
    return out
