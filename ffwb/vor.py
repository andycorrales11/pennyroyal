from __future__ import annotations

import pandas as pd
import numpy as np


# --------------------------------------------------------------------------- #
#  Replacement‑level helper
# --------------------------------------------------------------------------- #
def compute_replacement(
    totals: pd.DataFrame,
    roster_settings: dict[str, int],
    *,
    num_teams: int,
) -> pd.DataFrame:
    """
    One row per position with the season‑long replacement‑level points.
    """
    reps = []
    for pos_raw, starters in roster_settings.items():
        pos = pos_raw.upper()  # ▲ normalize once
        k = starters * num_teams
        pool = totals.query("position == @pos").nlargest(k + 1, "fantasy_pts_season")

        if len(pool) > k:
            rep_pts = pool.iloc[k]["fantasy_pts_season"]
        else:
            rep_pts = pool["fantasy_pts_season"].min()

        reps.append(
            {"position": pos, "replacement_pts": rep_pts}
        )  # ▲ pos already upper
    return pd.DataFrame(reps)


# --------------------------------------------------------------------------- #
#  VOR + tier calculation
# --------------------------------------------------------------------------- #
def compute_vor(
    totals: pd.DataFrame,
    roster_settings: dict[str, int],
    *,
    num_teams: int,
    tier_method: str = "quantile",  # "quantile" or "fixed"
    q: float = 0.2,
) -> pd.DataFrame:
    """
    Adds two columns to `totals`:
      • vor   –– points above position‑specific replacement
      • tier  –– tier number (1 = best)
    """
    rep = compute_replacement(totals, roster_settings, num_teams=num_teams)
    merged = totals.merge(rep, on="position", how="left")
    merged["vor"] = merged["fantasy_pts_season"] - merged["replacement_pts"]

    # -------------- helper to assign tier within each position --------------
    def _assign_tier(sub: pd.Series) -> pd.Series:
        if tier_method == "quantile":
            mask = sub > 0  # only positive‑VOR players get meaningful tiers
            pos_count = mask.sum()

            # if no one beats replacement → everyone tier 99
            if pos_count == 0:
                return pd.Series(99, index=sub.index, dtype="int8")

            n_bins = max(1, int(np.ceil(1 / q)))

            # fewer positive players than bins → all tier 1
            if pos_count < n_bins:
                out = pd.Series(99, index=sub.index, dtype="int8")
                out.loc[mask] = 1
                return out

            tiers = (
                pd.qcut(
                    sub[mask].rank(method="first", ascending=False),
                    q=n_bins,
                    labels=False,
                )
                + 1
            ).astype("int8")

            out = pd.Series(99, index=sub.index, dtype="int8")
            out.loc[mask] = tiers
            return out

        elif tier_method == "fixed":
            # fixed‑width buckets (e.g., every 20 pts)
            tier = ((-sub).divide(q).apply(np.floor) * -1).astype(int) * -1
            tier = tier.replace(0, 1).clip(lower=1)
            return tier.astype("int8")

        else:
            raise ValueError("tier_method must be 'quantile' or 'fixed'")

    merged["tier"] = merged.groupby("position", group_keys=False)["vor"].apply(
        _assign_tier
    )

    return merged


def attach_adp(
    vor_df: pd.DataFrame,
    adp_df: pd.DataFrame,
) -> pd.DataFrame:
    """Join ADP onto VOR table and compute 'value_over_adp' (lower ADP + high VOR)."""
    merged = vor_df.merge(
        adp_df[["player_id", "adp", "adp_stdev"]], on="player_id", how="left"
    )
    merged["value_vs_adp"] = merged["vor"] / merged["adp"]
    return merged
