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
    """
    Join ADP onto VOR and compute 'value_vs_adp'.
    This will auto-detect whichever ADP column you have (e.g. 'adp_mean', 'avg_pick', etc.)
    and any stdev column (e.g. 'adp_std', 'std_dev', ...). If none are found,
    fills with NA so the merge still works.
    """
    # Make a copy so we don’t clobber the original
    adp = adp_df.copy()

    # 1) Find the ADP column
    adp_cols = [
        c for c in adp.columns if "adp" in c.lower() and c.lower() not in ("adp_stdev",)
    ]
    if "adp" in adp.columns:
        adp_col = "adp"
    elif adp_cols:
        # pick the first matching column (e.g. 'adp_mean' or 'avg_pick')
        adp_col = adp_cols[0]
        adp = adp.rename(columns={adp_col: "adp"})
    else:
        adp_col = None

    # 2) Find the ADP-stdev column
    stdev_cols = [
        c for c in adp.columns if any(x in c.lower() for x in ("stdev", "std", "dev"))
    ]
    if "adp_stdev" in adp.columns:
        stdev_col = "adp_stdev"
    elif stdev_cols:
        stdev_col = stdev_cols[0]
        adp = adp.rename(columns={stdev_col: "adp_stdev"})
    else:
        stdev_col = None

    # 3) Ensure we have the columns (or create them)
    if adp_col is None:
        adp["adp"] = pd.NA
    if stdev_col is None:
        adp["adp_stdev"] = pd.NA

    # 4) Now do the merge
    merged = vor_df.merge(
        adp[["player_id", "adp", "adp_stdev"]],
        on="player_id",
        how="left",
    )

    # 5) Compute value_vs_adp if possible
    merged["value_vs_adp"] = np.where(
        merged["adp"].notna() & (merged["adp"] != 0),
        merged["vor"] / merged["adp"],
        pd.NA,
    )
    return merged
