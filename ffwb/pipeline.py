from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from rich import print

from ffwb import scoring, vor
from ffwb.ingest import io

DATA_DIR = Path.cwd() / "data"

# --------------------------------------------------------------------------- #
#  Scoring rules (half‑PPR, match nflfast column names)
# --------------------------------------------------------------------------- #
DEFAULT_RULES = {
    "pass_tds": 4,
    "pass_yds": 0.04,
    "pass_ints": -2,
    "rush_tds": 6,
    "rush_yds": 0.1,
    "rec_rec": 0.5,  # half‑PPR
    "rec_yds": 0.1,
    "rec_tds": 6,
    "fumbles_lost": -2,
}

ROSTER_SETTINGS = {"qb": 1, "rb": 2, "wr": 2, "te": 1}


# --------------------------------------------------------------------------- #
#  calc‑season: weekly → season totals
# --------------------------------------------------------------------------- #
def calc_season_main() -> None:
    parser = argparse.ArgumentParser(description="Score weekly stats → season totals")
    parser.add_argument("--season", type=int, required=True)
    args = parser.parse_args()

    wk_path = DATA_DIR / "actual_weekly" / f"season={args.season}"
    if not wk_path.exists():
        print(f"[red]No weekly stats found at {wk_path}[/red]")
        return

    print(f"[green]Loading weekly stats from {wk_path}[/green]")
    df_weekly = pd.read_parquet(wk_path)

    if "season" not in df_weekly.columns:
        df_weekly["season"] = args.season

    if "fantasy_pts" not in df_weekly.columns:
        print("RULE KEYS:", DEFAULT_RULES.keys())
        df_weekly = scoring.score_weekly(df_weekly, DEFAULT_RULES)
    if not wk_path.exists():
        print(f"[red]No weekly stats found at {wk_path}[/red]")
        return

    print(f"[green]Loading weekly stats from {wk_path}[/green]")
    # df_weekly = pd.read_parquet(wk_path)

    totals = scoring.aggregate_season(df_weekly)
    io.to_parquet(totals, "totals", partition_cols=["season"])
    print(f"[green]Wrote season totals to data/totals/season={args.season}[/green]")


# --------------------------------------------------------------------------- #
#  calc‑vor: season totals → VOR
# --------------------------------------------------------------------------- #
def calc_vor_main() -> None:
    parser = argparse.ArgumentParser(description="Season totals → VOR")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--teams", type=int, default=12)
    args = parser.parse_args()

    part_path = DATA_DIR / "totals" / f"season={args.season}"
    root_path = DATA_DIR / "totals"

    if part_path.exists():
        totals = pd.read_parquet(part_path)
    elif root_path.exists():
        # fall back: load full dataset and filter
        totals = pd.read_parquet(root_path).query("season == @args.season")
        if totals.empty:
            print(f"[red]No rows for season {args.season} in {root_path}[/red]")
            return
    else:
        print(f"[red]No season totals found at {root_path}[/red]")
        return

    vor_df = vor.compute_vor(
        totals,
        roster_settings=ROSTER_SETTINGS,
        num_teams=args.teams,
    )
    io.to_parquet(vor_df, "vor", partition_cols=["season"])
    print(f"[green]Wrote VOR table to data/vor/season={args.season}[/green]")


# --------------------------------------------------------------------------- #
#  module CLI dispatch
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys

    if sys.argv[1:2] == ["calc-season"]:
        sys.argv.pop(1)
        calc_season_main()
    elif sys.argv[1:2] == ["calc-vor"]:
        sys.argv.pop(1)
        calc_vor_main()
    else:
        print("Use: python -m ffwb.pipeline calc-season|calc-vor [...]")
