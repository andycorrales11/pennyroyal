# ffwb/cli_proj_tank.py
from __future__ import annotations

import argparse

import pandas as pd
from rich import print
from rich.table import Table

from ffwb.ingest.tank01 import ingest_tank01
from ffwb import vor
import numpy as np


ROSTER_SETTINGS = {"qb": 1, "rb": 2, "wr": 2, "te": 1}


def tank_board() -> None:
    p = argparse.ArgumentParser(description="Tank-01 weekly VOR draft board")
    p.add_argument("--season", type=int, required=True)
    p.add_argument("--week", type=int, required=True)
    p.add_argument("--teams", type=int, default=12)
    args = p.parse_args()

    # ------------------------------------------------------------------ #
    # pull projections (Sleeper IDs in player_id, plus names/teams)
    # ------------------------------------------------------------------ #
    proj = ingest_tank01(args.season, args.week)

    # one-week “season totals”
    totals = proj.rename(columns={"fantasy_pts": "fantasy_pts_season"})

    board = vor.compute_vor(
        totals,
        roster_settings=ROSTER_SETTINGS,
        num_teams=args.teams,
    )

    # ------------------------------------------------------------------ #
    # tidy & sort
    # ------------------------------------------------------------------ #
    board = board.sort_values(["tier", "vor"], ascending=[True, False])

    # ------------------------------------------------------------------ #
    # pretty print
    # ------------------------------------------------------------------ #
    tbl = Table(title=f"Week {args.week} Projections ({args.season})")
    for col in ["full_name", "position", "fantasy_pts_season", "vor", "tier"]:
        tbl.add_column(col)

    cols = ["full_name", "position", "fantasy_pts_season", "vor", "tier"]

    for _, row in board.head(150).iterrows():
        cells: list[str] = []
        for col in cols:
            val = row[col]
            # blank for missing values
            if pd.isna(val):
                cells.append("-")
            # pretty-print floats
            elif isinstance(val, (float, np.floating)):
                cells.append(f"{val:.2f}")
            else:
                cells.append(str(val))
        tbl.add_row(*cells)

    print(tbl)


if __name__ == "__main__":
    tank_board()
