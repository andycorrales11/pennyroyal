from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from rich import print
from rich.table import Table

from ffwb.ingest.adp import ingest_adp, ADPError, _map_to_players
from ffwb.ingest.ids import build_xwalk

# from ffwb.ingest import io
from ffwb import vor


# --------------------------- helpers ----------------------------------------
def _load_parquet(rel_path: str) -> pd.DataFrame:
    path = Path.cwd() / "data" / rel_path
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


# --------------------------- CLI --------------------------------------------
def draft_board() -> None:
    parser = argparse.ArgumentParser(description="Show VOR draft board")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--teams", type=int, default=12)
    parser.add_argument(
        "--source",
        choices=["fantasypros", "underdog", "ffc"],
        default="ffc",
        help="Online ADP source",
    )
    parser.add_argument(
        "--adp-file",
        help="Local CSV/JSON with columns full_name, position, adp "
        "(overrides --source)",
    )
    args = parser.parse_args()

    # ---------- ADP ----------
    if args.adp_file:
        ext = Path(args.adp_file).suffix.lower()
        if ext == ".csv":
            adp_raw = pd.read_csv(args.adp_file)
        elif ext in (".json", ".ndjson"):
            adp_raw = pd.read_json(args.adp_file)
        else:
            print(f"[red]Unsupported file extension: {ext}[/red]")
            return
        adp = _map_to_players(adp_raw, args.season)
    else:
        adp = _load_parquet(f"adp/season={args.season}/source={args.source}")
        if adp.empty or "adp" not in adp.columns:
            try:
                adp = ingest_adp(args.season, source=args.source, teams=args.teams)
            except ADPError as e:
                print(
                    f"[yellow]ADP fetch failed – {e}. "
                    "Draft board will omit ADP columns.[/yellow]"
                )
                adp = pd.DataFrame()

    # ---------- VOR ----------
    vor_df = _load_parquet("vor")
    if vor_df.empty:
        print("[yellow]No VOR data – compute season totals first.[/yellow]")
        return

    board = vor.attach_adp(vor_df, adp)
    exclude = ["DB", "DL", "LB", "P"]
    board = board[~board["position"].isin(exclude)]
    name_map = build_xwalk(
        args.season
    ).rename(  # returns columns: gsis_id, sleeper_id, full_name, position
        columns={"sleeper_id": "player_id"}
    )[
        ["player_id", "full_name"]
    ]
    board = board.merge(name_map, on="player_id", how="left")

    # Move full_name up front and drop raw IDs if you like
    board = board.rename(columns={"full_name": "player_name"})
    cols = ["player_name"] + [c for c in board.columns if c != "player_name"]
    board = board[cols]
    board = board.sort_values(
        ["tier", "value_vs_adp", "fantasy_pts_season"], ascending=[True, False, False]
    )

    table = Table(title=f"Draft Board {args.season}")
    # show player_name instead of player_id
    display_cols = [
        "player_name",
        "position",
        "fantasy_pts_season",
        "vor",
        "tier",
        "adp",
        "value_vs_adp",
    ]
    for col in display_cols:
        table.add_column(col.replace("_", " ").title())

    for _, row in board.head(150).iterrows():
        table.add_row(
            *(
                f"{x:.2f}" if isinstance(x, float) and not pd.isna(x) else str(x)
                for x in row[display_cols]
            )
        )

    print(table)


if __name__ == "__main__":
    draft_board()
