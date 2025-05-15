from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
from rich import print
from rich.table import Table

from ffwb.ingest.adp import ingest_adp, ADPError, _map_to_players

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
        if adp.empty:
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

    board = vor.attach_adp(vor_df, adp).sort_values(
        ["position", "tier", "value_vs_adp"], ascending=[True, True, False]
    )

    table = Table(title=f"Draft Board {args.season}")
    for col in [
        "player_id",
        "position",
        "fantasy_pts_season",
        "vor",
        "tier",
        "adp",
        "value_vs_adp",
    ]:
        table.add_column(col)

    for _, row in board.head(150).iterrows():
        table.add_row(
            *(
                f"{x:.2f}" if isinstance(x, float) and not pd.isna(x) else str(x)
                for x in row[
                    [
                        "player_id",
                        "position",
                        "fantasy_pts_season",
                        "vor",
                        "tier",
                        "adp",
                        "value_vs_adp",
                    ]
                ]
            )
        )

    print(table)


if __name__ == "__main__":
    draft_board()
