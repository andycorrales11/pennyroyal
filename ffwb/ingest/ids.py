# ffwb/ingest/ids.py
import pandas as pd
from ._nfl_compat import import_rosters


def build_xwalk(season: int) -> pd.DataFrame:
    """
    Return a DataFrame with the canonical columns
    sleeper_id, gsis_id, full_name, position
    regardless of nfl_data_py column drift.
    """
    roster = import_rosters([season])

    # -------- column fallbacks ----------
    name_col = "display_name" if "display_name" in roster.columns else "player_name"
    gsis_col = "gsis_id" if "gsis_id" in roster.columns else "gsis_it_id"

    xwalk = roster[["player_id", gsis_col, name_col, "position"]].rename(
        columns={
            "player_id": "sleeper_id",
            gsis_col: "gsis_id",
            name_col: "full_name",
        }
    )
    return xwalk
