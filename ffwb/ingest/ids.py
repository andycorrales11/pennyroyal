# ffwb/ingest/ids.py
import pandas as pd
from nfl_data_py import import_rosters


def build_xwalk(season: int) -> pd.DataFrame:
    """
    Returns df with columns:
      sleeper_id, gsis_id, full_name, position
    """
    roster = import_rosters([season])
    xwalk = roster[["player_id", "gsis_id", "display_name", "position"]]
    xwalk = xwalk.rename(
        columns={"player_id": "sleeper_id", "display_name": "full_name"}
    )
    return xwalk
