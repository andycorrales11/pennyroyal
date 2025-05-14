# ffwb/ingest/_nfl_compat.py
from importlib import import_module

nfld = import_module("nfl_data_py")

# Roster loader
if hasattr(nfld, "import_rosters"):
    import_rosters = nfld.import_rosters
else:  # new name
    import_rosters = nfld.import_seasonal_rosters  # type: ignore[attr-defined]

# Weekly stats loader
if hasattr(nfld, "import_weekly_data"):
    import_weekly_data = nfld.import_weekly_data
else:  # new name
    import_weekly_data = nfld.import_weekly_stats  # type: ignore[attr-defined]
