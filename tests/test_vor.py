import pandas as pd
from ffwb.vor import compute_vor


def test_compute_vor_quantile():
    totals = pd.DataFrame(
        {
            "player_id": ["A", "B", "C"],
            "position": ["QB", "QB", "QB"],
            "fantasy_pts_season": [400, 350, 250],
        }
    )
    out = compute_vor(
        totals,
        roster_settings={"qb": 1},
        num_teams=1,
        tier_method="quantile",
        q=0.5,
    )
    assert out.loc[out["player_id"] == "A", "vor"].iloc[0] == 50  # 400‑350
    # tier split 50%: top player tier 1, others tier 2 or 99
    assert out.loc[out["player_id"] == "A", "tier"].iloc[0] == 1
