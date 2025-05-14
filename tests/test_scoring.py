import pandas as pd
from ffwb.scoring import score_weekly


def test_score_weekly_basic():
    df = pd.DataFrame(
        {
            "player_id": ["demo"],
            "pass_yds": [300],
            "pass_tds": [3],
            "rush_yds": [20],
        }
    )
    rules = {"pass_yds": 0.04, "pass_tds": 4, "rush_yds": 0.1}
    out = score_weekly(df, rules)
    expected = 300 * 0.04 + 3 * 4 + 20 * 0.1  # 12 + 12 + 2 = 26
    assert out.loc[0, "fantasy_pts"] == expected
