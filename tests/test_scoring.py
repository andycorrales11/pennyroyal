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


def test_aggregate_season_sum():
    import pandas as pd
    from ffwb.scoring import aggregate_season

    weekly = pd.DataFrame(
        {
            "player_id": ["x", "x"],
            "season": [2023, 2023],
            "week": [1, 2],
            "fantasy_pts": [10.0, 15.0],
        }
    )
    total = aggregate_season(weekly)
    assert len(total) == 1
    assert total.loc[0, "fantasy_pts_season"] == 25.0
