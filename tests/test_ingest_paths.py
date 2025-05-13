from ffwb.ingest import io


def test_data_dir_created(tmp_path, monkeypatch):
    monkeypatch.setattr(io, "_DATA_ROOT", tmp_path)
    import pandas as pd

    df = pd.DataFrame({"player_id": ["x"], "proj_pts": [1.23]})
    path = io.to_parquet(df, "projection_weekly")
    assert path.exists()
