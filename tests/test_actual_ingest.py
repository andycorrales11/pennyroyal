from ffwb.ingest import nflfast


def test_actual_ingest(tmp_path, monkeypatch):
    from ffwb.ingest import io

    monkeypatch.setattr(io, "_DATA_ROOT", tmp_path)

    df = nflfast.ingest_actual_weekly(season=2023, weeks=[1])
    assert not df.empty
    assert {"player_id", "week", "season"}.issubset(df.columns)
