import pytest
from ffwb.ingest.adp import ingest_adp, ADPError


def test_fantasypros_adp(monkeypatch, tmp_path):
    from ffwb.ingest import io

    # redirect Parquet output to temp dir
    monkeypatch.setattr(io, "_DATA_ROOT", tmp_path)

    try:
        df = ingest_adp(season=2024, source="fantasypros")
    except ADPError:
        pytest.skip("FantasyPros feed unavailable; skipped ADP ingest test.")
    else:
        assert not df.empty and "player_id" in df.columns
