import shutil
from pathlib import Path

import pytest
from fastapi import HTTPException

from app.services import extract as extract_mod
from app.services.extract import extract_snapshot


def _seed_raw_html(tmp_path: Path, ticker: str, fixture_name: str) -> Path:
    """Copy a fixture into the expected data/raw/{TICKER}/YYYY-MM-DD_*.html spot."""
    raw_dir = tmp_path / "raw" / ticker
    raw_dir.mkdir(parents=True, exist_ok=True)
    src = Path("tests/fixtures") / fixture_name
    dst = (
        raw_dir / "2025-01-01_press.html"
    )  # any name; latest_file_for_ticker will pick it
    shutil.copyfile(src, dst)
    return dst


def test_extract_from_fixture_html(tmp_path: Path, monkeypatch):
    # Point the extractor to a temp DATA_DIR
    extract_mod.DATA_DIR = tmp_path

    # Seed one HTML file for MSFT
    path = _seed_raw_html(tmp_path, "MSFT", "msft_press.html")

    snap = extract_snapshot("MSFT")

    assert snap.ticker == "MSFT"
    assert snap.source_path == str(path)
    # 62.0 billion -> 62_000_000_000.0
    assert abs(snap.headline.revenue - 62_000_000_000.0) < 1e-6
    assert abs(snap.headline.eps_diluted - 2.94) < 1e-6


def test_extract_422_when_missing_numbers(tmp_path: Path, monkeypatch):
    extract_mod.DATA_DIR = tmp_path
    _seed_raw_html(tmp_path, "AAPL", "missing_eps.html")

    with pytest.raises(HTTPException) as exc:
        extract_snapshot("AAPL")

    assert exc.value.status_code == 422
    assert "Missing revenue or EPS" in exc.value.detail
