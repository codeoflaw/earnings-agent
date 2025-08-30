import json

from app.services.delta import compute_deltas


def _load(path: str):
    with open(path) as f:
        return json.load(f)["headline"]


def test_compute_deltas_happy_path(tmp_path):
    current = {"revenue": 62_000_000_000.0, "eps_diluted": 2.94}
    qoq = _load("tests/fixtures/sample_last_quarter.json")
    yoy = _load("tests/fixtures/sample_last_year.json")

    d = compute_deltas(current, yoy_baseline=yoy, qoq_baseline=qoq)

    # (62 - 54) / 54 = 14.81%
    assert d["revenue_yoy_pct"] == 14.81
    # (62 - 57.5) / 57.5 = 7.83%
    assert d["revenue_qoq_pct"] == 7.83
    # (2.94 - 2.40) / 2.40 = 22.5%
    assert d["eps_yoy_pct"] == 22.5
    # (2.94 - 2.75) / 2.75 = 6.91%
    assert d["eps_qoq_pct"] == 6.91


def test_compute_deltas_handles_zero_and_missing():
    current = {"revenue": 10.0}
    qoq = {"revenue": 0.0, "eps_diluted": 0.0}
    yoy = {"revenue": None, "eps_diluted": 2.0}  # type: ignore

    d = compute_deltas(current, yoy_baseline=yoy, qoq_baseline=qoq)

    # prior 0 => None, missing prior => None, missing current metric => None
    assert d["revenue_qoq_pct"] is None
    assert d["revenue_yoy_pct"] is None
    assert d["eps_qoq_pct"] is None
    assert d["eps_yoy_pct"] is None


def test_negative_delta_rounding():
    current = {"revenue": 90.0, "eps_diluted": 1.8}
    qoq = {"revenue": 100.0, "eps_diluted": 2.0}
    d = compute_deltas(current, qoq_baseline=qoq)
    assert d["revenue_qoq_pct"] == -10.0
    assert d["eps_qoq_pct"] == -10.0
