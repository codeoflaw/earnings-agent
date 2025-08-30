from __future__ import annotations

import json
from typing import Optional, TypedDict

from app.config import DATA_DIR


class Headline(TypedDict, total=False):
    revenue: float
    eps_diluted: float


class DeltaResult(TypedDict, total=False):
    revenue_yoy_pct: Optional[float]
    revenue_qoq_pct: Optional[float]
    eps_yoy_pct: Optional[float]
    eps_qoq_pct: Optional[float]


def _pct_change(current: Optional[float], prior: Optional[float]) -> Optional[float]:
    if current is None or prior is None:
        return None
    if prior == 0:
        return None  # avoid div-by-zero; undefined change
    return round(((current - prior) / prior) * 100.0, 2)


def compute_deltas(
    current: Headline,
    yoy_baseline: Optional[Headline] = None,
    qoq_baseline: Optional[Headline] = None,
) -> DeltaResult:
    """Compute YoY and QoQ % changes for revenue and EPS."""
    return {
        "revenue_yoy_pct": _pct_change(
            current.get("revenue"), (yoy_baseline or {}).get("revenue")
        ),
        "revenue_qoq_pct": _pct_change(
            current.get("revenue"), (qoq_baseline or {}).get("revenue")
        ),
        "eps_yoy_pct": _pct_change(
            current.get("eps_diluted"), (yoy_baseline or {}).get("eps_diluted")
        ),
        "eps_qoq_pct": _pct_change(
            current.get("eps_diluted"), (qoq_baseline or {}).get("eps_diluted")
        ),
    }


def load_baseline(ticker: str, kind: str) -> Headline | None:
    """
    kind: 'qoq' or 'yoy'. Looks for data/parsed/{ticker}/{kind}_baseline.json
    """
    p = DATA_DIR / "parsed" / ticker.upper() / f"{kind}_baseline.json"
    if not p.exists():
        return None
    return json.loads(p.read_text()).get("headline")
