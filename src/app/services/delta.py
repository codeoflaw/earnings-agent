from __future__ import annotations

import json
import re
from typing import Literal, Optional, Pattern, TypedDict

from app.config import DATA_DIR
from app.schemas.ingest import TICKER_PATTERN

BaselineKind = Literal["qoq", "yoy"]

_TICKER_RE: Pattern[str] = re.compile(TICKER_PATTERN)


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


def load_baseline(ticker: str, kind: BaselineKind) -> Headline | None:
    """Load baseline data for comparing financial metrics.

    Args:
        ticker: Company stock symbol
        kind: Type of baseline - "qoq" (quarter-over-quarter) or "yoy" (year-over-year)

    Returns:
        Headline with baseline metrics or None if not found

    Raises:
        ValueError: If ticker format is invalid
    """
    if not _TICKER_RE.match(ticker.upper()):
        raise ValueError(f"Invalid ticker format: {ticker}")

    p = DATA_DIR / "parsed" / ticker.upper() / f"{kind}_baseline.json"
    if not p.exists():
        return None
    return json.loads(p.read_text()).get("headline")
