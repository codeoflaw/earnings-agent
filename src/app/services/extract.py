import re
from pathlib import Path

from bs4 import BeautifulSoup
from fastapi import HTTPException

from app.config import DATA_DIR
from app.schemas.extract import CompanySnapshot, Headline


def latest_file_for_ticker(ticker: str) -> Path:
    folder = DATA_DIR / "raw" / ticker.upper()
    if not folder.exists():
        raise HTTPException(status_code=404, detail=f"No raw data for {ticker}")
    candidates = sorted(folder.glob("*.html"), reverse=True)
    if not candidates:
        raise HTTPException(status_code=404, detail=f"No HTML files found for {ticker}")
    return candidates[0]


def parse_revenue_and_eps(html: str) -> Headline:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    rev_pattern = r"Revenue.*?\$?([\d,\.]+)\s*(billion|million)?"
    eps_pattern = r"Diluted EPS.*?\$?([\d,\.]+)[,\s]"

    rev_match = re.search(rev_pattern, text, re.IGNORECASE)
    eps_match = re.search(eps_pattern, text, re.IGNORECASE)

    if not rev_match or not eps_match:
        raise HTTPException(status_code=422, detail="Missing revenue or EPS")

    revenue_val = float(rev_match.group(1).replace(",", ""))
    if rev_match.group(2) and rev_match.group(2).lower().startswith("b"):
        revenue_val *= 1_000_000_000
    elif rev_match.group(2) and rev_match.group(2).lower().startswith("m"):
        revenue_val *= 1_000_000

    eps_val = float(eps_match.group(1).replace(",", ""))

    return Headline(revenue=revenue_val, eps_diluted=eps_val)


def extract_snapshot(ticker: str) -> CompanySnapshot:
    path = latest_file_for_ticker(ticker)
    html = path.read_text(errors="ignore")
    headline = parse_revenue_and_eps(html)
    return CompanySnapshot(
        ticker=ticker.upper(), headline=headline, source_path=str(path)
    )
