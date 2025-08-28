from datetime import date
from pathlib import Path
from urllib.parse import urlparse

DATA_DIR = Path("data")

def ensure_ticker_dir(ticker: str) -> Path:
    p = DATA_DIR / "raw" / ticker.upper()
    p.mkdir(parents=True, exist_ok=True)
    return p

def guess_extension_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".pdf"):
        return ".pdf"
    if path.endswith(".html") or path.endswith(".htm"):
        return ".html"
    return ".bin"  # we'll refine after we add content-type probing

def build_save_path(ticker: str, url: str) -> Path:
    folder = ensure_ticker_dir(ticker)
    ext = guess_extension_from_url(url)
    # YYYY-MM-DD_basename.ext (basename without query)
    basename = Path(urlparse(url).path).name or "download"
    basename = basename.split("?")[0] or "download"
    return folder / f"{date.today().isoformat()}_{basename}{'' if basename.endswith(ext) else ext}"
