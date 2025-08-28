from datetime import date
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx

DATA_DIR = Path("data")
MAX_BYTES = 20 * 1024 * 1024  # 20 MB
TIMEOUT = httpx.Timeout(connect=5.0, read=25.0, write=10.0, pool=5.0)
DEFAULT_UA = "earnings-agent (+contact@example.com)"


class IngestTooLarge(Exception): ...


class IngestUnsupportedType(Exception): ...


def ensure_ticker_dir(ticker: str) -> Path:
    p = DATA_DIR / "raw" / ticker.upper()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _ext_from_content_type(ct: Optional[str]) -> str:
    if not ct:
        return ".bin"
    ct = ct.split(";")[0].strip().lower()
    if ct in {"text/html"}:
        return ".html"
    if ct in {"application/pdf"}:
        return ".pdf"
    return ".bin"


def guess_extension_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".pdf"):
        return ".pdf"
    if path.endswith(".html") or path.endswith(".htm"):
        return ".html"
    return ".bin"


def build_save_path(ticker: str, url: str, content_type: Optional[str] = None) -> Path:
    folder = ensure_ticker_dir(ticker)
    basename = Path(urlparse(url).path).name or "download"
    basename = basename.split("?")[0] or "download"
    # Decide extension: prefer content-type if known, else URL
    ext = (
        _ext_from_content_type(content_type)
        if content_type
        else guess_extension_from_url(url)
    )
    if not basename.endswith(ext):
        basename = f"{basename}{ext}"
    return folder / f"{date.today().isoformat()}_{basename}"


def fetch_to_disk(
    ticker: str, url: str, client: Optional[httpx.Client] = None
) -> tuple[Path, str, int]:
    """
    Download `url`, enforce size limits, and save to data/raw/{TICKER}/YYYY-MM-DD_*.ext.
    Returns: (saved_path, content_type, num_bytes)
    Raises: IngestTooLarge, IngestUnsupportedType, httpx.RequestError, httpx.HTTPStatusError
    """
    close_client = False
    if client is None:
        headers = {"User-Agent": DEFAULT_UA}
        client = httpx.Client(follow_redirects=True, timeout=TIMEOUT, headers=headers)
        close_client = True

    try:
        # HEAD first (best effort) to catch huge files quickly
        ct = None
        cl = None
        try:
            head = client.head(url)
            if head.status_code < 400:
                ct = head.headers.get("content-type")
                cl = head.headers.get("content-length")
        except httpx.RequestError:
            # Some origins don't allow HEAD; continue with GET
            pass

        if cl and cl.isdigit() and int(cl) > MAX_BYTES:
            raise IngestTooLarge(f"Content-Length {cl} exceeds limit {MAX_BYTES}")

        # Stream GET
        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            content_type = resp.headers.get("content-type") or ct
            save_path = build_save_path(ticker, url, content_type)

            bytes_written = 0
            with open(save_path, "wb") as f:
                for chunk in resp.iter_bytes():
                    if chunk:
                        bytes_written += len(chunk)
                        if bytes_written > MAX_BYTES:
                            f.close()
                            save_path.unlink(missing_ok=True)
                            raise IngestTooLarge(f"Downloaded > {MAX_BYTES} bytes")
                        f.write(chunk)

        # Optional: enforce type allowlist
        if content_type and not any(
            content_type.startswith(t) for t in ("text/html", "application/pdf")
        ):
            # Keep the bytes (useful later), but signal "unsupported" to the caller
            raise IngestUnsupportedType(content_type)

        return save_path, (content_type or "application/octet-stream"), bytes_written
    finally:
        if close_client:
            client.close()
