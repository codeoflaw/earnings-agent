import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from app.config import (
    ALLOWED_CONTENT_TYPES,
    CONNECT_TIMEOUT,
    DATA_DIR,
    IDEMPOTENCY_TTL_SECONDS,
    MAX_BYTES,
    POOL_TIMEOUT,
    READ_TIMEOUT,
    USER_AGENT,
    WRITE_TIMEOUT,
)
from app.schemas.ingest import TICKER_PATTERN

INDEX_FILE = DATA_DIR / ".ingest_index.json"

log = logging.getLogger(__name__)


class IngestTooLarge(Exception): ...


class IngestUnsupportedType(Exception): ...


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _index_load() -> dict[str, dict]:
    try:
        if INDEX_FILE.exists():
            return json.loads(INDEX_FILE.read_text())
    except Exception:
        log.warning("ingest.index_load_failed path=%s", INDEX_FILE)
    return {}


def _index_save(index: dict[str, dict]) -> None:
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = INDEX_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(index, ensure_ascii=False, indent=2))
    tmp.replace(INDEX_FILE)


def _index_key(ticker: str, url: str) -> str:
    return f"{ticker.upper()}|{url}"


def _index_get_recent(ticker: str, url: str) -> tuple[Path, str, int] | None:
    """Return cached path/ctype/bytes if entry is within TTL and file exists."""
    index = _index_load()
    key = _index_key(ticker, url)
    row = index.get(key)
    if not row:
        return None
    try:
        saved_at = datetime.fromisoformat(row["saved_at"])
    except Exception:
        return None
    if _now_utc() - saved_at > timedelta(seconds=IDEMPOTENCY_TTL_SECONDS):
        return None
    p = Path(row["saved_path"])
    if not p.exists():
        return None
    return (
        p,
        row.get("content_type") or "application/octet-stream",
        int(row.get("bytes") or 0),
    )


def _index_put(
    ticker: str, url: str, path: Path, content_type: str, nbytes: int
) -> None:
    index = _index_load()
    index[_index_key(ticker, url)] = {
        "saved_path": str(path),
        "content_type": content_type,
        "bytes": int(nbytes),
        "saved_at": _now_utc().isoformat(),
    }
    _index_save(index)


def ensure_ticker_dir(ticker: str) -> Path:
    p = DATA_DIR / "raw" / ticker.upper()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _is_allowed_content_type(content_type: Optional[str]) -> bool:
    if not content_type:
        return False
    ct = content_type.split(";")[0].strip().lower()
    return any(ct.startswith(p) for p in ALLOWED_CONTENT_TYPES)


def _get_extension(content_type: Optional[str], url: str) -> str:
    """Get file extension from content type (preferred) or URL."""
    if content_type:
        ct = content_type.split(";")[0].strip().lower()
        if ct in {"text/html"}:
            return ".html"
        if ct in {"application/pdf"}:
            return ".pdf"
    # Fall back to URL-based extension
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
    ext = _get_extension(content_type, url)
    if not basename.endswith(ext):
        basename = f"{basename}{ext}"
    date_prefix = date.today().strftime("%Y%m%d")
    return folder / f"{date_prefix}_{basename}"


def _mk_client() -> httpx.Client:
    timeout = httpx.Timeout(
        connect=CONNECT_TIMEOUT,
        read=READ_TIMEOUT,
        write=WRITE_TIMEOUT,
        pool=POOL_TIMEOUT,
    )
    return httpx.Client(
        follow_redirects=True, timeout=timeout, headers={"User-Agent": USER_AGENT}
    )


def _is_retryable(exc: BaseException) -> bool:
    # Network errors and 5xx are retryable
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError) and 500 <= exc.response.status_code < 600:
        return True
    return False


@retry(
    retry=retry_if_exception(_is_retryable),
    wait=wait_exponential(multiplier=0.5, min=1, max=8),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def fetch_to_disk(
    ticker: str, url: str, client: Optional[httpx.Client] = None
) -> tuple[Path, str, int]:
    """
    Download `url`, enforce size limits, save to data/raw/{TICKER}/YYYY-MM-DD_*.ext.
    Retries transient network errors and 5xx up to 3 attempts (exponential backoff).
    Returns: (saved_path, content_type, num_bytes)
    """
    # Validate ticker format at service level
    if not re.match(TICKER_PATTERN, ticker.upper()):
        raise ValueError(f"Invalid ticker format: {ticker}")

    cached = _index_get_recent(ticker, url)
    if cached:
        path, ctype, nbytes = cached
        log.info("ingest.cache_hit ticker=%s url=%s path=%s", ticker, url, path)
        return path, ctype, nbytes

    close_client = False
    if client is None:
        client = _mk_client()
        close_client = True

    try:
        ct = None
        cl = None
        try:
            head = client.head(url)
            if head.status_code < 400:
                ct = head.headers.get("content-type")
                cl = head.headers.get("content-length")
                if ct and not _is_allowed_content_type(ct):
                    raise IngestUnsupportedType(ct)
        except httpx.RequestError:
            pass

        if cl and cl.isdigit() and int(cl) > MAX_BYTES:
            raise IngestTooLarge(f"Content-Length {cl} exceeds limit {MAX_BYTES}")

        with client.stream("GET", url) as resp:
            resp.raise_for_status()
            content_type = resp.headers.get("content-type") or ct
            save_path = build_save_path(ticker, url, content_type)

            bytes_written = 0
            with open(save_path, "wb") as f:
                for chunk in resp.iter_bytes():
                    if not chunk:
                        continue
                    bytes_written += len(chunk)
                    if bytes_written > MAX_BYTES:
                        save_path.unlink(missing_ok=True)
                        raise IngestTooLarge(f"Downloaded > {MAX_BYTES} bytes")
                    f.write(chunk)

            if (
                content_type
                and content_type != ct
                and not _is_allowed_content_type(content_type)
            ):
                save_path.unlink(missing_ok=True)
                raise IngestUnsupportedType(content_type)

            log.info(
                "ingest.saved ticker=%s bytes=%s type=%s path=%s",
                ticker,
                bytes_written,
                content_type,
                save_path,
            )

        _index_put(
            ticker,
            url,
            save_path,
            content_type or "application/octet-stream",
            bytes_written,
        )

        return save_path, (content_type or "application/octet-stream"), bytes_written
    finally:
        if close_client:
            client.close()
