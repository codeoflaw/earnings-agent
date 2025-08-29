import os
from pathlib import Path


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

MAX_BYTES = _int("INGEST_MAX_BYTES", 20 * 1024 * 1024)  # 20 MB
CONNECT_TIMEOUT = float(os.getenv("INGEST_CONNECT_TIMEOUT", "5"))
READ_TIMEOUT = float(os.getenv("INGEST_READ_TIMEOUT", "25"))
WRITE_TIMEOUT = float(os.getenv("INGEST_WRITE_TIMEOUT", "10"))
POOL_TIMEOUT = float(os.getenv("INGEST_POOL_TIMEOUT", "5"))

USER_AGENT = os.getenv("USER_AGENT", "earnings-agent (+contact@example.com)")

ALLOWED_CONTENT_TYPES = tuple(
    ct.strip().lower()
    for ct in os.getenv("INGEST_ALLOWED_TYPES", "text/html,application/pdf").split(",")
)

IDEMPOTENCY_TTL_SECONDS = _int("INGEST_IDEMPOTENCY_TTL_SECONDS", 600)  # 10 minutes
