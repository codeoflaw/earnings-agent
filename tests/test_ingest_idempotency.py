import httpx
import pytest

from app.services import ingest as ingest_mod
from app.services.ingest import fetch_to_disk


@pytest.fixture
def zero_ttl_config(monkeypatch):
    """Configure ingest service with zero TTL for testing cache expiration"""
    # Set zero TTL
    monkeypatch.setattr(ingest_mod, "IDEMPOTENCY_TTL_SECONDS", 0)

    yield

    # No need to restore as monkeypatch handles cleanup


def test_idempotent_cache_hit(tmp_path, monkeypatch):
    # Point DATA_DIR (and thus index file) to tmp
    ingest_mod.DATA_DIR = tmp_path
    ingest_mod.INDEX_FILE = ingest_mod.DATA_DIR / ".ingest_index.json"

    # Simulate server (will count GETs)
    html = b"<html>press</html>"
    calls = {"get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-type": "text/html", "content-length": str(len(html))},
            )
        calls["get"] += 1
        return httpx.Response(200, headers={"content-type": "text/html"}, content=html)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    url = "https://example.com/press.html"

    # First call downloads and writes index
    p1, c1, n1 = fetch_to_disk("MSFT", url, client=client)
    assert calls["get"] == 1
    assert p1.exists()

    # Second call should be served from cache (no additional GET)
    p2, c2, n2 = fetch_to_disk("MSFT", url, client=client)
    assert calls["get"] == 1, "expected no second GET (cache hit)"
    assert p2 == p1 and c2 == c1 and n2 == n1


def test_idempotent_cache_expired(tmp_path, zero_ttl_config):
    # Point DATA_DIR to tmp
    ingest_mod.DATA_DIR = tmp_path
    ingest_mod.INDEX_FILE = ingest_mod.DATA_DIR / ".ingest_index.json"

    # Simulate server (will count GETs)
    html = b"<html>press</html>"
    calls = {"get": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-type": "text/html", "content-length": str(len(html))},
            )
        calls["get"] += 1
        return httpx.Response(200, headers={"content-type": "text/html"}, content=html)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    url = "https://example.com/press.html"

    # First call downloads and writes index
    p1, c1, n1 = fetch_to_disk("MSFT", url, client=client)
    assert calls["get"] == 1
    assert p1.exists()

    # Second call should trigger new GET (cache expired)
    p2, c2, n2 = fetch_to_disk("MSFT", url, client=client)
    assert calls["get"] == 2, "expected second GET (cache expired)"
    assert p2 == p1 and c2 == c1 and n2 == n1
