import httpx

from app.services import ingest as ingest_mod
from app.services.ingest import fetch_to_disk


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


def test_idempotent_cache_expired(tmp_path, monkeypatch):
    # Point to tmp & set very small TTL
    monkeypatch.setenv("INGEST_IDEMPOTENCY_TTL_SECONDS", "0")  # expire immediately

    # Re-import config values in module under test (simple way for this project)
    from importlib import reload

    from app import config as config_mod

    reload(config_mod)
    reload(ingest_mod)
    # After reload, functions are rebound; import fetch again:
    from app.services.ingest import fetch_to_disk

    ingest_mod.DATA_DIR = tmp_path
    ingest_mod.INDEX_FILE = ingest_mod.DATA_DIR / ".ingest_index.json"

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

    p1, _, _ = fetch_to_disk("AAPL", url, client=client)
    p2, _, _ = fetch_to_disk("AAPL", url, client=client)  # TTL=0 forces second GET
    assert calls["get"] >= 2
    assert p1.exists() and p2.exists()
