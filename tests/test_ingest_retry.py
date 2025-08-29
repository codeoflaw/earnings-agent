# tests/test_ingest_retry.py
import httpx

from app.services import ingest as ingest_mod
from app.services.ingest import fetch_to_disk


def test_retry_then_success(tmp_path, monkeypatch):
    ingest_mod.DATA_DIR = tmp_path

    calls = {"n": 0}
    html = b"<html>ok</html>"

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(502, text="bad gateway")
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-type": "text/html", "content-length": str(len(html))},
            )
        return httpx.Response(200, headers={"content-type": "text/html"}, content=html)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    path, ctype, nbytes = fetch_to_disk(
        "TEST", "https://example.com/x.html", client=client
    )
    assert calls["n"] >= 2
    assert path.exists() and nbytes == len(html) and ctype.startswith("text/html")
