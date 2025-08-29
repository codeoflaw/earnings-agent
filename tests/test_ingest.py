import re

import httpx

from app.services.ingest import IngestTooLarge, fetch_to_disk


def test_fetch_to_disk_html(tmp_path, monkeypatch):
    # Arrange a fake HTML response
    html = b"<html><body><h1>Press Release</h1></body></html>"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={"content-type": "text/html", "content-length": str(len(html))},
            )
        return httpx.Response(200, headers={"content-type": "text/html"}, content=html)

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)

    # Put DATA_DIR under tmp_path to keep filesystem clean
    from app.services import ingest as ingest_mod

    ingest_mod.DATA_DIR = tmp_path

    # Act
    path, content_type, nbytes = fetch_to_disk(
        "MSFT", "https://example.com/press.html", client=client
    )

    # Assert
    assert path.exists()
    assert path.read_bytes() == html
    assert content_type.startswith("text/html")
    assert nbytes == len(html)
    assert str(path.parent).endswith("/raw/MSFT")
    assert re.match(r"\d{8}_.*\.html", path.name)


def test_fetch_too_large(tmp_path):
    big = b"x" * (21 * 1024 * 1024)  # 21 MB

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(
                200,
                headers={
                    "content-type": "application/pdf",
                    "content-length": str(len(big)),
                },
            )
        return httpx.Response(
            200, headers={"content-type": "application/pdf"}, content=big
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))

    from app.services import ingest as ingest_mod

    ingest_mod.DATA_DIR = tmp_path

    try:
        fetch_to_disk("AAPL", "https://example.com/file.pdf", client=client)
        assert False, "expected IngestTooLarge"
    except IngestTooLarge:
        pass
