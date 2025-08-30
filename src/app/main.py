import httpx
from fastapi import FastAPI, HTTPException, Path

from app.schemas.extract import CompanySnapshot
from app.schemas.ingest import TICKER_PATTERN, IngestRequest, IngestResult
from app.services.extract import extract_snapshot
from app.services.ingest import IngestTooLarge, IngestUnsupportedType, fetch_to_disk

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ingest/{ticker}", response_model=IngestResult)
def ingest(
    req: IngestRequest,
    ticker: str = Path(..., min_length=1, max_length=8, pattern=TICKER_PATTERN),
):
    try:
        path, content_type, nbytes = fetch_to_disk(ticker, str(req.url))
        return IngestResult(
            ticker=ticker.upper(),
            source_url=req.url,
            saved_path=str(path),
            content_type=content_type,
            bytes=nbytes,
        )
    except IngestTooLarge as e:
        raise HTTPException(status_code=422, detail=str(e))
    except IngestUnsupportedType as e:
        raise HTTPException(status_code=422, detail=f"Unsupported content-type: {e}")
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=502, detail=f"Upstream error: {e.response.status_code}"
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=504, detail=f"Network error: {e}")


@app.post("/extract/{ticker}", response_model=CompanySnapshot)
def extract(ticker: str):
    return extract_snapshot(ticker)
