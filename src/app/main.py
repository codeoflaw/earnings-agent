from fastapi import FastAPI, HTTPException, Path

from app.schemas.ingest import IngestRequest, IngestResult
from app.services.ingest import build_save_path

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ingest/{ticker}", response_model=IngestResult)
async def ingest_stub(
    ticker: str = Path(..., min_length=1, max_length=8, pattern=r"^[A-Z0-9\-\.]+$"),
    req: IngestRequest | None = None,
):
    if req is None:
        raise HTTPException(status_code=400, detail="Body required")

    save_path = build_save_path(ticker, str(req.url))
    return IngestResult(
        ticker=ticker.upper(),
        source_url=req.url,
        saved_path=str(save_path),  # preview only—no file written yet
        content_type=None,
        bytes=None,
    )


