from pydantic import AnyHttpUrl, BaseModel, Field

TICKER_PATTERN = r"^[A-Z0-9\-\.]+$"

class IngestRequest(BaseModel):
    url: AnyHttpUrl  # ensures valid http/https


class IngestResult(BaseModel):
    ticker: str = Field(min_length=1, max_length=8, pattern=TICKER_PATTERN)
    source_url: AnyHttpUrl
    saved_path: str | None = None
    content_type: str | None = None
    bytes: int | None = None
