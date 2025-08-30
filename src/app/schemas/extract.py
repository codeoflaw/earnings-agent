from pydantic import BaseModel, Field


class Headline(BaseModel):
    revenue: float = Field(..., description="Total revenue for the quarter")
    eps_diluted: float = Field(..., description="Diluted EPS")


class CompanySnapshot(BaseModel):
    ticker: str
    headline: Headline
    source_path: str
