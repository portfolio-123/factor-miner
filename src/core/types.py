from pydantic import BaseModel


class AnalysisParams(BaseModel):
    dataset_path: str
    top_pct: float
    bottom_pct: float
    min_alpha: float
    benchmark_data: str
    benchmark_ticker: str
