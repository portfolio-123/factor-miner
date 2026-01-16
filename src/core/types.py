from enum import StrEnum
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field

from src.core.constants import AnalysisStatus


class TokenPayload(BaseModel):
    apiId: int
    apiKey: str


class ScalingMethod(StrEnum):
    NORMAL = "normal"
    MINMAX = "minmax"
    RANK = "rank"


class ScopeType(StrEnum):
    DATASET = "dataset"
    DATE = "date"


class AnalysisParams(BaseModel):
    dataset_path: str
    top_pct: float
    bottom_pct: float
    min_alpha: float
    benchmark_data: Optional[str] = None
    benchmark_ticker: str
    access_token: Optional[str] = None


class NormalizationConfig(BaseModel):

    model_config = ConfigDict(extra="ignore")

    scaling: str
    scope: str
    trimPct: float
    outliers: str
    outlierLimit: float
    mlTrainingEnd: str
    naFill: str


class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str | None = None
    description: str | None = None
    universeName: str
    frequency: int
    currency: str
    startDt: str
    endDt: str
    benchmark: str = Field(alias="benchName")
    precision: int
    normalization: bool
    preprocessor: NormalizationConfig
    factorCount: int | None = None
    pitMethod: int


class AnalysisSummary(BaseModel):
    id: str
    name: str | None = None
    status: AnalysisStatus
    created_at: str
    dataset_version: str | None = None
    params: AnalysisParams


class Analysis(AnalysisSummary):
    updated_at: str | None = None
    results: Dict[str, Any] | None = None
    error: str | None = None
