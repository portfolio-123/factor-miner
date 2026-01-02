from enum import StrEnum
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field


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
    api_key: Optional[str] = None
    api_id: Optional[str] = None


class NormalizationConfig(BaseModel):

    model_config = ConfigDict(extra="ignore")

    scaling: ScalingMethod | None = None
    scope: ScopeType | None = None
    trimPct: float | None = None
    outliers: bool | None = None
    outlierLimit: float | None = None
    mlTrainingEnd: str | None = None
    naFill: bool | None = None


class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str | None = Field(default=None)
    description: str | None = Field(default=None)

    flName: Optional[str] = Field(default="Unknown Name")
    universeName: str = Field(default="Unknown Universe")
    frequency: int = Field(default=1)
    currency: str = Field(default="USD")
    startDt: str | None = Field(default=None)
    endDt: str | None = Field(default=None)
    benchmark: str | None = Field(default=None)
    precision: str | None = Field(default=None)
    normalization: NormalizationConfig | None = Field(default=None)
    factorCount: int | None = Field(default=None)
    pitMethod: str | None = Field(default=None)


class Job(BaseModel):
    id: str
    name: str | None = None
    status: str
    created_at: str
    updated_at: str | None = None
    dataset_version: str | None = None
    params: AnalysisParams
    results: Dict[str, Any] | None = None
    error: str | None = None
