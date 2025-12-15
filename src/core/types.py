from enum import StrEnum
from typing import Any, Dict
from pydantic import BaseModel, ConfigDict, Field


class ScalingMethod(StrEnum):
    NORMAL = "normal"
    MINMAX = "minmax"
    RANK = "rank"


class ScopeType(StrEnum):
    DATASET = "dataset"
    DATE = "date"


class Frequency(StrEnum):
    WEEKLY = "WEEKLY"
    WEEKS2 = "WEEKS2"
    WEEKS4 = "WEEKS4"
    WEEKS8 = "WEEKS8"
    WEEKS13 = "WEEKS13"
    WEEKS26 = "WEEKS26"
    WEEKS52 = "WEEKS52"


class AnalysisParams(BaseModel):
    dataset_path: str
    top_pct: float
    bottom_pct: float
    min_alpha: float
    benchmark_data: str
    benchmark_ticker: str


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

    universeName: str = Field(default="Unknown Universe")
    frequency: Frequency | str = Field(default="Unknown")
    currency: str = Field(default="USD")
    startDt: str | None = Field(default=None)
    endDt: str | None = Field(default=None)
    benchName: str | None = Field(default=None)
    precision: str | None = Field(default=None)
    normalization: NormalizationConfig | None = Field(default=None)


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
