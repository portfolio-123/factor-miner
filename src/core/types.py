from dataclasses import dataclass
from enum import StrEnum
from typing import TypedDict

from pydantic import BaseModel, ConfigDict, Field


class AnalysisStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"

    @property
    def color(self) -> str:
        colors = {
            AnalysisStatus.PENDING: "blue",
            AnalysisStatus.RUNNING: "blue",
            AnalysisStatus.SUCCESS: "green",
            AnalysisStatus.FAILED: "red",
        }
        return colors[self]

    @property
    def display(self) -> str:
        return self.value.capitalize()


class AnalysisProgress(TypedDict):
    completed: int
    total: int
    current_factor: str


class AnalysisResults(TypedDict):
    all_metrics: str
    all_corr_matrix: str


class AnalysisUpdates(TypedDict, total=False):
    status: AnalysisStatus
    results: AnalysisResults
    error: str
    progress: AnalysisProgress


@dataclass
class FilterParams:
    n_features: int
    correlation_threshold: float
    min_alpha: float


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


class SettingsForm(BaseModel):
    benchmark_ticker: str
    min_alpha: float
    top_pct: float
    bottom_pct: float


class AnalysisParams(SettingsForm):
    active_dataset_file: str
    access_token: str | None = None


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

    version: str | None = None
    universeName: str
    frequency: int
    currency: str
    startDt: str
    endDt: str
    benchmark: str = Field(alias="benchName")
    precision: int
    normalization: NormalizationConfig | None = None
    factorCount: int | None = None
    formulas: list | None = None
    pitMethod: int


class AnalysisSummary(BaseModel):
    id: str
    fl_id: str
    name: str | None = None
    status: AnalysisStatus
    created_at: str
    dataset_version: str
    params: AnalysisParams


class Analysis(AnalysisSummary):
    updated_at: str | None = None
    results: AnalysisResults | None = None
    error: str | None = None
    progress: AnalysisProgress | None = None
    logs: list[str] | None = None
