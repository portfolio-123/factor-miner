from dataclasses import dataclass
from enum import IntEnum, StrEnum
from pathlib import Path
from typing import TypedDict

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from src.core.config.environment import INTERNAL_MODE
from src.core.config.paths import get_user_base_dir


class FactorMetricResult(TypedDict):
    beta: float
    t_stat: float
    annualized_alpha_pct: float


class AnalysisRunResult(TypedDict):
    all_metrics: str
    all_corr_matrix: str
    avg_abs_alpha: float
    best_feature_names: list[str]
    factor_classifications: dict[str, str]


@dataclass(repr=False, eq=False, slots=True)
class DatasetDetails:
    fl_id: str
    user_uid: str | None = None

    @property
    def _base_dir(self) -> Path:
        return get_user_base_dir(self.user_uid)

    def get_base_path(self) -> Path:
        filename = self.fl_id if INTERNAL_MODE else f"{self.fl_id}.parquet"
        return self._base_dir / filename

    def get_backup_dir(self) -> Path:
        return self._base_dir / "FactorMiner" / self.fl_id


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


class AnalysisProgress(BaseModel):
    completed: int
    total: int


class AnalysisResults(BaseModel):
    all_metrics: str
    all_corr_matrix: str
    best_feature_names: list[str] = []
    factor_classifications: dict[str, str] = {}


class TokenPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    apiId: int
    apiKey: str
    user_uid: str | None = Field(default=None, alias="sub")


class ScalingMethod(StrEnum):
    NORMAL = "normal"
    MINMAX = "minmax"
    RANK = "rank"


class ScopeType(StrEnum):
    DATASET = "dataset"
    DATE = "date"


class DatasetType(StrEnum):
    PERIOD = "period"
    DATE = "date"


P123_TO_WEEKS = {1: 1, 7: 2, 2: 4, 8: 8, 3: 13, 9: 26, 10: 52}
WEEKS_TO_P123 = {weeks: code for code, weeks in P123_TO_WEEKS.items()}


class Frequency(IntEnum):
    WEEKLY = 1
    BIWEEKLY = 2
    FOUR_WEEKS = 4
    EIGHT_WEEKS = 8
    THIRTEEN_WEEKS = 13
    TWENTY_SIX_WEEKS = 26
    FIFTY_TWO_WEEKS = 52

    @property
    def trading_days(self) -> int:
        return self.value * 5

    @property
    def calendar_days(self) -> int:
        return self.value * 7

    @property
    def periods_per_year(self) -> float:
        return 365 / self.calendar_days

    @property
    def p123_code(self) -> int:
        return WEEKS_TO_P123[self.value]


class AnalysisParams(BaseModel):
    min_alpha: float
    top_pct: float
    bottom_pct: float
    correlation_threshold: float
    n_factors: int
    max_na_pct: float
    min_ic: float
    rank_by: str = "Alpha"
    max_return_pct: float = 200


class NormalizationConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    scaling: str
    scope: str
    trimPct: float | None = None
    outliers: str | None = None
    outlierLimit: float | None = None
    mlTrainingEnd: str | None = None
    naFill: str


class DatasetConfig(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    version: str = ""
    factorListName: str | None = None
    universeName: str
    frequency: Frequency
    currency: str
    type: DatasetType = DatasetType.PERIOD
    startDt: str | None = None
    endDt: str | None = None
    asOfDt: str | None = None
    benchName: str
    precision: int
    normalization: bool = False
    preprocessor: NormalizationConfig | None = None
    formulas: list | None = None
    pitMethod: int
    active: bool = False
    numRows: int | None = None

    @field_validator("version", mode="before")
    @classmethod
    def coerce_version_to_str(cls, v):
        return str(v)

    @field_validator("frequency", mode="before")
    @classmethod
    def parse_p123_frequency(cls, v: int):
        return P123_TO_WEEKS.get(v, v)

    @field_serializer("frequency")
    def serialize_p123_frequency(self, v: Frequency):
        return v.p123_code

    # for when tag is missing completely
    @property
    def formulas_df(self) -> pl.DataFrame:
        df = pl.DataFrame(self.formulas)
        if "tag" not in df.columns:
            df = df.with_columns(pl.lit("").alias("tag"))
        return df


class AnalysisSummary(BaseModel):
    id: str
    fl_id: str
    status: AnalysisStatus
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    dataset_version: str
    params: AnalysisParams
    notes: str | None = None
    avg_abs_alpha: float | None = None
    best_factors_count: int | None = None


class Analysis(AnalysisSummary):
    updated_at: str | None = None
    results: AnalysisResults | None = None
    error: str | None = None
    progress: AnalysisProgress | None = None


class AnalysisUpdate(TypedDict, total=False):
    status: AnalysisStatus
    started_at: str
    finished_at: str
    notes: str
    avg_abs_alpha: float
    best_factors_count: int
    updated_at: str
    results: AnalysisResults
    error: str
    progress: AnalysisProgress | None
