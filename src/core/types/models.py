from enum import IntEnum, StrEnum

import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator


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


class Frequency(IntEnum):
    WEEKLY = 1
    BIWEEKLY = 7
    FOUR_WEEKS = 2
    EIGHT_WEEKS = 8
    THIRTEEN_WEEKS = 3
    TWENTY_SIX_WEEKS = 9
    FIFTY_TWO_WEEKS = 10

    @property
    def weeks(self) -> int:
        mapping = {
            Frequency.WEEKLY: 1,
            Frequency.BIWEEKLY: 2,
            Frequency.FOUR_WEEKS: 4,
            Frequency.EIGHT_WEEKS: 8,
            Frequency.THIRTEEN_WEEKS: 13,
            Frequency.TWENTY_SIX_WEEKS: 26,
            Frequency.FIFTY_TWO_WEEKS: 52,
        }
        return mapping[self]

    @property
    def trading_days(self) -> int:
        return self.weeks * 5

    @property
    def periods_per_year(self) -> float:
        return 365.0 / (self.weeks * 7)


class AnalysisParams(BaseModel):
    min_alpha: float
    top_pct: float
    bottom_pct: float
    correlation_threshold: float
    n_factors: int
    max_na_pct: float
    min_ic: float
    rank_by: str = "Alpha"


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

    version: str | None = None
    factorListName: str | None = None
    universeName: str
    frequency: Frequency
    currency: str
    type: DatasetType = DatasetType.PERIOD
    startDt: str | None = None
    endDt: str | None = None
    asOfDt: str | None = None
    benchmark: str = Field(alias="benchName")
    precision: int
    normalization: NormalizationConfig | None = None
    formulas: list | None = None
    pitMethod: int
    active: bool = False
    num_rows: int | None = Field(default=None, alias="numRows")

    @field_validator("normalization", mode="before")
    @classmethod
    def coerce_normalization(cls, v):
        if v is False:
            return None
        return v

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
