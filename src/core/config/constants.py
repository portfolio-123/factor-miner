from abc import ABC, abstractmethod
from typing import Literal, Protocol, TypedDict

import polars as pl

from src.core.types.models import Frequency, ScalingMethod

FUTURE_PERF_COLUMN = "__Future_Perf__"
REQUIRED_COLUMNS = {"Date", "Ticker", FUTURE_PERF_COLUMN}
SPECIAL_COLUMNS = REQUIRED_COLUMNS | {"P123 ID"}

INTERNAL_BENCHMARK_COL = "__benchmark__"

AUTH_COOKIE_KEY = "p123_access_token"

SCALING_LABELS = {ScalingMethod.NORMAL: "Z-Score", ScalingMethod.MINMAX: "Min/Max", ScalingMethod.RANK: "Rank"}

FREQUENCY_LABELS = {
    Frequency.WEEKLY: "Weekly",
    Frequency.BIWEEKLY: "2 weeks",
    Frequency.FOUR_WEEKS: "4 weeks",
    Frequency.EIGHT_WEEKS: "8 weeks",
    Frequency.THIRTEEN_WEEKS: "13 weeks",
    Frequency.TWENTY_SIX_WEEKS: "26 weeks",
    Frequency.FIFTY_TWO_WEEKS: "52 weeks",
}

PIT_METHOD_LABELS = {1: "Included Prelims", 2: "Excluded Prelims"}

CLASSIFICATION_COLORS = {
    "best": ("#a5d6a7", "Best Factor"),
    "correlation_conflict": ("#ef9a9a", "Correlation Conflict"),
    "high_na": ("#fff59d", "High NA %"),
    "below_rank_metric": ("#ffcc80", "Below Min. Rank Metric"),
    "n_limit": ("#b0bec5", "Max. Factors Reached"),
}


class RankConfigInputSettings(TypedDict):
    min_value: float
    max_value: float
    step: float


class RankConfig(ABC):
    metric_label: str
    input_settings: RankConfigInputSettings
    default: float

    def get_renames(self, low_q: float, high_q: float) -> dict[str, str]:
        if low_q == 0:
            return {"annualized_alpha_pct": "H Ann. Alpha %", "beta": "H Beta", "t_stat": "H T-Stat"}
        elif high_q == 0:
            return {"annualized_alpha_pct": "L Ann. Alpha %", "beta": "L Beta", "t_stat": "L T-Stat"}
        else:
            return {"annualized_alpha_pct": "H−L Alpha %", "beta": "H−L Beta", "t_stat": "H−L T-Stat"}

    @abstractmethod
    def format_filter(self, v: float) -> str | float:
        pass

    @abstractmethod
    def get_sorting(self, low_q: float, high_q: float) -> tuple[pl.Expr, bool]:
        pass


class AnnualizedAlphaPctRankConfig(RankConfig):
    metric_label = "Absolute Annual Alpha"
    input_settings = {"min_value": 0.0, "max_value": 100.0, "step": 0.1}
    default = 0.5

    def format_filter(self, v):
        return f"{v}%"

    def get_sorting(self, low_q, high_q):
        if low_q == 0:
            return pl.col("annualized_alpha_pct"), True
        elif high_q == 0:
            return pl.col("annualized_alpha_pct"), False
        else:
            return pl.col("annualized_alpha_pct").abs(), True


class IcRankConfig(RankConfig):
    metric_label = "IC"
    input_settings = {"min_value": 0.0, "max_value": 1.0, "step": 0.005}
    default = 0.01

    def format_filter(self, v):
        return v

    def get_sorting(self, low_q, high_q):
        return pl.col("ic").abs(), True


RANK_CONFIG: dict[Literal["annualized_alpha_pct", "ic"], RankConfig] = {
    "annualized_alpha_pct": AnnualizedAlphaPctRankConfig(),
    "ic": IcRankConfig(),
}
