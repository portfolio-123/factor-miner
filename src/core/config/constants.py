from collections.abc import Callable
from typing import TypedDict

from src.core.types.models import Frequency, ScalingMethod

PRICE_COLUMN = "Next Close"
SPECIAL_COLUMNS = {"Date", "Ticker", "P123 ID", PRICE_COLUMN}

INTERNAL_FUTURE_PERF_COL = "__future_perf__"
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


class RankConfigItem(TypedDict):
    metric_label: str
    format_filter: Callable[[float], str | float]
    input_settings: RankConfigInputSettings


RANK_CONFIG: dict[str, RankConfigItem] = {
    "annualized_alpha_pct": {
        "metric_label": "Absolute Annual Alpha",
        "format_filter": lambda v: f"{v}%",
        "input_settings": {"min_value": 0.0, "max_value": 100.0, "step": 0.1},
    },
    "ic": {"metric_label": "IC", "format_filter": lambda v: v, "input_settings": {"min_value": 0.0, "max_value": 1.0, "step": 0.005}},
}
