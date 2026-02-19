from typing import Final

from src.core.types.models import DatasetType, Frequency, ScalingMethod


PRICE_FORMULA: Final[str] = "Close(-1)"
PRICE_FORMULA_FRIDAY: Final[str] = "Close(0)"
BASE_REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID"]

INTERNAL_FUTURE_PERF_COL: Final[str] = "__future_perf__"
INTERNAL_BENCHMARK_COL: Final[str] = "__benchmark__"

DEFAULT_BENCHMARK: Final[str] = "SPY:USA"
DEFAULT_MIN_ALPHA: Final[float] = 0.5
DEFAULT_TOP_PCT: Final[float] = 20.0
DEFAULT_BOTTOM_PCT: Final[float] = 20.0
DEFAULT_CORRELATION_THRESHOLD: Final[float] = 0.5
DEFAULT_N_FACTORS: Final[int] = 10
DEFAULT_MAX_NA_PCT: Final[float] = 40.0
DEFAULT_MIN_IC: Final[float] = 0.015
AUTH_COOKIE_KEY: Final[str] = "p123_access_token"

SCALING_LABELS: Final[dict[ScalingMethod, str]] = {
    ScalingMethod.NORMAL: "Z-Score",
    ScalingMethod.MINMAX: "Min/Max",
    ScalingMethod.RANK: "Rank",
}

FREQUENCY_LABELS: Final[dict[Frequency, str]] = {
    Frequency.WEEKLY: "Weekly",
    Frequency.BIWEEKLY: "2 weeks",
    Frequency.FOUR_WEEKS: "4 weeks",
    Frequency.EIGHT_WEEKS: "8 weeks",
    Frequency.THIRTEEN_WEEKS: "13 weeks",
    Frequency.TWENTY_SIX_WEEKS: "26 weeks",
    Frequency.FIFTY_TWO_WEEKS: "52 weeks",
}

PIT_METHOD_LABELS: Final[dict[int, str]] = {
    1: "Included Prelims",
    2: "Excluded Prelims",
}

DATASET_TYPE_LABELS: Final[dict[DatasetType, str]] = {
    DatasetType.PERIOD: "Period",
    DatasetType.DATE: "Date",
}

CLASSIFICATION_COLORS: Final[dict[str, tuple[str, str]]] = {
    "best": ("#a5d6a7", "Best Factor"),
    "correlation_conflict": ("#ef9a9a", "Correlation Conflict"),
    "high_na": ("#fff59d", "High NA %"),
    "below_alpha": ("#ffcc80", "Below Min Alpha"),
    "below_ic": ("#ce93d8", "Below Min IC"),
    "n_limit": ("#b0bec5", "N Limit Reached"),
}
