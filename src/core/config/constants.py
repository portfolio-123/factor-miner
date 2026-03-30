from typing import Callable, Final

from src.core.types.models import Frequency, ScalingMethod


PRICE_COLUMN: Final[str] = "Next Close"
BASE_REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID"]

INTERNAL_FUTURE_PERF_COL: Final[str] = "__future_perf__"
INTERNAL_BENCHMARK_COL: Final[str] = "__benchmark__"

DEFAULT_MIN_ANNUALIZED_ALPHA_PCT: Final[float] = 0.5
DEFAULT_TOP_PCT: Final[float] = 10
DEFAULT_BOTTOM_PCT: Final[float] = 10
DEFAULT_CORRELATION_THRESHOLD: Final[float] = 0.5
DEFAULT_N_FACTORS: Final[int] = 10
DEFAULT_MAX_NA_PCT: Final[float] = 40
DEFAULT_MIN_IC: Final[float] = 0.015
DEFAULT_MAX_RETURN_PCT: Final[float] = 200
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

CLASSIFICATION_COLORS: Final[dict[str, tuple[str, str]]] = {
    "best": ("#a5d6a7", "Best Factor"),
    "correlation_conflict": ("#ef9a9a", "Correlation Conflict"),
    "high_na": ("#fff59d", "High NA %"),
    "below_annualized_alpha_pct": ("#ffcc80", "Below Min Annualized Alpha"),
    "below_ic": ("#ce93d8", "Below Min IC"),
    "n_limit": ("#b0bec5", "Max. Factors Reached"),
}

RANK_CONFIG: dict[
    str, dict[str, str | Callable[[float], str | float] | dict[str, float]]
] = {
    "annualized_alpha_pct": {
        "metric_label": "Absolute Annual Alpha",
        "format_filter": lambda v: f"{v}%",
        "input_settings": {"min_value": 0.0, "max_value": 100.0, "step": 0.1},
    },
    "ic": {
        "metric_label": "IC",
        "format_filter": lambda v: v,
        "input_settings": {"min_value": 0.0, "max_value": 1.0, "step": 0.01},
    },
}
