from typing import Final

from src.core.types.models import DatasetType, Frequency, ScalingMethod


PRICE_COLUMN_NAMES: Final[list[str]] = ["Next Close", "Future Return"]
BASE_REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID"]

INTERNAL_FUTURE_PERF_COL: Final[str] = "__future_perf__"
INTERNAL_BENCHMARK_COL: Final[str] = "__benchmark__"

DEFAULT_MIN_ALPHA: Final[float] = 0.5
DEFAULT_TOP_PCT: Final[float] = 10.0
DEFAULT_BOTTOM_PCT: Final[float] = 10.0
DEFAULT_CORRELATION_THRESHOLD: Final[float] = 0.5
DEFAULT_N_FACTORS: Final[int] = 10
DEFAULT_MAX_NA_PCT: Final[float] = 40.0
DEFAULT_MIN_IC: Final[float] = 0.015
AUTH_COOKIE_KEY: Final[str] = "p123_access_token"
SETTINGS_STORAGE_KEY: Final[str] = "last_analysis_settings"

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

# (hex_color, badge_color, label)
CLASSIFICATION_COLORS: Final[dict[str, tuple[str, str, str]]] = {
    "best": ("#21c354", "green", "Best Factor"),
    "correlation_conflict": ("#ff4b4b", "red", "Correlation Conflict"),
    "high_na": ("#ffe312", "yellow", "High NA %"),
    "below_alpha": ("#ffa421", "orange", "Below Min Alpha"),
    "below_ic": ("#803df5", "violet", "Below Min IC"),
    "n_limit": ("#808495", "gray", "Max. Factors Reached"),
}
