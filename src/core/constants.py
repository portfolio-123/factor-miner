from typing import Final

from src.core.types import DatasetType, Frequency, ScalingMethod


PRICE_COLUMN: Final[str] = "Last Close"
REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID", PRICE_COLUMN]
DEFAULT_BENCHMARK: Final[str] = "SPY:USA"
DEFAULT_MIN_ALPHA: Final[float] = 0.5
DEFAULT_TOP_PCT: Final[float] = 20.0
DEFAULT_BOTTOM_PCT: Final[float] = 20.0
DEFAULT_CORRELATION_THRESHOLD: Final[float] = 0.5
DEFAULT_N_FACTORS: Final[int] = 10
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
