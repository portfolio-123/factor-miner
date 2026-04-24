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
