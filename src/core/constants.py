from typing import Final


PRICE_COLUMN: Final[str] = "Last Close"
REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID", PRICE_COLUMN]
DEFAULT_BENCHMARK: Final[str] = "SPY:USA"
DEFAULT_MIN_ALPHA: Final[float] = 0.5
DEFAULT_TOP_PCT: Final[float] = 20
DEFAULT_BOTTOM_PCT: Final[float] = 20
AUTH_COOKIE_KEY: Final[str] = "p123_access_token"
