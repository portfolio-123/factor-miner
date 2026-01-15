from typing import Final, TypedDict
from enum import StrEnum


PRICE_COLUMN: Final[str] = "Last Close"
REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID", PRICE_COLUMN]
DEFAULT_BENCHMARK: Final[str] = "SPY:USA"
AUTH_COOKIE_KEY: Final[str] = "p123_access_token"


class JobStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"

class JobProgress(TypedDict):
    completed: int
    total: int
    current_factor: str


