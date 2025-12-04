from typing import Final
from enum import StrEnum


PRICE_COLUMN: Final[str] = "Last Close"
REQUIRED_COLUMNS: Final[list[str]] = ["Date", "Ticker", "P123 ID", PRICE_COLUMN]
DEFAULT_BENCHMARK: Final[str] = "SPY:USA"


class FileType(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


