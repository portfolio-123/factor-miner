import base64
from collections.abc import Callable
from io import BytesIO
from os import DirEntry, scandir
from pathlib import Path
from datetime import datetime, timezone
import polars as pl


def extract_benchmark_ticker(benchmark: str) -> str:
    return benchmark[benchmark.rfind("(") + 1 : benchmark.rfind(")")]


# "2024-01-15T14:30:00" -> "2024-01-15"
def format_date(date_value: str | None, format_str: str = "%Y-%m-%d") -> str:
    if not date_value:
        return "N/A"
    try:
        if format_str == "%Y-%m-%d":
            return date_value[:10]
        return datetime.fromisoformat(date_value[:10]).strftime(format_str)
    except Exception:
        return "N/A"


# "20240115_143052" (YYYYMMDD_HHMMSS)
def format_version_timestamp(unix_ts: float) -> str:
    return datetime.fromtimestamp(unix_ts).strftime("%Y%m%d_%H%M%S")


# "Jan 15, 2024 at 02:30 PM UTC"
def format_timestamp(
    timestamp: str | None, format_str: str = "%b %d, %Y at %I:%M %p UTC"
) -> str:
    if not timestamp:
        return "N/A"
    try:
        dt = datetime.fromisoformat(timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime(format_str)
    except (ValueError, TypeError):
        return "N/A"


# (start_iso, end_iso) -> "5m 30s" or "45s", etc
def format_runtime(started_at: str | None, finished_at: str | None) -> str:
    if not started_at or not finished_at:
        return "N/A"
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at)
        delta = end - start
        total_seconds = int(delta.total_seconds())

        if total_seconds < 60:
            return f"{total_seconds}s"

        minutes, seconds = divmod(total_seconds, 60)
        if minutes < 60:
            return f"{minutes}m {seconds}s"

        hours, minutes = divmod(minutes, 60)
        return f"{hours}h {minutes}m"
    except (ValueError, TypeError):
        return "N/A"


def serialize_dataframe(df: pl.DataFrame) -> str:
    buffer = BytesIO()
    df.write_ipc(buffer, compression="zstd")
    return base64.b64encode(buffer.getvalue()).decode("utf-8")


def deserialize_dataframe(data: str) -> pl.DataFrame:
    return pl.read_ipc(BytesIO(base64.b64decode(data)))


def add_formula_column(
    download_df: pl.DataFrame, formulas_df: pl.DataFrame, factor_col: str = "Factor"
) -> pl.DataFrame:
    if "Formula" in download_df.columns:
        return download_df

    mapping_df = formulas_df.unique(subset=["name"]).select(["name", "formula"])

    result = download_df.join(
        mapping_df, left_on=factor_col, right_on="name", how="left"
    ).rename({"formula": "Formula"})

    # place Formula after factor_col
    cols = result.columns
    factor_idx = cols.index(factor_col)
    new_order = (
        cols[: factor_idx + 1]
        + ["Formula"]
        + [c for c in cols[factor_idx + 1 :] if c not in ["Formula", "name"]]
    )
    return result.select(new_order)


def find_files(
    dirpath: Path,
    *,
    prefix: str | None = None,
    suffix: str | None = None,
    matcher: Callable[[DirEntry[str]], bool] | None = None,
):
    if matcher is None:
        if prefix:
            if suffix:
                matcher = lambda e: e.name.startswith(prefix) and e.name.endswith(
                    suffix
                )
            else:
                matcher = lambda e: e.name.startswith(prefix)
        else:
            if suffix:
                matcher = lambda e: e.name.endswith(suffix)
            else:
                raise ValueError("find_files called with invalid arguments")
    try:
        with scandir(dirpath) as it:
            for e in it:
                if e.is_file() and matcher(e):
                    yield e
    except (FileNotFoundError, NotADirectoryError):
        return
