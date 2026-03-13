import base64
import json
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
import polars as pl


def extract_benchmark_ticker(benchmark: str) -> str:
    return benchmark[benchmark.rfind("(") + 1 : benchmark.rfind(")")]
def read_json_file(path: Path) -> dict | None:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, IOError):
        return None


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
def format_timestamp(timestamp: str | int | None, format_str: str = "%b %d, %Y at %I:%M %p UTC") -> str:
    if not timestamp:
        return "N/A"
    try:
        ts = int(timestamp) // 1000
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime(format_str)
    except (ValueError, TypeError, OSError):
        pass
    try:
        dt = datetime.fromisoformat(str(timestamp))
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
    df.write_ipc(buffer)
    return base64.b64encode(buffer.getvalue()).decode('utf-8')


def deserialize_dataframe(*data: str) -> pl.DataFrame | tuple[pl.DataFrame, ...]:
    results = [pl.read_ipc(BytesIO(base64.b64decode(d))) for d in data]
    return results[0] if len(results) == 1 else tuple(results)


def find_price_column(column_names: list[str], price_column_names: list[str]) -> str:
    for name in price_column_names:
        if name in column_names:
            return name
    raise ValueError(
        f"[price-column-not-found] Dataset must include one of: {', '.join(price_column_names)}"
    )

def add_formula_and_tag_columns(
    download_df: pl.DataFrame,
    formulas_df: pl.DataFrame,
    factor_col: str = "Factor",
) -> pl.DataFrame:
    # df with all the factor names
    mapping_df = formulas_df.unique(subset=["name"]).select(["name", "formula", "tag"])

    # join with the download df that contains the results
    result = download_df.join(
        mapping_df,
        left_on=factor_col,
        right_on="name",
        how="left"
    ).rename({"formula": "Formula", "tag": "Tag"})

    # reorder columns to place Formula and Tag after factor_col
    cols = result.columns
    # know what index the factor column is at
    factor_idx = cols.index(factor_col)
    new_order = (
        # place formula+tag after factor column
        cols[:factor_idx + 1] +
        ["Formula", "Tag"] +
        [c for c in cols[factor_idx + 1:] if c not in ["Formula", "Tag", "name"]]
    )
    return result.select(new_order)
