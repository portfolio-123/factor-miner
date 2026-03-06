import json
from typing import Any
from io import StringIO
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd


def extract_benchmark_ticker(benchmark: str) -> str:
    return benchmark[benchmark.rfind("(") + 1 : benchmark.rfind(")")]
def read_json_file(path: Path) -> dict | None:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, IOError):
        return None


def format_date(date_value: Any, format_str: str = "%Y/%m/%d") -> str:
    try:
        date_obj = pd.to_datetime(date_value)
        return date_obj.strftime(format_str)
    except Exception:
        return "N/A"


def format_timestamp(timestamp: str | int | None, format_str: str = "%b %d, %Y at %I:%M %p UTC") -> str:
    if not timestamp:
        return "N/A"
    try:
        ts = int(timestamp)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime(format_str)
    except (ValueError, TypeError):
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


def serialize_dataframe(df: pd.DataFrame) -> str:
    return df.to_json(orient="split", date_format="iso")


def deserialize_dataframe(*data: str) -> pd.DataFrame | tuple[pd.DataFrame, ...]:
    results = [pd.read_json(StringIO(d), orient="split") for d in data]
    return results[0] if len(results) == 1 else tuple(results)


def find_price_column(column_names: list[str], price_column_names: list[str]) -> str:
    for name in price_column_names:
        if name in column_names:
            return name
    raise ValueError(
        f"[price-column-not-found] Dataset must include one of: {', '.join(price_column_names)}"
    )

def add_formula_and_tag_columns(
    download_df: pd.DataFrame,
    formulas_df: pd.DataFrame,
    factor_col: str = "Factor",
) -> pd.DataFrame:
    deduped = formulas_df.drop_duplicates(subset=["name"]).set_index("name")
    formula_map = deduped["formula"]
    tag_map = deduped["tag"]
    result = download_df.copy()
    factor_idx = result.columns.get_loc(factor_col)
    result.insert(factor_idx + 1, "Formula", result[factor_col].map(formula_map))
    result.insert(factor_idx + 2, "Tag", result[factor_col].map(tag_map))
    return result
