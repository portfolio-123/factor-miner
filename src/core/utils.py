import json
from typing import Any
from io import StringIO
from pathlib import Path
from datetime import datetime
import pandas as pd
from pydantic import ValidationError

from src.core.types import Analysis


def read_json_file(path: Path) -> dict | None:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, IOError):
        return None


def read_analysis_json(path: Path) -> Analysis | None:
    """Read JSON file and validate as Analysis, returning None on any error."""
    data = read_json_file(path)
    if data is None:
        return None
    try:
        return Analysis.model_validate(data)
    except ValidationError:
        return None


def format_timestamp(ts_str: str) -> str:
    try:
        ts = float(ts_str)
        return datetime.fromtimestamp(ts).strftime("%b %d, %Y at %I:%M %p")
    except (ValueError, TypeError):
        return f"Version: {ts_str}"


def format_date(date_value: Any, format_str: str = "%m/%d/%Y") -> str:
    try:
        import pandas as pd

        date_obj = pd.to_datetime(date_value)
        return date_obj.strftime(format_str)
    except Exception:
        return "N/A"



def serialize_dataframe(df: pd.DataFrame) -> str:
    return df.to_json(orient="split", date_format="iso")


def deserialize_dataframe(*data: str) -> pd.DataFrame | tuple[pd.DataFrame, ...]:
    results = [pd.read_json(StringIO(d), orient="split") for d in data]
    return results[0] if len(results) == 1 else tuple(results)


def format_dataset_option(ver: str) -> str:
    if ver == "active":
        return "Active Version"
    return f"Version {ver}"


def add_formula_column(
    download_df: pd.DataFrame,
    formulas_df: pd.DataFrame,
    factor_col: str = "Factor",
) -> pd.DataFrame:
    formula_map = formulas_df.drop_duplicates(subset=["name"]).set_index("name")[
        "formula"
    ]
    result = download_df.copy()
    factor_idx = result.columns.get_loc(factor_col)
    result.insert(factor_idx + 1, "Formula", result[factor_col].map(formula_map))
    return result
