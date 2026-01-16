from typing import Any
from io import StringIO
from datetime import datetime
import pandas as pd


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


def deserialize_dataframe(json_str: str) -> pd.DataFrame:
    return pd.read_json(StringIO(json_str), orient="split")


def format_dataset_option(ver: str, active_version: str | None) -> str:
    if ver == active_version:
        return "ACTIVE DATASET"
    return f"Version {ver}"


def add_formula_column(
    df: pd.DataFrame,
    formulas_df: pd.DataFrame | None,
    factor_col: str = "Factor",
) -> pd.DataFrame:
    if formulas_df is None or "name" not in formulas_df.columns:
        return df

    formula_map = formulas_df.drop_duplicates(subset=["name"]).set_index("name")[
        "formula"
    ]
    result = df.copy()
    factor_idx = result.columns.get_loc(factor_col)
    result.insert(factor_idx + 1, "Formula", result[factor_col].map(formula_map))
    return result
