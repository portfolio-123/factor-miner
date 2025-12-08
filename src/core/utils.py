from datetime import datetime
from pathlib import Path
from typing import Any
from io import StringIO
import os
import streamlit as st
from streamlit_local_storage import LocalStorage
import pandas as pd


def get_url_params(*keys: str) -> tuple:
    try:
        params = st.query_params
        return tuple(params.get(key, None) for key in keys)
    except Exception:
        return (None,) * len(keys)


def format_date(date_value: Any, format_str: str = "%m/%d/%Y") -> str:
    try:
        import pandas as pd
        date_obj = pd.to_datetime(date_value)
        return date_obj.strftime(format_str)
    except Exception:
        return 'N/A'

def locate_factor_list_file(fl_id: str) -> Path:
    base_dir = os.getenv('FACTOR_LIST_DIR')
    if not base_dir:
        raise ValueError("FACTOR_LIST_DIR environment variable not set")

    base_path = Path(base_dir)
    if not base_path.exists():
        raise ValueError(f"FACTOR_LIST_DIR does not exist: {base_dir}")

    path = base_path / fl_id
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}")

    return path


def get_local_storage():
    if 'local_storage' not in st.session_state:
        st.session_state.local_storage = LocalStorage()
    return st.session_state.local_storage


def serialize_dataframe(df: pd.DataFrame) -> str:
    return df.to_json(orient='split', date_format='iso')


def deserialize_dataframe(json_str: str) -> pd.DataFrame:    return pd.read_json(StringIO(json_str), orient='split')
