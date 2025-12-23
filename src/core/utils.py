from pathlib import Path
from typing import Any
from io import StringIO
from datetime import datetime
import os
import streamlit as st
from streamlit_local_storage import LocalStorage
from extra_streamlit_components import CookieManager
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
        return 'N/A'

def locate_factor_list_file(fl_id: str) -> str:
    base_dir = os.getenv('FACTOR_LIST_DIR')
    if not base_dir:
        raise ValueError("FACTOR_LIST_DIR environment variable not set")

    base_path = Path(base_dir)
    if not base_path.exists():
        raise ValueError(f"FACTOR_LIST_DIR does not exist: {base_dir}")

    path = base_path / fl_id
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}")

    return str(path)


def get_local_storage():
    if 'local_storage' not in st.session_state:
        st.session_state.local_storage = LocalStorage()
    return st.session_state.local_storage


def cookies():
    if 'cookie_manager' not in st.session_state:
        st.session_state.cookie_manager = CookieManager()
    return st.session_state.cookie_manager


def serialize_dataframe(df: pd.DataFrame) -> str:
    return df.to_json(orient='split', date_format='iso')


def deserialize_dataframe(json_str: str) -> pd.DataFrame:    return pd.read_json(StringIO(json_str), orient='split')
