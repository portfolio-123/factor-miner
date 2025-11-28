from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple
import os
import streamlit as st
from streamlit_local_storage import LocalStorage


def get_url_params(*keys: str) -> tuple:
    """Get multiple URL parameters at once.

    Args:
        *keys: Parameter names to retrieve

    Returns:
        Tuple of parameter values (None if not found)

    Example:
        fl_id, benchmark = get_url_params('fl_id', 'benchmark')
    """
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


def detect_file_type(file_path: Path) -> str:
    """Detect file type by reading magic bytes."""
    try:
        with open(file_path, 'rb') as f:
            magic = f.read(4)
            if magic == b'PAR1':
                return 'parquet'
    except Exception:
        pass
    return 'csv'


def locate_factor_list_files(fl_id: str) -> Tuple[Optional[Path], Optional[Path], Optional[str], Optional[str]]:
    """Locate dataset and formulas files for internal app mode.

    Args:
        fl_id: Factor list ID from URL parameter

    Returns:
        Tuple of (dataset_path, formulas_path, error_message, file_type)
    """
    base_dir = os.getenv('FACTOR_LIST_DIR')
    if not base_dir:
        return None, None, "FACTOR_LIST_DIR environment variable not set", None

    base_path = Path(base_dir)
    if not base_path.exists():
        return None, None, f"FACTOR_LIST_DIR does not exist: {base_dir}", None

    # Dataset file:
    dataset_path = base_path / fl_id
    if not dataset_path.exists():
        return None, None, f"Dataset file not found: {dataset_path}", None

    file_type = detect_file_type(dataset_path)

    if file_type == 'parquet':
        formulas_path = None
    else:
        formulas_path = base_path / f"{fl_id}_meta"
        if not formulas_path.exists():
            return None, None, f"Formulas file not found: {formulas_path}", None

    return dataset_path, formulas_path, None, file_type


def get_local_storage():
    """Get or create LocalStorage instance (lazy initialization)."""
    if 'local_storage' not in st.session_state:
        st.session_state.local_storage = LocalStorage()
    return st.session_state.local_storage
