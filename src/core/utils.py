from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Tuple
from src.core.context import state
import os
from urllib.parse import parse_qs

def log_debug(message: str) -> None:

    if state.debug_output is not None:
        with state.debug_output:
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] {message}")


def get_url_param(key: str, default: Optional[str] = None) -> Optional[str]:
    query_string = os.getenv('QUERY_STRING', '')
    if not query_string:
        return default

    params = parse_qs(query_string)
    values = params.get(key, [default])
    return values[0] if values else default


def get_url_params(*keys: str) -> tuple:
    """Get multiple URL parameters at once.

    Args:
        *keys: Parameter names to retrieve

    Returns:
        Tuple of parameter values (None if not found)

    Example:
        fl_id, benchmark = get_url_params('fl_id', 'benchmark')
    """
    query_string = os.getenv('QUERY_STRING', '')
    if not query_string:
        return (None,) * len(keys)

    params = parse_qs(query_string)
    return tuple(params.get(key, [None])[0] for key in keys)


def format_date(date_value: Any, format_str: str = "%m/%d/%Y") -> str:
    try:
        import pandas as pd
        date_obj = pd.to_datetime(date_value)
        return date_obj.strftime(format_str)
    except Exception:
        return 'N/A'


def format_percentage(value: float, decimals: int = 2) -> str:
    import pandas as pd
    if pd.isna(value):
        return 'N/A'
    return f"{value * 100:.{decimals}f}%"


def format_number(value: float, decimals: int = 4) -> str:
    import pandas as pd
    if pd.isna(value):
        return 'N/A'
    return f"{value:.{decimals}f}"


def detect_file_type(file_path: Path) -> str:
    try:
        with open(file_path, 'rb') as f:
            magic = f.read(4)
            if magic == b'PAR1':
                return 'parquet'
    except Exception:
        pass
    return 'csv'


def locate_factor_list_files(fl_id: str) -> Tuple[Optional[Path], Optional[Path], Optional[str], Optional[str]]:
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

    # Formulas file:
    formulas_path = base_path / f"{fl_id}_meta.csv"
    if not formulas_path.exists():
        return None, None, f"Formulas file not found: {formulas_path}", None

    file_type = detect_file_type(dataset_path)

    return dataset_path, formulas_path, None, file_type
