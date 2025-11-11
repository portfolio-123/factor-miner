from datetime import datetime
from typing import Any
from src.core.context import state

def log_debug(message: str) -> None:

    if state.debug_output is not None:
        with state.debug_output:
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] {message}")


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
