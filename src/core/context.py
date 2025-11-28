import streamlit as st
import pandas as pd
from pathlib import Path
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class AppState:
    """Central state management for the application."""
    current_step: int = 1
    completed_steps: set = field(default_factory=set)

    PRICE_COLUMN: str = "Last Close"

    # internal app config
    is_internal_app: bool = False
    factor_list_uid: Optional[str] = None

    # auto-located file paths (internal app mode)
    auto_dataset_path: Optional[Path] = None
    auto_formulas_path: Optional[Path] = None
    auto_dataset_file_type: Optional[str] = None  # 'csv' or 'parquet', detected via magic bytes
    files_verified: bool = False
    files_verification_error: Optional[str] = None

    # data state
    benchmark_data: Optional[pd.DataFrame] = None
    raw_data: Optional[pd.DataFrame] = None
    price_column: Optional[str] = None
    benchmark_ticker: Optional[str] = None
    api_id: Optional[str] = None
    api_key: Optional[str] = None

    file_type: Optional[str] = None  # 'csv' or 'parquet'
    dataset_path: Optional[Path] = None
    formulas_path: Optional[Path] = None
    formulas_data: Optional[pd.DataFrame] = None
    # original user-entered paths (for form restoration when going back to step 1)
    dataset_path_input: Optional[str] = None
    formulas_path_input: Optional[str] = None

    # calculation parameters
    min_alpha: float = 0.5
    top_x_pct: float = 20.0
    bottom_x_pct: float = 20.0
    correlation_threshold: float = 0.5
    n_features: int = 10

    # results storage for step 3 filtering
    all_metrics: Optional[pd.DataFrame] = None
    all_corr_matrix: Optional[pd.DataFrame] = None

    # debug logs
    debug_logs: List[str] = field(default_factory=list)


def get_state() -> AppState:
    if 'app_state' not in st.session_state:
        st.session_state.app_state = AppState()
    return st.session_state.app_state


def update_state(**kwargs) -> None:
    state = get_state()
    for key, value in kwargs.items():
        if hasattr(state, key):
            setattr(state, key, value)


def add_debug_log(message: str) -> None:
    state = get_state()
    timestamp = datetime.now().strftime('%H:%M:%S')
    state.debug_logs.append(f"[{timestamp}] {message}")
    # keep only the last 100 logs
    if len(state.debug_logs) > 100:
        state.debug_logs = state.debug_logs[-100:]


def clear_debug_logs() -> None:
    state = get_state()
    state.debug_logs = []
    add_debug_log("Logs cleared")
