import streamlit as st
import pandas as pd
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import datetime


from typing import Set, Optional


@dataclass
class AppState:
    """Central state management for the application."""
    page: str = "history"
    current_step: int = 1
    completed_steps: Set[int] = field(default_factory=set)

    # internal app config
    is_internal_app: bool = False
    factor_list_uid: Optional[str] = None

    # data state
    benchmark_data: Optional[pd.DataFrame] = None
    benchmark_ticker: Optional[str] = None
    api_id: Optional[str] = None
    api_key: Optional[str] = None

    dataset_path: Optional[str] = None
    formulas_data: Optional[pd.DataFrame] = None
    # original user-entered paths (for form restoration when going back to step 1)
    dataset_path_input: Optional[str] = None

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

    # background job tracking
    current_job_id: Optional[str] = None


def get_state() -> AppState:
    if 'app_state' not in st.session_state:
        st.session_state.app_state = AppState()
    return st.session_state.app_state


def update_state(**kwargs) -> None:
    state = get_state()

    for key, value in kwargs.items():
        if hasattr(state, key):
            setattr(state, key, value)
    
    # preserve fl_id if it exists in state
    if state.factor_list_uid:
        st.query_params["fl_id"] = state.factor_list_uid

    # determine if this call explicitly navigated to history
    explicit_history_nav = ("page" in kwargs) and (kwargs["page"] == "history")

    if state.page == "analysis":
        if state.current_job_id:
            st.query_params["job_id"] = state.current_job_id
        if state.current_step is not None:
            st.query_params["step"] = str(state.current_step)

    # clear analysis params when explicitly navigating to history.
    if explicit_history_nav:
        st.query_params.pop("job_id", None)
        st.query_params.pop("step", None)


def add_debug_log(message: str, without_timestamp: bool = False) -> None:
    state = get_state()
    timestamp = datetime.now().strftime('%H:%M:%S')
    message = message if without_timestamp else f"[{timestamp}] {message}"
    state.debug_logs.append(message)
    # keep only the last 100 logs
    if len(state.debug_logs) > 100:
        state.debug_logs = state.debug_logs[-100:]


def clear_debug_logs() -> None:
    state = get_state()
    state.debug_logs = []
    add_debug_log("Logs cleared")
