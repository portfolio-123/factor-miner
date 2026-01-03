import streamlit as st
import pandas as pd
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import datetime

from src.core.constants import DEFAULT_BENCHMARK


@dataclass
class AppState:
    """Central state management for the application."""

    page: str = "history"
    current_step: int = 1
    config_completed: bool = False

    # internal app config
    factor_list_uid: Optional[str] = None

    is_editing_dataset: bool = False

    # data state
    benchmark_data: Optional[pd.DataFrame] = None
    benchmark_ticker: Optional[str] = DEFAULT_BENCHMARK

    dataset_path: Optional[str] = None
    formulas_data: Optional[pd.DataFrame] = None

    # calculation parameters
    min_alpha: float = 0.5
    top_x_pct: float = 20.0
    bottom_x_pct: float = 20.0
    correlation_threshold: float = 0.5
    n_features: int = 10

    # results storage for results page filtering
    all_metrics: Optional[pd.DataFrame] = None
    all_corr_matrix: Optional[pd.DataFrame] = None

    # debug logs
    debug_logs: List[str] = field(default_factory=list)

    # background job tracking
    current_job_id: Optional[str] = None

    # error states
    config_error: Optional[str] = None
    analysis_error: Optional[str] = None

    # UI modal states
    show_debug_modal: bool = False
    formulas_ds_ver: Optional[str] = None
    edit_dataset_mode: bool = False

    # auth states
    user_payload: Optional[dict] = None
    auth_check_complete: bool = False

    # filter states (results page)
    filter_correlation: float = 0.5
    filter_n_features: int = 10


def get_state() -> AppState:
    if "app_state" not in st.session_state:
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

    if state.page == "new_analysis":
        if state.current_job_id:
            st.query_params["job_id"] = state.current_job_id
            st.query_params.pop("new_analysis", None)
            st.query_params.pop("step", None)
        else:
            st.query_params["new_analysis"] = "true"
            st.query_params["step"] = str(state.current_step)
            st.query_params.pop("job_id", None)

    if state.page == "results":
        st.query_params["job_id"] = state.current_job_id
        st.query_params.pop("new_analysis", None)
        st.query_params.pop("step", None)

    # clear analysis params when explicitly navigating to history.
    if explicit_history_nav:
        st.query_params.pop("job_id", None)
        st.query_params.pop("step", None)
        st.query_params.pop("new_analysis", None)


def reset_analysis_state() -> None:
    update_state(
        page="new_analysis",
        current_step=1,
        current_job_id=None,
        config_completed=False,
        # clear previous results
        all_metrics=None,
        all_corr_matrix=None,
        # clear errors
        config_error=None,
        analysis_error=None,
    )


def add_debug_log(message: str, without_timestamp: bool = False) -> None:
    state = get_state()
    timestamp = datetime.now().strftime("%H:%M:%S")
    message = message if without_timestamp else f"[{timestamp}] {message}"
    state.debug_logs.append(message)
    # keep only the last 100 logs
    if len(state.debug_logs) > 100:
        state.debug_logs = state.debug_logs[-100:]


def clear_debug_logs() -> None:
    state = get_state()
    state.debug_logs = []
    add_debug_log("Logs cleared")
