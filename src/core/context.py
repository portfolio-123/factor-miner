import streamlit as st
import pandas as pd
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import datetime

from src.core.types import Analysis, SettingsForm


@dataclass
class AppState:
    factor_list_uid: Optional[str] = None

    is_viewing_live_dataset: bool = True

    active_dataset_file: Optional[str] = None

    analysis_settings: Optional[SettingsForm] = None

    debug_logs: List[str] = field(default_factory=list)

    analysis_id: Optional[str] = None

    formulas_data: Optional[pd.DataFrame] = None

    active_backup_version: Optional[str] = None

    edit_dataset_mode: bool = False

    access_token: Optional[str] = None
    fl_name: Optional[str] = None


def get_state() -> AppState:
    if "app_state" not in st.session_state:
        st.session_state.app_state = AppState()
    return st.session_state.app_state


def update_state(**kwargs) -> None:
    state = get_state()
    for key, value in kwargs.items():
        if not hasattr(state, key):
            raise ValueError(f"Unknown state key: {key}")
        setattr(state, key, value)


def add_debug_log(message: str, without_timestamp: bool = False) -> None:
    state = get_state()
    timestamp = datetime.now().strftime("%H:%M:%S")
    message = message if without_timestamp else f"[{timestamp}] {message}"
    state.debug_logs.append(message)
    # keep only the last 100 logs
    if len(state.debug_logs) > 100:
        state.debug_logs = state.debug_logs[-100:]


def merge_analysis_logs(analysis: Analysis) -> None:
    for log_entry in analysis.logs or []:
        add_debug_log(log_entry, without_timestamp=True)


def clear_debug_logs() -> None:
    state = get_state()
    state.debug_logs = []
    add_debug_log("Logs cleared")
