import streamlit as st
from datetime import datetime

from src.core.types import Analysis


def add_debug_log(message: str, without_timestamp: bool = False) -> None:
    if "debug_logs" not in st.session_state:
        st.session_state.debug_logs = []

    timestamp = datetime.now().strftime("%H:%M:%S")
    message = message if without_timestamp else f"[{timestamp}] {message}"
    st.session_state.debug_logs.append(message)
    # keep only the last 100 logs
    if len(st.session_state.debug_logs) > 100:
        st.session_state.debug_logs = st.session_state.debug_logs[-100:]


def merge_analysis_logs(analysis: Analysis) -> None:
    for log_entry in analysis.logs or []:
        add_debug_log(log_entry, without_timestamp=True)


def clear_debug_logs() -> None:
    st.session_state.debug_logs = []
    add_debug_log("Logs cleared")
