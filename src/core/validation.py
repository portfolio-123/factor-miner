import streamlit as st

from src.core.constants import DEFAULT_BENCHMARK


def restore_step1_form(state) -> None:
    """Restore form field values from app state when returning to step 1."""
    defaults = {
        "api_key": state.api_key,
        "api_id": state.api_id,
        "benchmark_ticker": state.benchmark_ticker or DEFAULT_BENCHMARK,
        "min_alpha": state.min_alpha,
        "top_x_pct": int(state.top_x_pct),
        "bottom_x_pct": int(state.bottom_x_pct),
    }

    for key, value in defaults.items():
        if key not in st.session_state and value is not None:
            st.session_state[key] = value
