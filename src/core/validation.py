import json

import streamlit as st

from src.core.context import get_state
from src.core.utils import get_local_storage
from src.core.constants import DEFAULT_BENCHMARK


def validate_inputs() -> tuple[bool, str]:
    """Validate all required inputs after continue button is clicked."""
    state = get_state()

    if state.dataset_path is None:
        return False, "Dataset file not found"

    return True, ""


def load_saved_settings() -> dict:
    """Load settings from localStorage."""
    saved_data = get_local_storage().getAll() or {}
    try:
        return json.loads(saved_data.get("factor_eval_settings", "")) or {}
    except (json.JSONDecodeError, TypeError):
        return {}


def restore_session_defaults(state) -> None:
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
