import json
from pathlib import Path

import streamlit as st

from src.core.context import get_state
from src.core.utils import get_local_storage
from src.core.constants import DEFAULT_BENCHMARK


def check_required_fields() -> bool:
    """Check if required fields are filled to enable Continue button."""
    state = get_state()
    api_key = st.session_state.get('api_key', '')
    api_id = st.session_state.get('api_id', '')

    if state.is_internal_app:
        return all([api_key.strip(), api_id.strip()]) and state.dataset_path is not None
    else:
        dataset = st.session_state.get('dataset_path', '')

        return all([dataset.strip(), api_key.strip(), api_id.strip()])


def validate_inputs() -> tuple[bool, str]:
    """Validate all required inputs after continue button is clicked."""
    state = get_state()

    api_key = st.session_state.get('api_key', '')
    api_id = st.session_state.get('api_id', '')

    if state.is_internal_app:
        if state.dataset_path is None:
            return False, "Dataset file not found"

        if not api_key.strip():
            return False, "API Key is required"
        if not api_id.strip():
            return False, "API ID is required"
    else:
        dataset_path = st.session_state.get('dataset_path', '')

        if not dataset_path.strip():
            return False, "Dataset path is required"
        if not api_key.strip():
            return False, "API Key is required"
        if not api_id.strip():
            return False, "API ID is required"

        # Validate dataset file exists
        dataset_file = Path(dataset_path.strip())
        if not dataset_file.is_absolute():
            dataset_file = dataset_file.resolve()
        if not dataset_file.exists():
            return False, f"Dataset file not found: {dataset_path}"

    return True, ""


def load_saved_settings() -> dict:
    """Load settings from localStorage."""
    saved_data = get_local_storage().getAll() or {}
    try:
        return json.loads(saved_data.get('factor_eval_settings', '')) or {}
    except (json.JSONDecodeError, TypeError):
        return {}


def restore_session_defaults(state) -> None:
    """Restore form field values from app state when returning to step 1."""
    defaults = {
        'api_key': state.api_key,
        'api_id': state.api_id,
        'benchmark_ticker': state.benchmark_ticker or DEFAULT_BENCHMARK,
        'min_alpha': state.min_alpha,
        'top_x_pct': int(state.top_x_pct),
        'bottom_x_pct': int(state.bottom_x_pct),
    }

    for key, value in defaults.items():
        if key not in st.session_state and value is not None:
            st.session_state[key] = value

    if not state.is_internal_app:
        if 'dataset_path' not in st.session_state and state.dataset_path_input:
            st.session_state['dataset_path'] = state.dataset_path_input
