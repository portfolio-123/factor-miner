import json
from pathlib import Path

import streamlit as st

from src.core.context import get_state
from src.core.utils import detect_file_type, get_local_storage


def check_required_fields() -> bool:
    """Check if required fields are filled to enable Continue button."""
    state = get_state()
    api_key = st.session_state.get('api_key', '')
    api_id = st.session_state.get('api_id', '')

    if state.is_internal_app:
        return all([api_key.strip(), api_id.strip()]) and state.files_verified
    else:
        dataset = st.session_state.get('dataset_path', '')
        dataset_path = Path(dataset.strip()) if dataset.strip() else None
        is_parquet = dataset_path and dataset_path.exists() and detect_file_type(dataset_path) == 'parquet'

        if is_parquet:
            # Parquet files have formulas embedded in metadata
            return all([dataset.strip(), api_key.strip(), api_id.strip()])
        else:
            # CSV files require separate formulas file
            formulas = st.session_state.get('formulas_path', '')
            return all([dataset.strip(), formulas.strip(), api_key.strip(), api_id.strip()])


def validate_inputs() -> tuple[bool, str]:
    """Validate all required inputs after continue button is clicked."""
    state = get_state()

    api_key = st.session_state.get('api_key', '')
    api_id = st.session_state.get('api_id', '')

    if state.is_internal_app:
        if not state.files_verified:
            return False, state.files_verification_error or "Files not verified"

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

        # Only validate formulas path for CSV files (parquet has embedded metadata)
        if detect_file_type(dataset_file) != 'parquet':
            formulas_path = st.session_state.get('formulas_path', '')
            if not formulas_path.strip():
                return False, "Formulas path is required for CSV datasets"

            formulas_file = Path(formulas_path.strip())
            if not formulas_file.is_absolute():
                formulas_file = formulas_file.resolve()
            if not formulas_file.exists():
                return False, f"Formulas file not found: {formulas_path}"

    return True, ""


def load_saved_settings() -> dict:
    """Load settings from localStorage."""
    saved_data = get_local_storage().getAll() or {}
    try:
        return json.loads(saved_data.get('factor_eval_settings', '')) or {}
    except (json.JSONDecodeError, TypeError):
        return {}


def apply_saved_settings(saved: dict, is_internal: bool) -> None:
    """Apply saved settings to session_state."""
    for key in ('api_key', 'api_id', 'benchmark_ticker'):
        if saved.get(key):
            st.session_state[key] = saved[key]

    if not is_internal:
        for key in ('dataset_path', 'formulas_path'):
            if saved.get(key):
                st.session_state[key] = saved[key]

    if saved.get('min_alpha') is not None:
        st.session_state['min_alpha'] = float(saved['min_alpha'])
    for key in ('top_x_pct', 'bottom_x_pct'):
        if saved.get(key) is not None:
            st.session_state[key] = int(saved[key])

    st.session_state['step1_error'] = None


def restore_session_defaults(state) -> None:
    """Restore form field values from app state when returning to step 1."""
    defaults = {
        'api_key': state.api_key,
        'api_id': state.api_id,
        'benchmark_ticker': state.benchmark_ticker or "SPY:USA",
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
        if 'formulas_path' not in st.session_state and state.formulas_path_input:
            st.session_state['formulas_path'] = state.formulas_path_input
