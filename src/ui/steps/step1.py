import streamlit as st
from pathlib import Path

from src.core.context import get_state
from src.core.processing import process_step1
from src.core.utils import detect_file_type
from src.core.validation import (
    check_required_fields,
    load_saved_settings,
    apply_saved_settings,
    restore_session_defaults,
)
from src.ui.components import section_header


def render() -> None:
    state = get_state()
    saved = load_saved_settings()
    has_saved = bool(saved.get('api_key') or saved.get('dataset_path') or saved.get('formulas_path') or saved.get('api_id'))

    if st.session_state.pop('_apply_saved_settings', False):
        apply_saved_settings(saved, state.is_internal_app)

    restore_session_defaults(state)

    section_header("Data Sources")

    if state.is_internal_app:
        # Internal app mode - file-system implementation
        st.text_input(
            "Factor List UID",
            value=state.factor_list_uid or "",
            disabled=True,
            key="factor_list_uid"
        )

        if state.files_verified:
            dataset_name = state.auto_dataset_path.stem if state.auto_dataset_path else ""
            formulas_display = state.auto_formulas_path.name if state.auto_formulas_path else ""

            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Dataset", value=dataset_name, disabled=True)
            with col2:
                st.text_input("Formulas", value=formulas_display, disabled=True)
        elif state.files_verification_error:
            st.error(state.files_verification_error)
        else:
            st.warning("No fl_id provided in URL")
    else:
        # External app mode - file path inputs
        dataset_path = st.session_state.get('dataset_path', '')
        dataset_file = Path(dataset_path.strip()) if dataset_path.strip() else None
        is_parquet = dataset_file and dataset_file.exists() and detect_file_type(dataset_file) == 'parquet'

        if is_parquet:
            st.text_input(
                "Dataset Path",
                placeholder="data/dataset.parquet",
                key="dataset_path",
            )
            st.caption(
                "**Note:** Parquet files include formulas in metadata. Your dataset must contain 'Last Close' for price data."
            )
        else:
            col1, col2 = st.columns(2)

            with col1:
                st.text_input(
                    "Dataset Path",
                    placeholder="data/dataset.parquet",
                    key="dataset_path",
                )

            with col2:
                st.text_input(
                    "Formulas Path",
                    placeholder="data/formulas.csv",
                    key="formulas_path",
                )

            st.caption(
                "**Note:** Your dataset must contain a column named 'Last Close' for price data. We support both CSV and Parquet file formats."
            )

    section_header("Configuration")

    col1, col2 = st.columns(2)
    with col1:
        st.text_input(
            "Benchmark Ticker",
            placeholder="SPY:USA",
            key="benchmark_ticker",
        )
    with col2:
        st.number_input(
            "Min Absolute Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            step=0.1,
            key="min_alpha",
        )

    col1, col2 = st.columns(2)
    with col1:
        st.number_input(
            "Top X (%)",
            min_value=1,
            max_value=100,
            step=1,
            key="top_x_pct",
        )
    with col2:
        st.number_input(
            "Bottom X (%)",
            min_value=1,
            max_value=100,
            step=1,
            key="bottom_x_pct",
        )

    section_header("Authentication")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.text_input(
            "API Key",
            placeholder="Enter API Key",
            key="api_key",
        )
    with col2:
        st.text_input(
            "API ID",
            placeholder="Enter API ID",
            key="api_id",
        )

    error_placeholder = st.empty()

    if st.session_state.get('step1_error'):
        error_placeholder.error(st.session_state['step1_error'])

    # check if required fields are filled to enable Continue button
    can_continue = check_required_fields()

    col1, col2, col3 = st.columns([2, 1, 1])
    with col2:
        if has_saved:
            if st.button("Use saved settings", width='stretch'):
                # set flag to apply settings on next render (before widgets are created)
                st.session_state['_apply_saved_settings'] = True
                st.rerun()
    with col3:
        is_loading = st.session_state.get('step1_loading', False)

        if is_loading:
            st.markdown('''
            <div class="spinner-button">
                <div class="spinner"></div>
            </div>
            ''', unsafe_allow_html=True)
        else:
            if st.button("Continue", type="primary", width='stretch', disabled=not can_continue):
                st.session_state.step1_loading = True
                st.rerun()

    # process continue action after rerun if loading flag is set
    if st.session_state.get('step1_loading', False):
        st.session_state.step1_loading = False
        process_step1()
        st.rerun()
