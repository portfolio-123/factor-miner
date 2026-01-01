import streamlit as st

from src.core.context import get_state, update_state
from src.services.processing import process_step1
from src.core.validation import (
    restore_session_defaults,
)
from src.ui.components import section_header
from src.core.constants import DEFAULT_BENCHMARK


def render() -> None:
    state = get_state()

    if state.user_payload:
        jwt_api_key = state.user_payload.get("apiKey")
        jwt_api_id = state.user_payload.get("apiId")

        if jwt_api_key:
            update_state(api_key=jwt_api_key)

        if jwt_api_id:
            update_state(api_id=jwt_api_id)

    restore_session_defaults(state)

    section_header("Data Sources")

    st.text_input(
        "Factor List UID",
        value=state.factor_list_uid or "",
        disabled=True,
        key="factor_list_uid",
    )

    if not state.factor_list_uid:
        st.warning("No fl_id provided in URL")
    elif not state.dataset_path:
        st.error(f"Dataset file not found for: {state.factor_list_uid}")

    section_header("Configuration")

    col1, col2 = st.columns(2)
    with col1:
        st.text_input(
            "Benchmark Ticker",
            placeholder=DEFAULT_BENCHMARK,
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

    api_key_present = bool(state.api_key)
    api_id_present = bool(state.api_id)

    if not api_key_present or not api_id_present:
        st.warning("Missing authentication details.")

    error_placeholder = st.empty()

    if state.step1_error:
        error_placeholder.error(state.step1_error)

    col1, col2, col3 = st.columns([2, 1, 1])
    with col3:
        if state.step1_loading:
            st.markdown(
                """
            <div class="spinner-button">
                <div class="spinner"></div>
            </div>
            """,
                unsafe_allow_html=True,
            )
        else:
            if st.button("Continue", type="primary", width="stretch"):
                update_state(step1_loading=True)
                st.rerun()

    # process continue action after rerun if loading flag is set
    if state.step1_loading:
        update_state(step1_loading=False)
        process_step1()
        st.rerun()
