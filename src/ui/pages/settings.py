import streamlit as st

from src.core.context import get_state
from src.services.processing import process_config
from src.ui.components import section_header


def render() -> None:
    state = get_state()

    section_header("Data Sources")

    st.text_input(
        "Factor List UID",
        value=state.factor_list_uid,
        disabled=True,
        key="factor_list_uid",
    )

    section_header("Configuration")

    col1, col2 = st.columns(2)
    with col1:
        st.text_input(
            "Benchmark Ticker",
            value=state.benchmark_ticker,
            key="benchmark_ticker",
        )
    with col2:
        st.number_input(
            "Min Absolute Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            value=state.min_alpha,
            step=0.1,
            key="min_alpha",
        )

    col1, col2 = st.columns(2)
    with col1:
        st.number_input(
            "Top X (%)",
            min_value=1,
            max_value=100,
            value=int(state.top_x_pct),
            step=1,
            key="top_x_pct",
        )
    with col2:
        st.number_input(
            "Bottom X (%)",
            min_value=1,
            max_value=100,
            value=int(state.bottom_x_pct),
            step=1,
            key="bottom_x_pct",
        )

    if state.config_error:
        st.error(state.config_error)

    _, col = st.columns([3, 1])
    with col:
        if st.button("Continue", type="primary", width="stretch"):
            with st.spinner(""):
                process_config()
            st.rerun()
