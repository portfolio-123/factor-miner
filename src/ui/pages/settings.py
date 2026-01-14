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
    )

    section_header("Configuration")

    col1, col2 = st.columns(2)
    with col1:
        benchmark_ticker = st.text_input(
            "Benchmark Ticker",
            value=state.benchmark_ticker,
        )
    with col2:
        min_alpha = st.number_input(
            "Min Absolute Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            value=state.min_alpha,
            step=0.1,
        )

    col1, col2 = st.columns(2)
    with col1:
        top_x_pct = st.number_input(
            "Top X (%)",
            min_value=1,
            max_value=100,
            value=int(state.top_x_pct),
            step=1,
        )
    with col2:
        bottom_x_pct = st.number_input(
            "Bottom X (%)",
            min_value=1,
            max_value=100,
            value=int(state.bottom_x_pct),
            step=1,
        )

    if state.config_error:
        st.error(state.config_error)

    with st.columns([4, 1])[1]:
        button_placeholder = st.empty()
        if button_placeholder.button("Continue", type="primary", width="stretch"):
            button_placeholder.button(
                "Processing...", type="primary", icon="spinner", disabled=True, width="stretch"
            )
            process_config(
                benchmark_ticker=benchmark_ticker,
                min_alpha=min_alpha,
                top_x_pct=top_x_pct,
                bottom_x_pct=bottom_x_pct,
            )
            st.rerun()
