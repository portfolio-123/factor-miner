import streamlit as st

from src.core.constants import (
    DEFAULT_BENCHMARK,
    DEFAULT_MIN_ALPHA,
    DEFAULT_TOP_PCT,
    DEFAULT_BOTTOM_PCT,
)
from src.core.context import get_state
from src.core.types import SettingsForm
from src.services.dataset_service import (
    get_active_dataset_metadata,
    get_dataset_review_data,
)
from src.services.create_service import submit_settings, submit_analysis_creation
from src.ui.components.common import section_header
from src.ui.components.headers import navbar
from src.ui.components.datasets import (
    render_dataset_card,
    render_dataset_preview,
    render_dataset_statistics,
)


def create_form() -> None:
    navbar()
    try:
        active_dataset_metadata = get_active_dataset_metadata(st.query_params.get("fl_id"))
    except Exception:
        st.error(f"Failed to load dataset")
        return
        
    render_dataset_card(active_dataset_metadata)

    settings_tab, review_tab = st.tabs(["Settings", "Review"])
    with settings_tab:
        _render_settings()
    with review_tab:
        _render_review()


def _render_settings() -> None:
    state = get_state()

    section_header("Configuration")

    settings = state.analysis_settings

    col1, col2 = st.columns(2)
    with col1:
        benchmark_ticker = st.text_input(
            "Benchmark Ticker",
            value=settings.benchmark_ticker if settings else DEFAULT_BENCHMARK,
        )
    with col2:
        min_alpha = st.number_input(
            "Min Absolute Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            value=settings.min_alpha if settings else DEFAULT_MIN_ALPHA,
            step=0.1,
        )

    col1, col2 = st.columns(2)
    with col1:
        top_pct = st.number_input(
            "Top X (%)",
            min_value=1.0,
            max_value=100.0,
            value=settings.top_pct if settings else DEFAULT_TOP_PCT,
            step=1.0,
        )
    with col2:
        bottom_pct = st.number_input(
            "Bottom X (%)",
            min_value=1.0,
            max_value=100.0,
            value=settings.bottom_pct if settings else DEFAULT_BOTTOM_PCT,
            step=1.0,
        )

    with st.columns([4, 1])[1]:
        button_placeholder = st.empty()
        if button_placeholder.button("Continue", type="primary", width="stretch"):
            button_placeholder.button(
                "Processing...",
                type="primary",
                icon="spinner",
                disabled=True,
                width="stretch",
            )
            submit_settings(
                SettingsForm(
                    benchmark_ticker=benchmark_ticker,
                    min_alpha=min_alpha,
                    top_pct=top_pct,
                    bottom_pct=bottom_pct,
                )
            )


def _render_review() -> None:
    try:
        preview_df, stats = get_dataset_review_data()
    except Exception as e:
        st.error(f"Failed to load dataset preview: {e}")
        return

    if preview_df.empty:
        st.error("Dataset is empty")
        return

    render_dataset_statistics(stats)
    render_dataset_preview(preview_df)

    with st.columns([4, 1])[1]:
        st.button(
            "Run Analysis",
            type="primary",
            width="stretch",
            on_click=submit_analysis_creation,
        )
