import streamlit as st

from src.core.constants import (
    DEFAULT_BENCHMARK,
    DEFAULT_MIN_ALPHA,
    DEFAULT_TOP_PCT,
    DEFAULT_BOTTOM_PCT,
)
from src.services.dataset_service import get_dataset_review_data
from src.services.create_service import submit_analysis_creation
from src.ui.components.headers import navbar
from src.ui.components.datasets import (
    load_active_dataset,
    render_dataset_card,
    render_dataset_preview,
    render_dataset_statistics,
)


def create_form() -> None:
    if redirect_info := st.session_state.pop("_redirect_to_results", None):
        st.switch_page(
            st.session_state["pages"]["results"],
            query_params={
                "fl_id": redirect_info["fl_id"],
                "id": redirect_info["analysis_id"],
            },
        )

    navbar()

    if not (active_dataset_metadata := load_active_dataset()):
        return

    render_dataset_card(active_dataset_metadata)

    settings_tab, review_tab = st.tabs(["Settings", "Review"])
    with settings_tab:
        _render_settings()
    with review_tab:
        _render_review()

    with st.columns([4, 1])[1]:
        st.button(
            "Run Analysis",
            type="primary",
            width="stretch",
            on_click=submit_analysis_creation,
        )


def _render_settings() -> None:
    col1, col2 = st.columns(2)
    with col1:
        st.text_input(
            "Benchmark Ticker",
            value=DEFAULT_BENCHMARK,
            key="benchmark_ticker",
        )
    with col2:
        st.number_input(
            "Min Absolute Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            value=DEFAULT_MIN_ALPHA,
            step=0.1,
            key="min_alpha",
        )

    col1, col2 = st.columns(2)
    with col1:
        st.number_input(
            "Top X (%)",
            min_value=1.0,
            max_value=100.0,
            value=DEFAULT_TOP_PCT,
            step=1.0,
            key="top_pct",
        )
    with col2:
        st.number_input(
            "Bottom X (%)",
            min_value=1.0,
            max_value=100.0,
            value=DEFAULT_BOTTOM_PCT,
            step=1.0,
            key="bottom_pct",
        )


def _render_review() -> None:
    try:
        fl_id = st.query_params.get("fl_id")
        dataset_preview, stats = get_dataset_review_data(fl_id)
    except Exception:
        st.error("Failed to load dataset preview")
        return

    if dataset_preview.empty:
        st.error("Dataset is empty")
        return

    render_dataset_statistics(stats)
    render_dataset_preview(dataset_preview)
