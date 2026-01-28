import uuid

import streamlit as st

from src.core.constants import (
    DEFAULT_BENCHMARK,
    DEFAULT_MIN_ALPHA,
    DEFAULT_TOP_PCT,
    DEFAULT_BOTTOM_PCT,
)
from src.core.types import AnalysisParams
from src.services.dataset_service import dataset_service
from src.ui.components.datasets import load_active_dataset, render_dataset_card
from src.workers.analysis_service import analysis_service


def _submit_analysis_creation() -> None:
    fl_id = st.query_params.get("fl_id")

    dataset_version = dataset_service(fl_id).current_version
    analysis_id = uuid.uuid4().hex[:8]

    try:
        params = AnalysisParams(
            benchmark_ticker=st.session_state.get("benchmark_ticker"),
            min_alpha=st.session_state.get("min_alpha"),
            top_pct=st.session_state.get("top_pct"),
            bottom_pct=st.session_state.get("bottom_pct"),
            access_token=st.session_state.get("access_token"),
        )
        analysis_service.start(fl_id, analysis_id, dataset_version, params)
        st.session_state["_redirect_to_results"] = analysis_id
    except Exception as e:
        st.toast(f"Error starting analysis: {e}")


def create_form() -> None:
    if analysis_id := st.session_state.pop("_redirect_to_results", None):
        st.switch_page(
            st.session_state["pages"]["results"],
            query_params={
                "fl_id": st.query_params.get("fl_id"),
                "id": analysis_id,
            },
        )

    if not (active_dataset_metadata := load_active_dataset()):
        return

    render_dataset_card(active_dataset_metadata)

    _render_settings()

    with st.columns([4, 1])[1]:
        st.button(
            "Run Analysis",
            type="primary",
            width="stretch",
            on_click=_submit_analysis_creation,
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


