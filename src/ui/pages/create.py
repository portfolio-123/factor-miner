import uuid

import streamlit as st

from src.core.config.constants import (
    DEFAULT_BENCHMARK,
    DEFAULT_MIN_ALPHA,
    DEFAULT_TOP_PCT,
    DEFAULT_BOTTOM_PCT,
    DEFAULT_CORRELATION_THRESHOLD,
    DEFAULT_N_FACTORS,
    DEFAULT_MAX_NA_PCT,
    DEFAULT_MIN_IC,
)
from src.core.types.models import AnalysisParams
from src.services.dataset_service import DatasetService
from src.ui.components.common import section_header
from src.ui.components.datasets import load_active_dataset, render_dataset_card
from src.workers.analysis_service import analysis_service


def _submit_analysis_creation() -> None:
    fl_id = st.query_params.get("fl_id")

    dataset_version = DatasetService(fl_id).current_version
    analysis_id = uuid.uuid4().hex[:8]

    try:
        params = AnalysisParams(
            benchmark_ticker=st.session_state.get("benchmark_ticker"),
            min_alpha=st.session_state.get("min_alpha"),
            top_pct=st.session_state.get("top_pct"),
            bottom_pct=st.session_state.get("bottom_pct"),
            correlation_threshold=st.session_state.get("correlation_threshold"),
            n_factors=st.session_state.get("n_factors"),
            max_na_pct=st.session_state.get("max_na_pct"),
            min_ic=float(st.session_state.get("min_ic", DEFAULT_MIN_IC)),
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

    st.title("Create Analysis")
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
    section_header("Portfolio Settings")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.text_input(
            "Benchmark Ticker",
            value=DEFAULT_BENCHMARK,
            key="benchmark_ticker",
        )
    with col2:
        st.number_input(
            "Top X (Long) %",
            min_value=1.0,
            max_value=100.0,
            value=DEFAULT_TOP_PCT,
            step=1.0,
            key="top_pct",
            help="Percentage of top-ranked stocks to go long",
        )
    with col3:
        st.number_input(
            "Bottom X (Short) %",
            min_value=1.0,
            max_value=100.0,
            value=DEFAULT_BOTTOM_PCT,
            step=1.0,
            key="bottom_pct",
            help="Percentage of bottom-ranked stocks to short",
        )

    section_header("Analysis Filters")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.number_input(
            "Min. Abs. Annual Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            value=DEFAULT_MIN_ALPHA,
            step=0.1,
            key="min_alpha",
        )
    with col2:
        st.number_input(
            "Min. IC",
            min_value=0.0,
            max_value=1.0,
            value=DEFAULT_MIN_IC,
            step=0.01,
            key="min_ic",
        )
    with col3:
        st.number_input(
            "Max. Factors",
            min_value=1,
            max_value=100,
            value=DEFAULT_N_FACTORS,
            step=1,
            key="n_factors",
            help="Maximum number of 'Best Factors' to select",
        )
    with col4:
        st.number_input(
            "Max. NA (%)",
            min_value=0.0,
            max_value=100.0,
            value=DEFAULT_MAX_NA_PCT,
            step=1.0,
            key="max_na_pct",
            help="If a factor has a higher percentage of NAs, it will be excluded",
        )
    with col5:
        st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            value=DEFAULT_CORRELATION_THRESHOLD,
            step=0.05,
            key="correlation_threshold",
            help="Maximum allowed correlation between selected factors",
        )
