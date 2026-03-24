import streamlit as st

from src.core.config.constants import (
    DEFAULT_MIN_ALPHA,
    DEFAULT_TOP_PCT,
    DEFAULT_BOTTOM_PCT,
    DEFAULT_CORRELATION_THRESHOLD,
    DEFAULT_N_FACTORS,
    DEFAULT_MAX_NA_PCT,
    DEFAULT_MIN_IC,
)
from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import AnalysisParams
from src.internal.links import p123_link
from src.services.dataset_service import DatasetService
from src.ui.components.common import section_header
from src.ui.components.datasets import render_dataset_card
from src.workers.analysis_service import AnalysisService


def _get_default_settings() -> dict:
    return {
        "rank_by": "Alpha",
        "top_pct": DEFAULT_TOP_PCT,
        "bottom_pct": DEFAULT_BOTTOM_PCT,
        "min_alpha": DEFAULT_MIN_ALPHA,
        "min_ic": DEFAULT_MIN_IC,
        "n_factors": DEFAULT_N_FACTORS,
        "max_na_pct": DEFAULT_MAX_NA_PCT,
        "correlation_threshold": DEFAULT_CORRELATION_THRESHOLD,
    }


def _load_last_analysis_params() -> None:
    analyses = AnalysisService(
        st.session_state.get("user_uid") if INTERNAL_MODE else None
    ).list_all(st.query_params.get("fl_id"))
    if not analyses:
        st.toast("No previous analyses found for this dataset")
        return

    # list_all returns sorted by created_at desc, so first is most recent
    last_params = analyses[0].params

    for key, value in last_params.model_dump().items():
        st.session_state[key] = value


def _submit_analysis() -> None:
    fl_id = st.query_params.get("fl_id")
    user_uid = st.session_state.get("user_uid")
    dataset_version = DatasetService(
        st.session_state["dataset_details"]
    ).current_version
    analysis_id = AnalysisService(user_uid).next_analysis_id(fl_id)

    try:
        rank_by = st.session_state.get("rank_by", "Alpha")
        params = AnalysisParams(
            min_alpha=st.session_state.get("min_alpha", DEFAULT_MIN_ALPHA),
            top_pct=st.session_state["top_pct"],
            bottom_pct=st.session_state["bottom_pct"],
            correlation_threshold=st.session_state["correlation_threshold"],
            n_factors=st.session_state["n_factors"],
            max_na_pct=st.session_state["max_na_pct"],
            min_ic=float(st.session_state.get("min_ic", DEFAULT_MIN_IC)),
            rank_by=rank_by,
        )
        AnalysisService(user_uid).start(
            fl_id,
            analysis_id,
            dataset_version,
            params,
            access_token=st.session_state.get("access_token"),
        )
        st.session_state["_redirect_to_results"] = analysis_id

    except Exception as e:
        st.toast(f"Error starting analysis: {e}")


def create_form() -> None:
    if analysis_id := st.session_state.pop("_redirect_to_results", None):
        st.switch_page(
            st.session_state["pages"]["results"],
            query_params=(
                ("fl_id", st.query_params.get("fl_id")),
                ("id", analysis_id),
            ),
        )

    fl_id = st.query_params.get("fl_id")

    try:
        with DatasetService(st.session_state["dataset_details"]) as svc:
            active_dataset_metadata = svc.get_metadata()
    except FileNotFoundError:
        st.warning(
            f"No dataset found for this Factor List. [Generate]({p123_link(fl_id, 'generate')})"
            if INTERNAL_MODE
            else "No dataset found. Please select a valid .parquet file."
        )
        return
    except Exception as e:
        st.error(f"Failed to load dataset: {e}")
        return

    st.title("Create Analysis")
    render_dataset_card(active_dataset_metadata)
    _render_settings()

    _, col_last_settings, col_run = st.columns([3, 1, 1])
    with col_last_settings:
        st.button(
            "Use Last Settings",
            type="secondary",
            on_click=_load_last_analysis_params,
            width="stretch",
        )
    with col_run:
        st.button(
            "Run Analysis", type="primary", on_click=_submit_analysis, width="stretch"
        )


def _render_settings() -> None:
    defaults = _get_default_settings()

    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

    section_header("Portfolio Settings")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.radio(
            "Rank By",
            options=["Alpha", "Information Coefficient (IC)"],
            key="rank_by",
            horizontal=True,
            help="Select metric to rank factors by",
        )
    with col2:
        st.number_input(
            "Top X (Long) %",
            min_value=1.0,
            max_value=100.0,
            step=1.0,
            key="top_pct",
            help="Percentage of top-ranked stocks to go long",
        )
    with col3:
        st.number_input(
            "Bottom X (Short) %",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            key="bottom_pct",
            help="Percentage of bottom-ranked stocks to short (0 = long-only)",
        )

    section_header("Analysis Filters")
    rank_by = st.session_state.get("rank_by", "Alpha")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if rank_by == "Alpha":
            st.number_input(
                "Min. Abs. Annual Alpha (%)",
                min_value=0.0,
                max_value=100.0,
                step=0.1,
                key="min_alpha",
            )
        else:
            st.number_input(
                "Min. IC",
                min_value=0.0,
                max_value=1.0,
                step=0.01,
                key="min_ic",
            )
    with col2:
        st.number_input(
            "Max. Factors",
            min_value=1,
            max_value=100,
            step=1,
            key="n_factors",
            help="Maximum number of 'Best Factors' to select",
        )
    with col3:
        st.number_input(
            "Max. NA (%)",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            key="max_na_pct",
            help="If a factor has a higher percentage of NAs, it will be excluded",
        )
    with col4:
        st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            step=0.05,
            key="correlation_threshold",
            help="Maximum allowed correlation between selected factors",
        )
