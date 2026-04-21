import streamlit as st
import polars as pl
from src.core.config.constants import RANK_CONFIG
from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import AnalysisParams
from src.internal.links import p123_link
from src.services.dataset_service import DatasetService
from src.ui.components.common import section_header
from src.ui.components.datasets import render_dataset_card
from src.workers.analysis_service import AnalysisService


@st.dialog("Factor Sorting", width="large")
def factor_sorting_dialog(factors_df: pl.DataFrame, asc_factors: list[str]):

    df_with_check = factors_df.with_columns(pl.col("name").is_in(asc_factors).alias("asc"))

    edited: pl.DataFrame = st.data_editor(  # type: ignore[assignment]
        df_with_check,
        column_config={"asc": st.column_config.CheckboxColumn("Asc (Lower Values)", width="small")},
        disabled=factors_df.columns,
        hide_index=True,
    )

    _, col_cancel, col_save = st.columns([5, 1, 1])
    with col_cancel:
        if st.button("Cancel", width="stretch"):
            st.rerun()
    with col_save:
        if st.button("Save Changes", type="primary", width="stretch"):
            selected_formulas = edited.filter(pl.col("asc"))["name"].to_list()
            st.session_state["asc_factors"] = selected_formulas
            st.rerun()


def _load_last_analysis_params(fl_id: str) -> None:
    analyses = AnalysisService(st.session_state.get("user_uid") if INTERNAL_MODE else None).list_all(fl_id)
    if not analyses:
        st.toast("No previous analyses found for this dataset")
        return

    for key, value in analyses[0].params.model_dump().items():
        st.session_state[key] = value


def _submit_analysis(fl_id: str) -> None:
    if st.session_state.get("high_quantile") == 0 and st.session_state.get("low_quantile") == 0:
        st.toast("High and Low Quantiles cannot both be 0%")
        return

    user_uid = st.session_state.get("user_uid")
    dataset_version = DatasetService(st.session_state["dataset_details"]).current_version
    analysis_id = AnalysisService(user_uid).next_analysis_id(fl_id)

    try:
        params = AnalysisParams(**{field: st.session_state[field] for field in AnalysisParams.model_fields})
        AnalysisService(user_uid).start(
            fl_id, analysis_id, dataset_version, params, api_credentials=st.session_state.get("api_credentials")
        )
        st.session_state["_redirect_to_results"] = analysis_id

    except Exception as e:
        st.toast(f"Error starting analysis: {e}")


def create_form() -> None:

    fl_id = st.query_params.get("fl_id")
    if not fl_id:
        st.warning("No Factor List selected. Please select a Factor List to view analysis history.")
        return

    if analysis_id := st.session_state.pop("_redirect_to_results", None):
        st.switch_page(st.session_state["pages"]["results"], query_params=(("fl_id", fl_id), ("id", analysis_id)))

    try:
        with DatasetService(st.session_state["dataset_details"]) as svc:
            active_dataset_metadata = svc.get_metadata()
            st.session_state["formulas_data"] = active_dataset_metadata.formulas_df
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
        st.button("Use Last Settings", type="secondary", on_click=_load_last_analysis_params, args=(fl_id,), width="stretch")
    with col_run:
        st.button("Run Analysis", type="primary", on_click=_submit_analysis, args=(fl_id,), width="stretch")


def _render_settings() -> None:
    defaults = AnalysisParams()
    for key, value in defaults.model_dump().items():
        if key not in st.session_state:
            st.session_state[key] = value

    section_header("Portfolio Settings")
    col1, col2, col3, col4 = st.columns(4)
    with col1:

        def _on_rank_change():
            st.session_state["min_rank_metric"] = RANK_CONFIG[st.session_state["rank_by"]]["default"]

        st.radio(
            "Rank By",
            options=RANK_CONFIG,
            format_func=lambda v: RANK_CONFIG[v]["metric_label"],
            key="rank_by",
            horizontal=True,
            on_change=_on_rank_change,
            help="Select metric to rank factors by",
        )
    with col2:
        st.number_input(
            "High Quantile (%)",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            key="high_quantile",
            help="Percentage of top-ranked stocks to include in the high quantile portfolio",
        )
    with col3:
        st.number_input(
            "Low Quantile (%)",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            key="low_quantile",
            help="Percentage of bottom-ranked stocks to include in the low quantile portfolio (0 = high quantile only)",
        )

    with col4:
        st.number_input(
            "Max. Return %",
            min_value=20.0,
            max_value=1000.0,
            step=10.0,
            key="max_return_pct",
            help="Exclude date-stock pairs where return exceeds this %",
        )

    analyzable_factors = st.session_state["formulas_data"]

    asc_factors = st.session_state.get("asc_factors", [])

    if st.button(
        f"Factor Sorting ({len(analyzable_factors) - len(asc_factors)} desc, {len(asc_factors)} asc)",
        disabled=st.session_state.get("auto_detect_direction") == True,
    ):
        factor_sorting_dialog(analyzable_factors, asc_factors)

    st.toggle(
        "Auto-detect direction",
        help="Automatically inverts factors with a negative IC. Overrides manual sorting metrics",
        key="auto_detect_direction",
    )

    section_header("Analysis Filters")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        rank_by = st.session_state["rank_by"]
        rank_config = RANK_CONFIG[rank_by]
        metric_label = rank_config["metric_label"]
        input_settings = rank_config["input_settings"]
        st.number_input(f"Min. {metric_label}", **input_settings, key="min_rank_metric")
    with col2:
        st.number_input(
            "Max. Factors", min_value=1, max_value=100, step=1, key="n_factors", help="Maximum number of 'Best Factors' to select"
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
