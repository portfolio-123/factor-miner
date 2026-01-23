import streamlit as st
import pandas as pd
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured
from src.core.context import merge_analysis_logs
from src.core.types import AnalysisParams, AnalysisStatus, FilterParams
from src.ui.components.common import render_info_item, section_header
from src.ui.components.headers import navbar
from src.ui.components.tables import render_results_table
from src.ui.components.datasets import render_dataset_card
from src.ui.components.analyses import render_analysis_notes
from src.core.utils import add_formula_column, deserialize_dataframe
from src.core.calculations import select_best_features as _select_best_features
from src.workers.manager import read_analysis
from src.services.dataset_service import get_dataset_metadata


@st.cache_data
def _deserialize_results(
    metrics_json: str, corr_json: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return deserialize_dataframe(metrics_json, corr_json)


@st.cache_data
def select_best_features_cached(
    analysis_id: str,
    _metrics: pd.DataFrame,
    _corr_matrix: pd.DataFrame,
    params: FilterParams,
):
    return _select_best_features(
        _metrics,
        _corr_matrix,
        N=params.n_features,
        correlation_threshold=params.correlation_threshold,
        a_min=params.min_alpha,
    )


@st.fragment(run_every="0.5s")
def _render_analysis_progress(fl_id: str, analysis_id: str) -> None:
    analysis = read_analysis(fl_id, analysis_id)

    if not analysis:
        st.error("Analysis not found")
        return

    if analysis.status == AnalysisStatus.SUCCESS:
        merge_analysis_logs(analysis)
        st.success("Analysis completed! Refreshing...")
        st.rerun(scope="app")

    if analysis.status == AnalysisStatus.FAILED:
        merge_analysis_logs(analysis)
        st.error((analysis.error or "Analysis failed").split("\n")[0])
        return

    progress = analysis.progress
    if not progress:
        st.info("Starting analysis...")
        return

    with st.columns([1, 2, 1])[1]:
        st.space(100)
        st.subheader("Running Factor Analysis")

        completed = progress.get("completed", 0)
        total = progress.get("total", 1)
        current_factor = progress.get("current_factor")

        st.progress(
            completed / total if total > 0 else 0,
            text=f"{completed} / {total} factors analyzed",
        )

        if current_factor:
            st.info(f"Analyzing: **{current_factor}**")
        else:
            st.info("Starting...")


@st.fragment
def _render_filter_and_results(
    analysis_id: str,
    params: AnalysisParams,
    metrics: pd.DataFrame,
    corr_matrix: pd.DataFrame,
) -> None:
    section_header("Parameters")

    col_analysis, col_sep, col_filter = st.columns([2, 0.1, 2])

    with col_analysis:
        param_items = [
            render_info_item("Min Alpha", f"{params.min_alpha}%"),
            render_info_item("Top X", f"{params.top_pct}%"),
            render_info_item("Bottom X", f"{params.bottom_pct}%"),
        ]
        st.html(f'<div class="dataset-info-group">{"".join(param_items)}</div>')

    with col_sep:
        st.html(
            '<div style="border-left: 1px solid #e0e0e0; height: 60px; margin: 0 auto;"></div>'
        )

    with col_filter:
        subcol1, subcol2 = st.columns(2)
        with subcol1:
            correlation_threshold = st.slider(
                "Correlation Threshold",
                min_value=0.0,
                max_value=1.0,
                value=st.session_state.get("filter_correlation", 0.5),
                key="filter_correlation",
                step=0.05,
            )
        with subcol2:
            n_features = st.number_input(
                "Number of Features",
                min_value=1,
                max_value=100,
                value=st.session_state.get("filter_n_features", 10),
                key="filter_n_features",
                step=1,
            )

    best_features = select_best_features_cached(
        analysis_id,
        metrics,
        corr_matrix,
        FilterParams(n_features, correlation_threshold, params.min_alpha),
    )

    section_header("Best Performing Factors")

    filtered_best_features = render_results_table(best_features, metrics)

    _render_action_buttons(filtered_best_features)


def _prepare_download_csv(display_df: pd.DataFrame) -> str:
    formulas_data = st.session_state.get("formulas_data")
    download_df = add_formula_column(display_df, formulas_data)
    return download_df.to_csv(index=False)


def _render_action_buttons(display_df: pd.DataFrame | None) -> None:
    if display_df is None or display_df.empty:
        return

    fl_id = st.query_params.get("fl_id")

    _, col1, col2 = st.columns([3, 1, 1])

    # tab delimited for copy to clipboard (without Formula)
    csv_to_copy = display_df.to_csv(index=False, sep="\t")

    # comma delimited for file download (with Formula in second position)
    csv_to_download = _prepare_download_csv(display_df)

    with col1:
        if st.button(type="primary", label="Copy to Clipboard", width="stretch"):
            copy_to_clipboard_unsecured(csv_to_copy)
            copy_to_clipboard(csv_to_copy)
            st.toast("Best features copied to clipboard")

    with col2:
        st.download_button(
            type="primary",
            label="Download CSV",
            data=csv_to_download,
            file_name=f"{fl_id}_best_features.csv",
            mime="text/csv",
            width="stretch",
        )


def results() -> None:
    fl_id = st.query_params.get("fl_id")
    if not (analysis_id := st.query_params.get("id")):
        st.error("Missing analysis id")
        return

    navbar()

    analysis = read_analysis(fl_id, analysis_id)
    if not analysis:
        st.error("Analysis not found")
        return

    try:
        dataset_metadata = get_dataset_metadata(fl_id, analysis.dataset_version)
        st.session_state.formulas_data = pd.DataFrame(dataset_metadata.formulas)
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return

    render_dataset_card(dataset_metadata)

    if analysis.status == AnalysisStatus.FAILED:
        st.subheader("Analysis Failed")
        st.error((analysis.error or "Analysis failed").split("\n")[0])
        return

    if analysis.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        _render_analysis_progress(fl_id, analysis_id)
        return

    # completed: load results and render
    merge_analysis_logs(analysis)

    render_analysis_notes(analysis)
    metrics, corr = _deserialize_results(
        analysis.results["all_metrics"],
        analysis.results["all_corr_matrix"],
    )
    _render_filter_and_results(analysis.id, analysis.params, metrics, corr)
