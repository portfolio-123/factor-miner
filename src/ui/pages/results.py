import streamlit as st
import pandas as pd
from src.core.context import get_state, update_state, merge_analysis_logs
from src.core.types import AnalysisStatus, FilterParams
from src.ui.components.common import copy_button, section_header
from src.ui.components.headers import navbar
from src.ui.components.tables import render_results_table
from src.ui.components.datasets import render_dataset_card
from src.ui.components.analyses import render_analysis_params
from src.core.utils import add_formula_column, deserialize_dataframe
from src.core.calculations import select_best_features as _select_best_features
from src.workers.manager import read_analysis
from src.services.dataset_service import get_backup_dataset_metadata


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
def _render_analysis_progress(analysis_id: str) -> None:
    analysis = read_analysis(analysis_id)

    if analysis.status == AnalysisStatus.COMPLETED:
        merge_analysis_logs(analysis)
        st.success("Analysis completed! Refreshing...")
        st.rerun(scope="app")
        return

    if analysis.status == AnalysisStatus.ERROR:
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

        st.progress(
            progress.completed / progress.total,
            text=f"{progress.completed} / {progress.total} factors analyzed",
        )

        if progress.current_factor:
            st.info(f"Analyzing: **{progress.current_factor}**")
        else:
            st.info("Starting...")


@st.fragment
def _render_filter_and_results(
    metrics: pd.DataFrame, corr_matrix: pd.DataFrame
) -> None:
    state = get_state()
    analysis = read_analysis(state.analysis_id)

    section_header("Filter Parameters")

    col1, col2, _ = st.columns([1, 1, 2])

    with col1:
        correlation_threshold = st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            value=st.session_state.get("filter_correlation", 0.5),
            key="filter_correlation",
            step=0.05,
        )

    with col2:
        n_features = st.number_input(
            "Number of Features",
            min_value=1,
            max_value=100,
            value=st.session_state.get("filter_n_features", 10),
            key="filter_n_features",
            step=1,
        )

    best_features = select_best_features_cached(
        state.analysis_id,
        metrics,
        corr_matrix,
        FilterParams(n_features, correlation_threshold, analysis.params.min_alpha),
    )

    section_header("Best Performing Factors")

    filtered_best_features = render_results_table(best_features, metrics)

    _render_action_buttons(filtered_best_features)


def _prepare_download_csv(display_df: pd.DataFrame) -> str:
    state = get_state()
    download_df = add_formula_column(display_df, state.formulas_data)
    return download_df.to_csv(index=False)


def _render_action_buttons(display_df: pd.DataFrame | None) -> None:
    state = get_state()

    if display_df is None or display_df.empty:
        return

    _, col1, col2 = st.columns([3, 1, 1])

    # tab delimited for copy to clipboard (without Formula)
    csv_to_copy = display_df.to_csv(index=False, sep="\t")

    # comma delimited for file download (with Formula in second position)
    csv_to_download = _prepare_download_csv(display_df)

    with col1:
        copy_button(csv_to_copy, label="Copy to Clipboard", width="stretch")

    with col2:
        st.download_button(
            type="primary",
            label="Download CSV",
            data=csv_to_download,
            file_name=f"{state.factor_list_uid}_best_features.csv",
            mime="text/csv",
            width="stretch",
        )


def results(analysis_id: str) -> None:
    navbar()

    analysis = read_analysis(analysis_id)
    if not analysis:
        st.error("Analysis not found")
        return

    try:
        dataset_metadata = get_backup_dataset_metadata(
            get_state().factor_list_uid, analysis_id.split("/")[1]
        )
        update_state(
            formulas_data=pd.DataFrame(dataset_metadata.formulas),
            analysis_id=analysis_id,
        )
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return

    render_dataset_card(dataset_metadata)

    render_analysis_params(analysis.params)

    if analysis.status == AnalysisStatus.ERROR:
        print("hola")
        st.error((analysis.error or "Analysis failed"))
        return

    if analysis.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        _render_analysis_progress(analysis_id)
        return

    # completed: load results and render
    merge_analysis_logs(analysis)
    metrics, corr = _deserialize_results(
        analysis.results["all_metrics"],
        analysis.results["all_corr_matrix"],
    )
    _render_filter_and_results(metrics, corr)
