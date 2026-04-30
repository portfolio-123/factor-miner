import streamlit as st
import polars as pl

from src.core.types.models import RANK_CONFIG, AnalysisStatus, BenchmarkDisplayResults
from src.internal.errors import format_analysis_error
from src.ui.components.common import render_info_item
from src.ui.components.tables import render_conflict_explorer, render_results_table, render_correlation_matrix
from src.ui.components.datasets import render_dataset_card
from src.ui.components.analyses import render_analysis_notes, show_analysis_logs_modal, render_analysis_progress
from src.core.utils.common import deserialize_dataframe, format_runtime, format_timestamp
from src.workers.analysis_service import AnalysisService
from src.services.dataset_service import BackupDatasetService


def render_benchmark_badges(benchmark: BenchmarkDisplayResults) -> None:
    b1, b2 = st.columns(2)
    with b1:
        st.badge(f"Total Bench Ret: {round(benchmark['total_benchmark_return'], 2)}%")
    with b2:
        st.badge(f"Ann. Bench Ret: {round(benchmark['annualized_benchmark_return'], 2)}%")


def results() -> None:
    fl_id = st.query_params.get("fl_id")
    user_uid = st.session_state.get("user_uid")
    analysis_id = st.query_params.get("id")

    if not analysis_id or not fl_id:
        st.error("Missing analysis details")
        return

    analysis_service = AnalysisService(user_uid)
    analysis = analysis_service.get(fl_id, analysis_id)
    if not analysis:
        st.error("Analysis not found")
        return

    try:
        svc = BackupDatasetService(st.session_state["dataset_details"])
        dataset_metadata = svc.get_metadata(analysis.dataset_version, svc.current_version)
        st.session_state["formulas_data"] = dataset_metadata.formulas_df
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return
    created_on = format_timestamp(analysis.created_at)
    status = analysis.status

    header_left, header_right = st.columns([8, 1])
    with header_left:
        st.html(
            f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">'
            f'Analysis Results <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">{created_on}</span>'
            f"</p>"
        )
    with header_right:
        if st.button("Logs", type="primary", key="header_logs", width="stretch"):
            show_analysis_logs_modal(analysis_id)

    if status == AnalysisStatus.FAILED:
        st.error(format_analysis_error(analysis.error, analysis.error_type))
        return

    if status == AnalysisStatus.PENDING or status == AnalysisStatus.RUNNING:
        render_analysis_progress(fl_id, analysis_id)
        return

    if not analysis.results:
        st.error("No results found for this analysis")
        return
    all_metrics_df = deserialize_dataframe(analysis.results.all_metrics)
    corr_matrix_df = deserialize_dataframe(analysis.results.all_corr_matrix)

    p = analysis.params
    rank_by = p.rank_by
    rank_config = RANK_CONFIG[rank_by]
    metric_label = rank_config.metric_label

    sort_by, is_desc = rank_config.get_sorting(high_q=p.high_quantile)
    quantile_renames = rank_config.get_renames(low_q=p.low_quantile, high_q=p.high_quantile)

    all_metrics_df = all_metrics_df.sort(sort_by, descending=is_desc).with_row_index("rank", offset=1)

    best_feature_names = analysis.results.best_feature_names
    factor_classifications = analysis.results.factor_classifications
    na_excluded_count = sum(1 for c in factor_classifications.values() if c == "high_na")
    st.success(
        f"Analysis completed in {format_runtime(analysis.started_at, analysis.finished_at)}. "
        f"Found **{len(best_feature_names)}** of **{p.n_factors}** requested Best Factors. "
        f"Number of factors excluded by NAs: {na_excluded_count}."
    )
    if analysis.results.first_valid_date:
        st.warning(
            f"Due to invalid factor data on certain dates, the dataset was analyzed starting from **{analysis.results.first_valid_date}**"
        )

    settings_tab, best_factors_tab, all_factors_tab, correlations_tab = st.tabs(["Settings", "Best Factors", "All Factors", "Correlations"])

    with settings_tab:
        render_dataset_card(dataset_metadata)

        with st.container(border=True):
            st.markdown("#### Analysis Settings", unsafe_allow_html=True)
            settings = [
                ("Rank By", metric_label),
                ("Max. Factors", p.n_factors),
                (("Max. " if analysis.params.high_quantile == 0 else "Min. ") + metric_label, rank_config.format_filter(p.min_rank_metric)),
                ("Max. Correlation", p.correlation_threshold),
                ("Max. NA", f"{p.max_na_pct}%"),
                ("Max. Return", f"{p.max_return_pct}%"),
                ("Benchmark", dataset_metadata.benchName),
                ("High Quantile (%)", f"{p.high_quantile}%"),
                ("Low Quantile (%)", f"{p.low_quantile}%"),
            ]
            items_html = "".join(render_info_item(label, value) for label, value in settings)
            st.html(f'<div style="display: flex; gap: 24px; flex-wrap: wrap;">{items_html}</div>')

        render_analysis_notes(analysis)

    with best_factors_tab:
        if best_feature_names:
            col1, col2 = st.columns([3, 2])
            with col1:
                st.caption(f"Best factors ranked by {metric_label} (highest first)")
            with col2:
                render_benchmark_badges(analysis.results.benchmark)

            render_results_table(
                all_metrics_df.lazy().filter(pl.col("column").is_in(best_feature_names)),
                quantile_renames=quantile_renames,
                key="best_factors",
                sortable=True,
            )

        else:
            st.info(
                "No factors met all the selection criteria. "
                "You can view all factors and their classifications in the **All Factors** tab."
            )

    with all_factors_tab:
        col1, col2 = st.columns([3, 2])
        with col1:
            st.caption(f"All factors ranked by {metric_label} (highest first). Click column headers to sort.")
        with col2:
            render_benchmark_badges(analysis.results.benchmark)

        render_results_table(
            all_metrics_df.lazy(),
            quantile_renames=quantile_renames,
            factor_classifications=factor_classifications,
            key="all_factors",
            sortable=True,
        )

    with correlations_tab:
        if not best_feature_names:
            st.info("No Best Factors were found.")
            return

        best_corr_matrix = (
            corr_matrix_df.lazy()
            .filter(pl.col("factor").is_in(best_feature_names))
            .select(["factor"] + best_feature_names)
            .sort(pl.col("factor").replace_strict(best_feature_names, range(len(best_feature_names))))
            .collect()
        )

        render_correlation_matrix(corr_matrix_df=best_corr_matrix, title="Correlation Matrix (Best Factors)", file_prefix=fl_id)

        st.subheader("Correlation Conflicts")

        render_conflict_explorer(
            corr_matrix_df=corr_matrix_df,
            conflicting_factors=[f for f, label in factor_classifications.items() if label == "correlation_conflict"],
            best_factors=best_feature_names,
            threshold=p.correlation_threshold,
        )
