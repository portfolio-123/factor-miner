import pandas as pd
from src.core.context import get_state, update_state, add_debug_log, sync_url_for_results
from src.core.utils import deserialize_dataframe
from src.core.constants import AnalysisStatus
from src.core.types import AnalysisParams
from src.core.environment import FACTORMINER_DIR
from src.workers.manager import read_analysis


def _load_formulas(analysis_id: str) -> pd.DataFrame:
    try:
        analysis_path_rel = analysis_id if analysis_id.endswith('.json') else f"{analysis_id}.json"
        analysis_full_path = FACTORMINER_DIR / analysis_path_rel

        metadata_path = analysis_full_path.parent / "dataset_metadata.parquet"

        if metadata_path.exists():
            add_debug_log("Loading formulas from dataset_metadata.parquet")
            return pd.read_parquet(metadata_path)
        else:
            add_debug_log(f"Dataset metadata not found at {metadata_path}")

    except Exception as e:
        add_debug_log(f"Warning: Could not read dataset metadata: {e}")

    return None


def restore_running_analysis(analysis_id: str, params: AnalysisParams) -> None:
    add_debug_log(f"Found running analysis for {analysis_id}, restoring to results page with progress")

    formulas_data = _load_formulas(analysis_id)

    state = get_state()
    state.config_completed = True
    update_state(
        page="results",
        current_analysis_id=analysis_id,
        benchmark_ticker=params.benchmark_ticker,
        formulas_data=formulas_data,
    )
    sync_url_for_results(analysis_id)


def restore_completed_analysis(analysis_id: str, analysis_data: dict, params: AnalysisParams) -> None:
    add_debug_log(f"Found completed analysis for {analysis_id}, loading results")

    try:
        results = analysis_data['results']
        metrics_df = deserialize_dataframe(results['all_metrics'])
        corr_matrix = deserialize_dataframe(results['all_corr_matrix'])

        formulas_data = _load_formulas(analysis_id)

        state = get_state()
        state.config_completed = True
        update_state(
            page="results",
            current_analysis_id=analysis_id,
            benchmark_ticker=params.benchmark_ticker,
            formulas_data=formulas_data,
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            min_alpha=params.min_alpha,
            top_x_pct=params.top_pct,
            bottom_x_pct=params.bottom_pct,
        )
        sync_url_for_results(analysis_id)
    except Exception as e:
        add_debug_log(f"Error loading completed analysis results: {e}")


def restore_analysis_state(analysis_id: str) -> bool:
    analysis_data = read_analysis(analysis_id)
    if not analysis_data:
        return False

    status = analysis_data['status']
    if status == AnalysisStatus.FAILED:
        return False

    params = AnalysisParams(**analysis_data['params'])

    if status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        restore_running_analysis(analysis_id, params)
    elif status == AnalysisStatus.COMPLETED:
        restore_completed_analysis(analysis_id, analysis_data, params)

    return True
