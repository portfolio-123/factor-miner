import pandas as pd
from src.core.context import get_state, update_state, add_debug_log, sync_url_for_results
from src.core.utils import deserialize_dataframe
from src.core.constants import JobStatus
from src.core.types import AnalysisParams
from src.workers.manager import read_job, INTEGRATIONS_DIR


def _load_formulas(job_id: str) -> pd.DataFrame:
    try:
        job_path_rel = job_id if job_id.endswith('.json') else f"{job_id}.json"
        job_full_path = INTEGRATIONS_DIR / job_path_rel
        
        metadata_path = job_full_path.parent / "dataset_metadata.parquet"
        
        if metadata_path.exists():
            add_debug_log("Loading formulas from dataset_metadata.parquet")
            return pd.read_parquet(metadata_path)
        else:
            add_debug_log(f"Dataset metadata not found at {metadata_path}")
            
    except Exception as e:
        add_debug_log(f"Warning: Could not read dataset metadata: {e}")

    return None


def restore_running_job(job_id: str, params: AnalysisParams) -> None:
    add_debug_log(f"Found running job for {job_id}, restoring to results page with progress")

    formulas_data = _load_formulas(job_id)

    state = get_state()
    state.config_completed = True
    update_state(
        page="results",
        current_job_id=job_id,
        benchmark_ticker=params.benchmark_ticker,
        formulas_data=formulas_data,
    )
    sync_url_for_results(job_id)


def restore_completed_job(job_id: str, job_data: dict, params: AnalysisParams) -> None:
    add_debug_log(f"Found completed job for {job_id}, loading results")

    try:
        results = job_data['results']
        metrics_df = deserialize_dataframe(results['all_metrics'])
        corr_matrix = deserialize_dataframe(results['all_corr_matrix'])

        formulas_data = _load_formulas(job_id)

        state = get_state()
        state.config_completed = True
        update_state(
            page="results",
            current_job_id=job_id,
            benchmark_ticker=params.benchmark_ticker,
            formulas_data=formulas_data,
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            min_alpha=params.min_alpha,
            top_x_pct=params.top_pct,
            bottom_x_pct=params.bottom_pct,
        )
        sync_url_for_results(job_id)
    except Exception as e:
        add_debug_log(f"Error loading completed job results: {e}")


def restore_job_state(job_id: str) -> bool:
    job_data = read_job(job_id)
    # if there's no job, nothing to restore
    if not job_data:
        return False

    params = AnalysisParams(**job_data['params'])
    status = job_data['status']

    if status in (JobStatus.PENDING, JobStatus.RUNNING):
        restore_running_job(job_id, params)
        return True
    elif status == JobStatus.COMPLETED:
        restore_completed_job(job_id, job_data, params)
        return True

    return False
