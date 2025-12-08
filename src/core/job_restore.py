from pathlib import Path

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import deserialize_dataframe
from src.core.constants import JobStatus
from src.services.readers import ParquetDataReader
from src.workers.manager import read_job


def restore_running_job(job_id: str, params: dict, dataset_path: Path) -> None:
    """Restore UI state for PENDING/RUNNING jobs (step 2)."""
    add_debug_log(f"Found running job for {job_id}, restoring step 2 state")

    formulas_data = ParquetDataReader(dataset_path).get_formulas_from_metadata()

    state = get_state()
    state.completed_steps.add(1)
    update_state(
        current_job_id=job_id,
        current_step=2,
        benchmark_ticker=params.get('benchmark_ticker'),
        formulas_data=formulas_data,
    )


def restore_completed_job(job_id: str, job_data: dict, params: dict, dataset_path: Path) -> None:
    """Restore UI state for COMPLETED jobs (step 3)."""
    add_debug_log(f"Found completed job for {job_id}, loading results")

    try:
        results = job_data['results']
        metrics_df = deserialize_dataframe(results['all_metrics'])
        corr_matrix = deserialize_dataframe(results['all_corr_matrix'])

        formulas_data = ParquetDataReader(dataset_path).get_formulas_from_metadata()

        state = get_state()
        state.completed_steps.add(1)
        state.completed_steps.add(2)
        state.completed_steps.add(3)
        update_state(
            current_step=3,
            benchmark_ticker=params.get('benchmark_ticker'),
            formulas_data=formulas_data,
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            min_alpha=params.get('min_alpha', 0.5),
            top_x_pct=params.get('top_pct', 20.0),
            bottom_x_pct=params.get('bottom_pct', 20.0),
        )
    except Exception as e:
        add_debug_log(f"Error loading completed job results: {e}")


def restore_job_state(job_id: str, dataset_path: Path) -> bool:
    # check job state and dispatch to appropriate restore function
    job_data = read_job(job_id)
    if not job_data:
        return False

    params = job_data['params']
    status = job_data['status']

    if status in (JobStatus.PENDING, JobStatus.RUNNING):
        restore_running_job(job_id, params, dataset_path)
        return True
    elif status == JobStatus.COMPLETED:
        restore_completed_job(job_id, job_data, params, dataset_path)
        return True

    return False
