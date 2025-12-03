import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd

from src.core.utils import serialize_dataframe, deserialize_dataframe

# Jobs directory: project_root/data/jobs
# TODO: add env variable for this?
JOBS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "jobs"
JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _get_job_path(job_id: str) -> Path:
    return JOBS_DIR / f"{job_id}.json"


def create_job(job_id: str, params: Dict[str, Any]) -> Path:
    job_data = {
        "id": job_id,
        # pending for now, will be updated to running when worker starts
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "params": params,
        "results": None,
        "error": None
    }

    job_path = _get_job_path(job_id)
    with open(job_path, 'w') as f:
        json.dump(job_data, f, indent=2)

    return job_path


def read_job(job_id: str) -> Optional[Dict[str, Any]]:
    job_path = _get_job_path(job_id)

    if not job_path.exists():
        return None

    try:
        with open(job_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def update_job(
    job_id: str,
    status: str,
    results: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    progress: Optional[Dict[str, int]] = None
) -> bool:
    job_data = read_job(job_id)

    if job_data is None:
        return False

    job_data["status"] = status
    job_data["updated_at"] = datetime.now().isoformat()

    if results is not None:
        job_data["results"] = results

    if error is not None:
        job_data["error"] = error

    if progress is not None:
        job_data["progress"] = progress

    # Clear progress when job is finished (no longer needed)
    if status in ('completed', 'error'):
        job_data.pop("progress", None)

    job_path = _get_job_path(job_id)
    with open(job_path, 'w') as f:
        json.dump(job_data, f, indent=2)

    return True


def delete_job(job_id: str) -> bool:
    job_path = _get_job_path(job_id)

    if job_path.exists():
        # just remove file
        job_path.unlink()
        return True

    return False


def start_analysis_job(job_id: str, params: Dict[str, Any]) -> str:
    project_root = Path(__file__).resolve().parent.parent.parent

    create_job(job_id, params)

    # Spawn worker process
    subprocess.Popen(
        [sys.executable, '-m', 'src.workers.worker', job_id],
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return job_id


def get_job_results(job_data: Dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    results = job_data['results']
    metrics_df = deserialize_dataframe(results['all_metrics'])
    corr_matrix = deserialize_dataframe(results['all_corr_matrix'])
    return metrics_df, corr_matrix


