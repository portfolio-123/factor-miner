import json
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd
from dotenv import load_dotenv

from src.core.utils import deserialize_dataframe
from src.core.constants import JobStatus, JobProgress
from src.services.readers import ParquetDataReader

load_dotenv()

factor_list_dir_env = os.getenv('FACTOR_LIST_DIR')
if factor_list_dir_env:
    JOBS_DIR = Path(factor_list_dir_env) / "integrations"

JOBS_DIR.mkdir(parents=True, exist_ok=True)


def _get_job_path(job_id: str) -> Path:
    if not job_id.endswith('.json'):
        return JOBS_DIR / f"{job_id}.json"
    return JOBS_DIR / job_id


def ensure_formulas_backup(job_dir: Path, dataset_path: str) -> None:
    formulas_path = job_dir / "formulas.csv"
    if formulas_path.exists():
        return

    try:
        reader = ParquetDataReader(dataset_path)
        formulas_df = reader.get_formulas_from_metadata()
        if formulas_df is not None:
            formulas_df.to_csv(formulas_path, index=False)
    except Exception as e:
        print(f"Error backing up formulas: {e}")


def create_job(job_id: str, params: Dict[str, Any]) -> Path:
    job_data = {
        "id": job_id,
        # pending for now, will be updated to running when worker starts
        "status": JobStatus.PENDING,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "params": params,
        "results": None,
        "error": None
    }

    job_path = _get_job_path(job_id)
    
    # ensure directory for the dataset version exists
    job_path.parent.mkdir(parents=True, exist_ok=True)
    
    ensure_formulas_backup(job_path.parent, params['dataset_path'])

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
    status: JobStatus,
    results: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    progress: Optional[JobProgress] = None
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
    if status in (JobStatus.COMPLETED, JobStatus.ERROR):
        job_data.pop("progress", None)

    job_path = _get_job_path(job_id)
    with open(job_path, 'w') as f:
        json.dump(job_data, f, indent=2)

    return True


def append_job_log(job_id: str, message: str) -> None:
    job_data = read_job(job_id)
    if job_data is None:
        return

    if "logs" not in job_data:
        job_data["logs"] = []

    job_data["logs"].append(message)

    job_path = _get_job_path(job_id)
    with open(job_path, 'w') as f:
        json.dump(job_data, f, indent=2)


def delete_job(job_id: str) -> bool:
    job_path = _get_job_path(job_id)

    if job_path.exists():
        job_path.unlink()
        return True

    return False


def list_jobs(fl_id: str) -> List[Dict[str, Any]]:
    fl_dir = JOBS_DIR / fl_id
    if not fl_dir.exists():
        return []
    
    jobs = []
    for dataset_dir in fl_dir.iterdir():
        if not dataset_dir.is_dir():
            continue
            
        dataset_version = dataset_dir.name
        
        for job_file in dataset_dir.glob("*.json"):
            try:
                with open(job_file, 'r') as f:
                    job_data = json.load(f)
                
                jobs.append({
                    "id": job_data.get("id"),
                    "created_at": job_data.get("created_at"),
                    "status": job_data.get("status"),
                    "dataset_version": dataset_version,
                    "params": job_data.get("params"),
                })
            except Exception:
                continue
    
    # sort by creation. newest first
    jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return jobs


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
