import os
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import pandas as pd


def get_jobs_dir() -> Path:
    """Get the jobs directory from environment variable."""
    jobs_dir = os.environ.get('JOBS_DIR', './data/jobs')
    path = Path(jobs_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _get_job_path(job_id: str) -> Path:
    """Get the path to a job file."""
    return get_jobs_dir() / f"{job_id}.json"


def create_job(job_id: str, params: Dict[str, Any]) -> Path:
    """Create a new job file with pending status."""
    job_data = {
        "id": job_id,
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
    """Read a job file and return its data."""
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
    error: Optional[str] = None
) -> bool:
    """Update a job file with new status and optionally results or error."""
    job_data = read_job(job_id)

    if job_data is None:
        return False

    job_data["status"] = status
    job_data["updated_at"] = datetime.now().isoformat()

    if results is not None:
        job_data["results"] = results

    if error is not None:
        job_data["error"] = error

    job_path = _get_job_path(job_id)
    with open(job_path, 'w') as f:
        json.dump(job_data, f, indent=2)

    return True


def delete_job(job_id: str) -> bool:
    """Delete a job file."""
    job_path = _get_job_path(job_id)

    if job_path.exists():
        job_path.unlink()
        return True

    return False


def serialize_dataframe(df: pd.DataFrame) -> str:
    """Serialize a DataFrame to JSON string for storage."""
    return df.to_json(orient='split', date_format='iso')


def deserialize_dataframe(json_str: str) -> pd.DataFrame:
    """Deserialize a JSON string back to DataFrame."""
    return pd.read_json(json_str, orient='split')
