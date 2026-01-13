import json
import logging
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import defaultdict
import pandas as pd
from dotenv import load_dotenv

from src.core.context import get_state
from src.core.utils import deserialize_dataframe
from src.core.constants import JobStatus, JobProgress
from src.services.parquet_utils import (
    get_file_version,
    get_dataset_file_path,
)
from src.services.writers import (
    update_parquet_metadata,
    update_parquet_metadata_preserve_mtime,
    backup_parquet_metadata,
)
from src.core.types import Job

load_dotenv()

logger = logging.getLogger(__name__)

FACTOR_LIST_DIR = Path(os.getenv("FACTOR_LIST_DIR"))

INTEGRATIONS_DIR = FACTOR_LIST_DIR / "factor-eval"
INTEGRATIONS_DIR.mkdir(parents=True, exist_ok=True)


def _get_job_path(job_id: str) -> Path:
    if not job_id.endswith(".json"):
        return INTEGRATIONS_DIR / f"{job_id}.json"
    return INTEGRATIONS_DIR / job_id


def _write_job(job_id: str, job_data: dict) -> None:
    job_path = _get_job_path(job_id)
    with open(job_path, "w") as f:
        json.dump(job_data, f, indent=2)


def update_dataset_info(
    dataset_path: str, dataset_version: str, updates: Dict[str, Any]
) -> bool:

    try:
        state = get_state()
        current_version = get_file_version(dataset_path)
        backup_path = get_dataset_file_path(state.factor_list_uid, dataset_version)

        if backup_path.exists():
            update_parquet_metadata(backup_path, b"dataset", updates)

        if dataset_version == current_version:
            update_parquet_metadata_preserve_mtime(Path(dataset_path), b"dataset", updates)

        return True
    except Exception:
        return False


def create_job(job_id: str, params: Dict[str, Any]) -> Path:
    job_data = {
        "id": job_id,
        # pending for now, will be updated to running when worker starts
        "status": JobStatus.PENDING,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "params": params,
        "results": None,
        "error": None,
    }

    job_path = _get_job_path(job_id)

    # ensure directory for the dataset version exists
    job_path.parent.mkdir(parents=True, exist_ok=True)

    backup_parquet_metadata(params["dataset_path"], job_path.parent / "dataset_metadata.parquet")

    with open(job_path, "w") as f:
        json.dump(job_data, f, indent=2)

    return job_path


def read_job(job_id: str) -> Optional[Dict[str, Any]]:
    job_path = _get_job_path(job_id)

    if not job_path.exists():
        return None

    try:
        with open(job_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def update_job(
    job_id: str,
    status: JobStatus,
    results: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    progress: Optional[JobProgress] = None,
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

    _write_job(job_id, job_data)
    return True


def append_job_log(job_id: str, message: str) -> None:
    job_data = read_job(job_id)
    if job_data is None:
        return

    if "logs" not in job_data:
        job_data["logs"] = []

    job_data["logs"].append(message)
    _write_job(job_id, job_data)


def delete_job(job_id: str) -> bool:
    job_path = _get_job_path(job_id)

    if job_path.exists():
        job_path.unlink()
        return True

    return False


def clear_job_credentials(job_id: str) -> bool:
    job_data = read_job(job_id)
    if job_data is None:
        return False

    params = job_data.get("params", {})
    params.pop("api_key", None)
    params.pop("api_id", None)
    job_data["params"] = params

    _write_job(job_id, job_data)
    return True


def list_jobs(fl_id: str) -> List[Dict[str, Any]]:
    fl_dir = INTEGRATIONS_DIR / fl_id
    if not fl_dir.exists():
        return []

    jobs = []
    for dataset_dir in fl_dir.iterdir():
        if not dataset_dir.is_dir():
            continue

        dataset_version = dataset_dir.name

        for job_file in dataset_dir.glob("*.json"):
            try:
                with open(job_file, "r") as f:
                    job_data = json.load(f)

                jobs.append(
                    {
                        "id": job_data.get("id"),
                        "name": job_data.get("name"),
                        "created_at": job_data.get("created_at"),
                        "status": job_data.get("status"),
                        "dataset_version": dataset_version,
                        "params": job_data.get("params"),
                    }
                )
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
        [sys.executable, "-m", "src.workers.worker", job_id],
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return job_id


def get_job_results(job_data: Dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    results = job_data["results"]
    metrics_df = deserialize_dataframe(results["all_metrics"])
    corr_matrix = deserialize_dataframe(results["all_corr_matrix"])
    return metrics_df, corr_matrix


def update_job_name(job_id: str, name: str) -> bool:
    job_data = read_job(job_id)
    if job_data is None:
        return False

    job_data["name"] = name
    job_data["updated_at"] = datetime.now().isoformat()

    _write_job(job_id, job_data)
    return True


def get_job_name(job_id: str) -> Optional[str]:
    job_data = read_job(job_id)
    if job_data is None:
        return None
    return job_data.get("name")


def get_grouped_jobs(fl_id: str) -> dict[str, list[Job]]:
    jobs_data = list_jobs(fl_id)

    grouped_jobs = defaultdict(list)
    for job in jobs_data:
        ds_ver = job.get("dataset_version")
        if ds_ver:
            try:
                grouped_jobs[ds_ver].append(Job(**job))
            except Exception:
                logger.warning(f"Failed to parse job: {job.get('id')}")
                continue

    return grouped_jobs


def sort_dataset_versions(versions: List[str]) -> List[str]:
    return sorted(
        versions,
        key=lambda x: float(x) if x.replace(".", "", 1).isdigit() else 0,
        reverse=True,
    )
