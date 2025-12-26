import json
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from src.core.context import get_state
from src.core.utils import deserialize_dataframe
from src.core.constants import JobStatus, JobProgress
from src.services.readers import ParquetDataReader
from src.core.types import Job, DatasetConfig

load_dotenv()

FACTOR_LIST_DIR = Path(os.getenv('FACTOR_LIST_DIR'))

INTEGRATIONS_DIR = FACTOR_LIST_DIR / "factor-eval"
INTEGRATIONS_DIR.mkdir(parents=True, exist_ok=True)


def _get_job_path(job_id: str) -> Path:
    if not job_id.endswith('.json'):
        return INTEGRATIONS_DIR / f"{job_id}.json"
    return INTEGRATIONS_DIR / job_id


def ensure_dataset_metadata(job_dir: Path, dataset_path: str) -> None:
    metadata_path = job_dir / "dataset_metadata.parquet"

    if metadata_path.exists():
        return

    try:
        reader = ParquetDataReader(dataset_path)
        formulas_df = reader.get_formulas_df()
        dataset_info = reader.get_dataset_info()

        if formulas_df is None:
            formulas_df = pd.DataFrame(columns=["formula", "name", "tag", "Normalization"])

        table = pa.Table.from_pandas(formulas_df)
        
        if dataset_info:
            existing_meta = table.schema.metadata
            new_meta = existing_meta.copy() if existing_meta else {}
            new_meta[b'dataset'] = json.dumps(dataset_info).encode('utf-8')
            
            if formulas_df is not None:
                 new_meta[b'features'] = json.dumps(formulas_df.to_dict(orient='records')).encode('utf-8')

            table = table.replace_schema_metadata(new_meta)
            
        pq.write_table(table, metadata_path)

    except Exception as e:
        print(f"Error backing up dataset artifacts: {e}")


def get_dataset_backup_path(fl_id: str, dataset_version: str) -> Optional[str]:
    path = INTEGRATIONS_DIR / fl_id / dataset_version / "dataset_metadata.parquet"
    if path.exists():
        return str(path)
    return None


def get_dataset_info_from_backup(fl_id: str, dataset_version: str) -> Optional[DatasetConfig]:
    path = INTEGRATIONS_DIR / fl_id / dataset_version / "dataset_metadata.parquet"
    if not path.exists():
        return None
    try:
        return ParquetDataReader(str(path)).get_dataset_info()
    except Exception:
        return None


def get_formulas_df_for_version(fl_id: str, ds_ver: str) -> Optional[pd.DataFrame]:
    try:
        state = get_state()
        dataset_path = None
        
        # check current dataset first and if we're trying to view the current version, use it
        if state.dataset_path and os.path.exists(state.dataset_path):
            ts = os.path.getmtime(state.dataset_path)
            current_ver = str(int(ts))
            if current_ver == ds_ver:
                dataset_path = state.dataset_path
                
        # fallback to backup if not found as current
        if not dataset_path:
            dataset_path = get_dataset_backup_path(fl_id, ds_ver)

        if dataset_path and os.path.exists(dataset_path):
            reader = ParquetDataReader(dataset_path)
            return reader.get_formulas_df()
            
    except Exception:
        pass
        
    return None


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
    
    ensure_dataset_metadata(job_path.parent, params['dataset_path'])

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
                with open(job_file, 'r') as f:
                    job_data = json.load(f)
                
                jobs.append({
                    "id": job_data.get("id"),
                    "name": job_data.get("name"),
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


def update_job_name(job_id: str, name: str) -> bool:
    job_data = read_job(job_id)
    if job_data is None:
        return False

    job_data["name"] = name
    job_data["updated_at"] = datetime.now().isoformat()

    job_path = _get_job_path(job_id)
    with open(job_path, 'w') as f:
        json.dump(job_data, f, indent=2)
    return True


def get_job_name(job_id: str) -> Optional[str]:
    job_data = read_job(job_id)
    if job_data is None:
        return None
    return job_data.get("name")


def get_grouped_jobs(fl_id: str) -> Tuple[List[Job], Dict[str, List[Job]]]:
    jobs_data = list_jobs(fl_id)
    jobs = [Job(**j) for j in jobs_data]
    
    grouped_jobs = defaultdict(list)
    for job in jobs:
        ds_ver = job.dataset_version
        if ds_ver:
            grouped_jobs[ds_ver].append(job)
            
    return jobs, grouped_jobs


def sort_dataset_versions(versions: List[str]) -> List[str]:
    return sorted(
        versions,
        key=lambda x: float(x) if x.replace(".", "", 1).isdigit() else 0,
        reverse=True,
    )
