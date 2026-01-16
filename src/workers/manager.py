import json
import logging
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
import pandas as pd
from dotenv import load_dotenv
from pydantic import ValidationError

from src.core.context import get_state
from src.core.types import AnalysisSummary
from src.core.utils import deserialize_dataframe
from src.core.constants import AnalysisStatus, AnalysisProgress
from src.services.parquet_utils import (
    get_file_version,
    get_dataset_file_path,
)
from src.services.writers import (
    update_parquet_metadata,
    update_parquet_metadata_preserve_mtime,
    backup_parquet_metadata,
)

load_dotenv()

logger = logging.getLogger(__name__)

FACTOR_LIST_DIR = Path(os.getenv("FACTOR_LIST_DIR"))

INTEGRATIONS_DIR = FACTOR_LIST_DIR / "factor-eval"
INTEGRATIONS_DIR.mkdir(parents=True, exist_ok=True)


def _get_analysis_path(analysis_id: str) -> Path:
    if not analysis_id.endswith(".json"):
        return INTEGRATIONS_DIR / f"{analysis_id}.json"
    return INTEGRATIONS_DIR / analysis_id


def _write_analysis(analysis_id: str, analysis_data: dict) -> None:
    analysis_path = _get_analysis_path(analysis_id)
    with open(analysis_path, "w") as f:
        json.dump(analysis_data, f, indent=2)


def update_dataset_info(dataset_version: str, updates: Dict[str, Any]) -> bool:
    try:
        state = get_state()
        dataset_path = state.dataset_path
        current_version = get_file_version(dataset_path) if dataset_path else None
        backup_path = get_dataset_file_path(state.factor_list_uid, dataset_version)

        if backup_path.exists():
            update_parquet_metadata(backup_path, b"datasetMetadata", updates)

        if dataset_version == current_version:
            update_parquet_metadata_preserve_mtime(Path(dataset_path), b"datasetMetadata", updates)

        return True
    except Exception:
        return False


def create_analysis(analysis_id: str, params: Dict[str, Any]) -> Path:
    analysis_data = {
        "id": analysis_id,
        # pending for now, will be updated to running when worker starts
        "status": AnalysisStatus.PENDING,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "params": params,
        "results": None,
        "error": None,
    }

    analysis_path = _get_analysis_path(analysis_id)

    # ensure directory for the dataset version exists
    analysis_path.parent.mkdir(parents=True, exist_ok=True)

    backup_parquet_metadata(params["dataset_path"], analysis_path.parent / "dataset_metadata.parquet")

    with open(analysis_path, "w") as f:
        json.dump(analysis_data, f, indent=2)

    return analysis_path


def read_analysis(analysis_id: str) -> Optional[Dict[str, Any]]:
    analysis_path = _get_analysis_path(analysis_id)

    if not analysis_path.exists():
        return None

    try:
        with open(analysis_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def update_analysis(
    analysis_id: str,
    status: AnalysisStatus,
    results: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    progress: Optional[AnalysisProgress] = None,
) -> bool:
    analysis_data = read_analysis(analysis_id)

    if analysis_data is None:
        return False

    analysis_data["status"] = status
    analysis_data["updated_at"] = datetime.now().isoformat()

    if results is not None:
        analysis_data["results"] = results

    if error is not None:
        analysis_data["error"] = error

    if progress is not None:
        analysis_data["progress"] = progress

    # Clear progress when analysis is finished (no longer needed)
    if status in (AnalysisStatus.COMPLETED, AnalysisStatus.ERROR):
        analysis_data.pop("progress", None)

    _write_analysis(analysis_id, analysis_data)
    return True


def append_analysis_log(analysis_id: str, message: str) -> None:
    analysis_data = read_analysis(analysis_id)
    if analysis_data is None:
        return

    if "logs" not in analysis_data:
        analysis_data["logs"] = []

    analysis_data["logs"].append(message)
    _write_analysis(analysis_id, analysis_data)


def delete_analysis(analysis_id: str) -> bool:
    analysis_path = _get_analysis_path(analysis_id)

    if analysis_path.exists():
        analysis_path.unlink()
        return True

    return False


def clear_analysis_credentials(analysis_id: str) -> bool:
    analysis_data = read_analysis(analysis_id)
    if analysis_data is None:
        return False

    params = analysis_data.get("params", {})
    params.pop("access_token", None)
    analysis_data["params"] = params

    _write_analysis(analysis_id, analysis_data)
    return True


def list_analyses(fl_id: str) -> List[AnalysisSummary]:
    fl_dir = INTEGRATIONS_DIR / fl_id
    if not fl_dir.exists():
        return []

    analyses = []
    for dataset_dir in fl_dir.iterdir():
        if not dataset_dir.is_dir():
            continue

        for analysis_file in dataset_dir.glob("*.json"):
            try:
                with open(analysis_file, "r") as f:
                    data = json.load(f)

                data["dataset_version"] = dataset_dir.name
                analyses.append(AnalysisSummary.model_validate(data))

            except (json.JSONDecodeError, IOError, ValidationError) as e:
                logger.warning(f"Failed to load analysis {analysis_file}: {e}")

    analyses.sort(key=lambda x: x.created_at, reverse=True)
    return analyses


def start_analysis(analysis_id: str, params: Dict[str, Any]) -> str:
    project_root = Path(__file__).resolve().parent.parent.parent

    create_analysis(analysis_id, params)

    # Spawn worker process
    subprocess.Popen(
        [sys.executable, "-m", "src.workers.worker", analysis_id],
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return analysis_id


def get_analysis_results(analysis_data: Dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    results = analysis_data["results"]
    metrics_df = deserialize_dataframe(results["all_metrics"])
    corr_matrix = deserialize_dataframe(results["all_corr_matrix"])
    return metrics_df, corr_matrix


def update_analysis_name(analysis_id: str, name: str) -> bool:
    analysis_data = read_analysis(analysis_id)
    if analysis_data is None:
        return False

    analysis_data["name"] = name
    analysis_data["updated_at"] = datetime.now().isoformat()

    _write_analysis(analysis_id, analysis_data)
    return True


def get_analysis_name(analysis_id: str) -> Optional[str]:
    analysis_data = read_analysis(analysis_id)
    if analysis_data is None:
        return None
    return analysis_data.get("name")


