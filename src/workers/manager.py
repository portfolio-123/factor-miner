import json
import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from typing import Any, List, Unpack
from pydantic import ValidationError

from src.core.environment import FACTORMINER_DIR
from src.core.types import (
    Analysis,
    AnalysisSummary,
    AnalysisParams,
    AnalysisStatus,
    AnalysisUpdates,
)
from src.core.utils import read_json_file, read_analysis_json
from src.services.dataset_service import get_dataset_file_path
from src.services.writers import backup_parquet_metadata

logger = logging.getLogger(__name__)

FACTORMINER_DIR.mkdir(parents=True, exist_ok=True)


def _get_analysis_path(fl_id: str, analysis_id: str) -> Path:
    return FACTORMINER_DIR / fl_id / f"{analysis_id}.json"


def _write_analysis(fl_id: str, analysis_id: str, analysis_data: dict[str, Any]) -> None:
    path = _get_analysis_path(fl_id, analysis_id)
    with open(path, "w") as f:
        json.dump(analysis_data, f, indent=2)


def create_analysis(
    fl_id: str, analysis_id: str, dataset_version: str, params: AnalysisParams
) -> None:
    analysis = Analysis(
        id=analysis_id,
        fl_id=fl_id,
        dataset_version=dataset_version,
        status=AnalysisStatus.PENDING,
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat(),
        params=params,
    )

    # Ensure fl_id directory exists
    fl_dir = FACTORMINER_DIR / fl_id
    fl_dir.mkdir(parents=True, exist_ok=True)

    # Create backup of dataset metadata if it doesn't exist
    dest_path = get_dataset_file_path(fl_id, dataset_version)
    if not dest_path.exists():
        backup_parquet_metadata(fl_id, dest_path)

    try:
        _write_analysis(fl_id, analysis_id, analysis.model_dump())
    except (IOError, OSError) as e:
        logger.error(f"Failed to create analysis {fl_id}/{analysis_id}: {e}")
        raise


def read_analysis(fl_id: str, analysis_id: str) -> Analysis | None:
    return read_analysis_json(_get_analysis_path(fl_id, analysis_id))


def update_analysis(
    fl_id: str, analysis_id: str, **updates: Unpack[AnalysisUpdates]
) -> None:
    analysis = read_analysis(fl_id, analysis_id)
    if not analysis:
        return

    analysis_data = analysis.model_dump()
    analysis_data["updated_at"] = datetime.now().isoformat()

    for key, value in updates.items():
        analysis_data[key] = value

    # clear progress when analysis is finished (no longer needed)
    if updates.get("status") in (AnalysisStatus.SUCCESS, AnalysisStatus.FAILED):
        analysis_data["progress"] = None

    _write_analysis(fl_id, analysis_id, analysis_data)


def append_analysis_log(fl_id: str, analysis_id: str, message: str) -> None:
    analysis = read_analysis(fl_id, analysis_id)
    if not analysis:
        return

    analysis_data = analysis.model_dump()
    if analysis_data["logs"] is None:
        analysis_data["logs"] = []

    analysis_data["logs"].append(message)
    _write_analysis(fl_id, analysis_id, analysis_data)


def clear_analysis_credentials(fl_id: str, analysis_id: str) -> None:
    analysis = read_analysis(fl_id, analysis_id)
    if not analysis:
        return

    analysis_data = analysis.model_dump()
    analysis_data["params"].pop("access_token", None)

    _write_analysis(fl_id, analysis_id, analysis_data)


def list_all_analyses(fl_id: str) -> List[AnalysisSummary]:
    fl_dir = FACTORMINER_DIR / fl_id
    if not fl_dir.exists():
        return []

    analyses = []
    for json_file in fl_dir.glob("*.json"):
        data = read_json_file(json_file)
        if data is None:
            continue
        try:
            analyses.append(AnalysisSummary.model_validate(data))
        except ValidationError:
            continue
    return sorted(analyses, key=lambda a: a.created_at, reverse=True)


def start_analysis(
    fl_id: str, analysis_id: str, dataset_version: str, params: AnalysisParams
) -> None:
    project_root = Path(__file__).resolve().parent.parent.parent

    create_analysis(fl_id, analysis_id, dataset_version, params)

    # Spawn worker process
    subprocess.Popen(
        [sys.executable, "-m", "src.workers.worker", fl_id, analysis_id],
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
