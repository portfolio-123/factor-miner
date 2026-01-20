import json
import logging
import subprocess
import sys
from pathlib import Path
from datetime import datetime
from typing import Any, List, Unpack
from pydantic import ValidationError

from src.core.environment import FACTORMINER_DIR
from src.core.context import get_state
from src.core.types import (
    Analysis,
    AnalysisSummary,
    AnalysisParams,
    AnalysisUpdates,
    AnalysisStatus,
)
from src.core.utils import read_json_file, read_analysis_json
from src.services.dataset_service import get_dataset_file_path
from src.services.writers import (
    update_parquet_metadata,
    update_active_dataset_metadata,
    backup_parquet_metadata,
)

logger = logging.getLogger(__name__)

FACTORMINER_DIR.mkdir(parents=True, exist_ok=True)


def _write_analysis(analysis_id: str, analysis_data: dict[str, Any]) -> None:
    with open(FACTORMINER_DIR / analysis_id, "w") as f:
        json.dump(analysis_data, f, indent=2)


def update_dataset_description(dataset_version: str, description: str) -> None:
    state = get_state()
    backup_path = get_dataset_file_path(state.factor_list_uid, dataset_version)

    if backup_path.exists():
        update_parquet_metadata(
            backup_path, b"datasetMetadata", {"description": description}
        )

    if state.is_viewing_live_dataset:
        update_active_dataset_metadata(
            Path(state.active_dataset_file),
            b"datasetMetadata",
            {"description": description},
        )


def create_analysis(analysis_id: str, params: AnalysisParams) -> None:
    analysis = Analysis(
        id=analysis_id,
        status=AnalysisStatus.PENDING,
        created_at=datetime.now().isoformat(),
        updated_at=datetime.now().isoformat(),
        params=params,
    )

    version_dir = FACTORMINER_DIR / Path(analysis_id).parent
    version_dir.mkdir(parents=True, exist_ok=True)

    backup_parquet_metadata(
        params.active_dataset_file, version_dir / "dataset_metadata.parquet"
    )

    try:
        with open(FACTORMINER_DIR / analysis_id, "w") as f:
            json.dump(analysis.model_dump(), f, indent=2)
    except (IOError, OSError) as e:
        logger.error(f"Failed to create analysis {analysis_id}: {e}")
        raise


def read_analysis(analysis_id: str) -> Analysis | None:
    return read_analysis_json(FACTORMINER_DIR / analysis_id)


def update_analysis(analysis_id: str, **updates: Unpack[AnalysisUpdates]) -> None:
    analysis = read_analysis(analysis_id)
    if not analysis:
        return

    analysis_data = analysis.model_dump()
    analysis_data["updated_at"] = datetime.now().isoformat()

    for key, value in updates.items():
        analysis_data[key] = value

    # clear progress when analysis is finished (no longer needed)
    if updates.get("status") in (AnalysisStatus.COMPLETED, AnalysisStatus.ERROR):
        analysis_data["progress"] = None

    _write_analysis(analysis_id, analysis_data)


def append_analysis_log(analysis_id: str, message: str) -> None:
    analysis = read_analysis(analysis_id)
    if not analysis:
        return

    analysis_data = analysis.model_dump()
    if analysis_data["logs"] is None:
        analysis_data["logs"] = []

    analysis_data["logs"].append(message)
    _write_analysis(analysis_id, analysis_data)


def clear_analysis_credentials(analysis_id: str) -> None:
    analysis = read_analysis(analysis_id)
    if not analysis:
        return

    analysis_data = analysis.model_dump()
    analysis_data["params"].pop("access_token", None)

    _write_analysis(analysis_id, analysis_data)


def list_analyses_for_version(fl_id: str, version: str) -> List[AnalysisSummary]:
    version_dir = Path(FACTORMINER_DIR / fl_id / version)
    analyses = []
    for json_file in version_dir.glob("*.json"):
        data = read_json_file(json_file)
        if data is None:
            continue
        try:
            data["dataset_version"] = version
            analyses.append(AnalysisSummary.model_validate(data))
        except ValidationError:
            continue
    return sorted(analyses, key=lambda a: a.created_at, reverse=True)


def start_analysis(analysis_id: str, params: AnalysisParams) -> None:
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
