import json
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from src.core.config.environment import FACTORMINER_DIR
from src.core.types.models import (
    Analysis,
    AnalysisParams,
    AnalysisSummary,
    AnalysisStatus,
)
from src.core.utils.common import read_json_file
from src.services.dataset_service import BackupDatasetService, DatasetService

logger = logging.getLogger(__name__)


class AnalysisService:
    def __init__(self):
        self.base_dir = FACTORMINER_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, fl_id: str, analysis_id: str) -> Path:
        return self.base_dir / fl_id / f"{analysis_id}.json"

    def _write(self, analysis: Analysis) -> None:
        path = self._get_path(analysis.fl_id, analysis.id)
        with open(path, "w") as f:
            json.dump(analysis.model_dump(), f, indent=2)

    def get(self, fl_id: str, analysis_id: str, retries: int = 2) -> Analysis | None:
        for attempt in range(retries + 1):
            data = read_json_file(self._get_path(fl_id, analysis_id))
            try:
                return Analysis.model_validate(data)
            except ValidationError:
                if attempt < retries:
                    time.sleep(0.1) # to avoid race conditions in the progress functions which triggers this every second
                    continue
                return None

    def create(
        self,
        fl_id: str,
        analysis_id: str,
        dataset_version: str,
        params: AnalysisParams,
    ) -> Analysis:
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
        fl_dir = self.base_dir / fl_id
        fl_dir.mkdir(parents=True, exist_ok=True)

        # Create backup of dataset metadata if it doesn't exist
        dest_path = BackupDatasetService(fl_id).get_backup_path(dataset_version)
        if not dest_path.exists():
            with DatasetService(fl_id) as dataset_svc:
                dataset_svc.backup_metadata(dest_path)

        try:
            self._write(analysis)
        except (IOError, OSError) as e:
            logger.error(f"Failed to create analysis {fl_id}/{analysis_id}: {e}")
            raise

        return analysis

    def save(self, analysis: Analysis, **updates: Any) -> Analysis:
        update_dict: dict[str, Any] = {"updated_at": datetime.now().isoformat()}
        update_dict.update(updates)

        status = updates.get("status")
        if status == AnalysisStatus.RUNNING and analysis.started_at is None:
            update_dict["started_at"] = datetime.now().isoformat()
        if status in (AnalysisStatus.SUCCESS, AnalysisStatus.FAILED):
            update_dict["finished_at"] = datetime.now().isoformat()
            update_dict["progress"] = None

        updated = analysis.model_copy(update=update_dict)
        self._write(updated)
        return updated

    def append_log(self, analysis: Analysis, message: str) -> Analysis:
        logs = list(analysis.logs or [])
        logs.append(message)
        return self.save(analysis, logs=logs)

    def clear_credentials(self, analysis: Analysis) -> Analysis:
        updated_params = analysis.params.model_copy(update={"access_token": None})
        return self.save(analysis, params=updated_params)

    def list_all(self, fl_id: str) -> list[AnalysisSummary]:
        fl_dir = self.base_dir / fl_id
        if not fl_dir.exists():
            return []

        analyses = []
        for json_file in fl_dir.glob("*.json"):
            data = read_json_file(json_file)
            try:
                analyses.append(AnalysisSummary.model_validate(data))
            except ValidationError:
                continue

        return sorted(analyses, key=lambda a: a.created_at, reverse=True)

    def start(
        self,
        fl_id: str,
        analysis_id: str,
        dataset_version: str,
        params: AnalysisParams,
    ) -> Analysis:
        analysis = self.create(fl_id, analysis_id, dataset_version, params)

        project_root = Path(__file__).resolve().parent.parent.parent
        subprocess.Popen(
            [sys.executable, "-m", "src.workers.worker", fl_id, analysis_id],
            cwd=str(project_root),
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        return analysis


analysis_service = AnalysisService()
