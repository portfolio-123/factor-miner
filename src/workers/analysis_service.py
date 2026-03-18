import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.config.environment import DATASET_DIR, INTERNAL_MODE
from src.core.types.models import (
    Analysis,
    AnalysisParams,
    AnalysisSummary,
    AnalysisStatus,
)
from src.services.dataset_service import BackupDatasetService, DatasetService

logger = logging.getLogger(__name__)


class AnalysisService:
    def __init__(self, user_uid: str | None = None):
        self.user_uid = user_uid
        if INTERNAL_MODE and user_uid:
            self.base_dir = DATASET_DIR / user_uid / "FactorMiner"
        else:
            self.base_dir = DATASET_DIR / "FactorMiner"

    def _get_path(self, fl_id: str, analysis_id: str) -> Path:
        return self.base_dir / fl_id / f"{analysis_id}.json"

    def _write(self, analysis: Analysis) -> None:
        path = self._get_path(analysis.fl_id, analysis.id)
        with open(path, "w") as f:
            json.dump(analysis.model_dump(), f, indent=2)

    def get(self, fl_id: str, analysis_id: str, retries=2) -> Analysis | None:
        path = self._get_path(fl_id, analysis_id)
        tries = 0
        while True:
            with open(path, "r") as f:
                try:
                    data = json.load(f)
                except (FileNotFoundError, IOError):
                    return None
                except json.JSONDecodeError:
                    if (tries := tries + 1) < retries:
                        # to avoid race conditions in the progress functions which triggers this every second
                        time.sleep(0.1)
                        continue
                    return None

                return Analysis.model_validate(data)

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
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            params=params,
        )

        # Ensure fl_id directory exists
        fl_dir = self.base_dir / fl_id
        fl_dir.mkdir(parents=True, exist_ok=True)

        # Create backup of dataset metadata if it doesn't exist
        dest_path = BackupDatasetService(self.user_uid, fl_id).get_backup_path(
            dataset_version
        )
        if not dest_path.exists():
            with DatasetService(fl_id, self.user_uid) as dataset_svc:
                dataset_svc.backup_metadata(dest_path)

        try:
            self._write(analysis)
        except (IOError, OSError) as e:
            logger.error(f"Failed to create analysis {fl_id}/{analysis_id}: {e}")
            raise

        return analysis

    def save(self, analysis: Analysis, **updates: Any) -> Analysis:
        update_dict: dict[str, Any] = {
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        update_dict.update(updates)

        status = updates.get("status")
        if status == AnalysisStatus.RUNNING and analysis.started_at is None:
            update_dict["started_at"] = datetime.now(timezone.utc).isoformat()
        if status in (AnalysisStatus.SUCCESS, AnalysisStatus.FAILED):
            update_dict["finished_at"] = datetime.now(timezone.utc).isoformat()
            update_dict["progress"] = None

        updated = analysis.model_copy(update=update_dict)
        self._write(updated)
        return updated

    def get_logs(self, fl_id: str, analysis_id: str) -> list[str]:
        """Read logs from the stderr.log file for an analysis."""
        log_path = self.base_dir / fl_id / "logs" / f"{analysis_id}.stderr.log"
        if not log_path.exists():
            return []
        try:
            content = log_path.read_text()
            return content.splitlines()
        except Exception:
            return []

    def list_all(self, fl_id: str) -> list[AnalysisSummary]:
        fl_dir = self.base_dir / fl_id
        if not fl_dir.exists():
            return []

        analyses = []
        for json_file in fl_dir.glob("*.json"):
            if json_file.stem.startswith("dataset_"):
                continue
            with open(json_file, "r") as f:
                try:
                    data = json.load(f)
                    analyses.append(AnalysisSummary.model_validate(data))
                except Exception:
                    continue

        return sorted(analyses, key=lambda a: a.created_at, reverse=True)

    def next_analysis_id(self, fl_id: str) -> str:
        fl_dir = self.base_dir / fl_id
        if not fl_dir.exists():
            return "analysis_1"

        max_num = 0
        for json_file in fl_dir.glob("analysis_*.json"):
            try:
                num = int(json_file.stem.split("_")[1])
                max_num = max(max_num, num)
            except (IndexError, ValueError):
                continue

        return f"analysis_{max_num + 1}"

    def start(
        self,
        fl_id: str,
        analysis_id: str,
        dataset_version: str,
        params: AnalysisParams,
        access_token: str | None = None,
    ) -> Analysis:
        analysis = self.create(fl_id, analysis_id, dataset_version, params)

        project_root = Path(__file__).resolve().parent.parent.parent
        log_dir = self.base_dir / fl_id / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stderr_path = log_dir / f"{analysis_id}.stderr.log"

        stderr_file = open(stderr_path, "w")
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "src.workers.worker",
                fl_id,
                analysis_id,
                self.user_uid or "",
                access_token or "",
            ],
            cwd=str(project_root),
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=stderr_file,
        )

        return analysis
