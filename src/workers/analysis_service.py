import json
import logging
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from core.utils.common import find_files
from src.core.config.environment import DATASET_DIR, INTERNAL_MODE
from src.core.types.models import (
    Analysis,
    AnalysisParams,
    AnalysisSummary,
    AnalysisStatus,
    AnalysisUpdate,
)
from src.services.dataset_service import BackupDatasetService, DatasetService

logger = logging.getLogger(__name__)


class AnalysisService:
    def __init__(self, user_uid: str | None = None):
        self.user_uid = user_uid
        if INTERNAL_MODE and user_uid:
            self.base_dir = Path(DATASET_DIR, user_uid, "FactorMiner")
        else:
            self.base_dir = Path(DATASET_DIR, "FactorMiner")

    def _get_path(self, fl_id: str, analysis_id: str):
        return Path(self.base_dir, fl_id, f"{analysis_id}.json")

    def _write(self, analysis: Analysis):
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
        self, fl_id: str, analysis_id: str, dataset_version: str, params: AnalysisParams
    ) -> Analysis:
        now = datetime.now(timezone.utc).isoformat()
        analysis = Analysis(
            id=analysis_id,
            fl_id=fl_id,
            dataset_version=dataset_version,
            status=AnalysisStatus.PENDING,
            created_at=now,
            updated_at=now,
            params=params,
        )

        # Ensure fl_id directory exists
        fl_dir = Path(self.base_dir, fl_id)
        fl_dir.mkdir(parents=True, exist_ok=True)

        # Create backup of dataset metadata if it doesn't exist
        dest_path = BackupDatasetService(self.user_uid, fl_id).get_backup_path(
            dataset_version
        )
        if not dest_path.exists():
            with DatasetService(fl_id, self.user_uid) as dataset_svc:
                dataset_svc.back_up_metadata(dest_path)

        try:
            self._write(analysis)
        except (IOError, OSError) as e:
            logger.error(f"Failed to create analysis {fl_id}/{analysis_id}: {e}")
            raise

        return analysis

    def save(self, analysis: Analysis, updates: AnalysisUpdate) -> Analysis:
        now = datetime.now(timezone.utc).isoformat()
        updates["updated_at"] = now

        status = updates.get("status")
        if status is not None:
            if status == AnalysisStatus.RUNNING and analysis.started_at is None:
                updates["started_at"] = now
            if status in (AnalysisStatus.SUCCESS, AnalysisStatus.FAILED):
                updates["finished_at"] = now
                updates["progress"] = None

        updated = analysis.model_copy(update=updates)
        self._write(updated)
        return updated

    def get_logs(self, fl_id: str, analysis_id: str) -> list[str]:
        """Read logs from the stderr.log file for an analysis."""
        log_path = Path(self.base_dir, fl_id, "logs", f"{analysis_id}.stderr.log")
        try:
            content = log_path.read_text()
            return content.splitlines()
        except Exception:
            return []

    def list_all(self, fl_id: str) -> list[AnalysisSummary]:
        fl_dir = Path(self.base_dir, fl_id)

        analyses: list[AnalysisSummary] = []
        for json_file in find_files(fl_dir, prefix="analysis_", suffix=".json"):
            with open(json_file.path, "r") as f:
                try:
                    data = json.load(f)
                    analyses.append(AnalysisSummary.model_validate(data))
                except Exception:
                    continue

        if analyses:
            analyses = sorted(analyses, key=lambda a: a.created_at, reverse=True)

        return analyses

    def next_analysis_id(self, fl_id: str) -> str:
        max_num = 0
        for json_file in find_files(
            Path(self.base_dir, fl_id), prefix="analysis_", suffix=".json"
        ):
            try:
                num = int(json_file.name[9:-5])  # slice "analysis_" and ".json"
                if num > max_num:
                    max_num = num
            except ValueError:
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
        log_dir = Path(self.base_dir, fl_id, "logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        stderr_path = Path(log_dir, f"{analysis_id}.stderr.log")

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
