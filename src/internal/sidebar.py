from pathlib import Path

from src.core.utils.common import find_files
from src.core.config.environment import DATASET_DIR
from src.core.types.models import DatasetDetails
from src.services.dataset_service import BackupDatasetService, DatasetService


def list_user_datasets(user_uid: str) -> dict[str, str]:
    user_dir = Path(DATASET_DIR, user_uid)
    factor_miner_dir = Path(user_dir, "FactorMiner")
    results = {}

    try:
        for f in filter(Path.is_file, user_dir.iterdir()):
            try:
                with DatasetService(
                    DatasetDetails(fl_id=f.name, user_uid=user_uid)
                ) as svc:
                    results[f.name] = svc.get_metadata().factorListName
            except Exception:
                pass
    except (FileNotFoundError, NotADirectoryError):
        return {}

    try:
        for d in factor_miner_dir.iterdir():
            fl_id = d.name
            if fl_id in results or not any(
                find_files(d, prefix="dataset_", suffix=".json")
            ):
                continue
            try:
                latest = BackupDatasetService(
                    DatasetDetails(fl_id=fl_id, user_uid=user_uid)
                ).load_latest_version()
                if latest:
                    results[fl_id] = latest.factorListName
            except Exception:
                pass
    except (FileNotFoundError, NotADirectoryError):
        pass

    return dict(sorted(results.items()))
