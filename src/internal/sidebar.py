from pathlib import Path

from core.utils.common import find_files
from src.core.config.environment import DATASET_DIR
from src.core.types.models import DatasetDetails
from src.services.dataset_service import BackupDatasetService, DatasetService


def list_user_datasets(user_uid: str) -> dict[str, str]:
    user_dir = Path(DATASET_DIR, user_uid)

    try:
        fl_ids = {f.name for f in user_dir.iterdir() if f.is_file()}
    except (FileNotFoundError, NotADirectoryError):
        return {}

    factor_miner_dir = Path(user_dir, "FactorMiner")
    try:
        fl_ids |= {
            d.name
            for d in factor_miner_dir.iterdir()
            if any(find_files(d, suffix=".json"))
        }
    except (FileNotFoundError, NotADirectoryError):
        pass

    results = {}

    # load backups first
    for fl_id in fl_ids:
        dataset_details = DatasetDetails(fl_id=fl_id, user_uid=user_uid)
        try:
            versions = BackupDatasetService(dataset_details).load_all_versions()
            if versions:
                results[fl_id] = versions[max(versions.keys())].factorListName
        except Exception:
            results[fl_id] = fl_id

    # overlay with active file if present
    for fl_id in fl_ids:
        dataset_details = DatasetDetails(fl_id=fl_id, user_uid=user_uid)
        try:
            with DatasetService(dataset_details) as svc:
                results[fl_id] = svc.get_metadata().factorListName
        except FileNotFoundError:
            pass  # no active file, keep backup name

    return dict(sorted(results.items()))
