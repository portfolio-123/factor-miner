import json
from pathlib import Path

from core.utils.common import find_files
from src.core.config.environment import DATASET_DIR
from src.services.readers import ParquetDataReader


def list_user_datasets(user_uid: str) -> list[tuple[str, str]]:
    user_dir = Path(DATASET_DIR, user_uid)

    try:
        fl_ids = {f.name for f in user_dir.iterdir() if f.is_file()}
    except (FileNotFoundError, NotADirectoryError):
        return []

    factor_miner_dir = Path(user_dir, "FactorMiner")
    try:
        fl_ids.union(
            d.name
            for d in factor_miner_dir.iterdir()
            if any(find_files(d, suffix=".json"))
        )
    except (FileNotFoundError, NotADirectoryError):
        pass

    results = []
    for fl_id in sorted(fl_ids):
        name = None
        try:
            try:
                with ParquetDataReader(Path(user_dir, fl_id)) as reader:
                    name = reader.get_dataset_info().factorListName
            except FileNotFoundError:
                backup = max(
                    find_files(
                        Path(factor_miner_dir, fl_id), prefix="dataset_", suffix=".json"
                    ),
                    key=lambda p: p.name,
                    default=None,
                )
                if backup:
                    with open(backup.path) as f:
                        name = json.load(f).get("factorListName")
        except Exception:
            pass
        results.append((fl_id, name or fl_id))
    return results
