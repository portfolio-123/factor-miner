import json

from src.core.config.environment import DATASET_DIR
from src.services.readers import ParquetDataReader


def list_user_datasets(user_uid: str) -> list[tuple[str, str]]:
    user_dir = DATASET_DIR / user_uid
    if not user_dir.exists():
        return []

    fl_ids = {f.name for f in user_dir.iterdir() if f.is_file()}

    factor_miner_dir = user_dir / "FactorMiner"
    if factor_miner_dir.exists():
        fl_ids |= {
            d.name
            for d in factor_miner_dir.iterdir()
            if d.is_dir() and any(d.glob("*.json"))
        }

    results = []
    for fl_id in sorted(fl_ids):
        name = None
        main_file = user_dir / fl_id
        if main_file.exists():
            try:
                with ParquetDataReader(main_file) as reader:
                    name = reader.get_dataset_info().factorListName
            except Exception:
                pass
        else:
            backup_dir = factor_miner_dir / fl_id
            if backup_dir.exists():
                backups = list(backup_dir.glob("dataset_*.json"))
                if backups:
                    try:
                        with open(max(backups, key=lambda p: p.stem)) as f:
                            name = json.load(f).get("factorListName")
                    except Exception:
                        pass
        results.append((fl_id, name or fl_id))
    return results
