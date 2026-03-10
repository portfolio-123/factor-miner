import streamlit as st

from src.core.config.environment import FACTOR_LIST_DIR
from src.internal.p123_client import verify_factor_list_access
from src.services.readers import ParquetDataReader


def _read_name_from_parquet(path) -> str | None:
    try:
        with ParquetDataReader(path) as reader:
            return reader.get_dataset_info().factorListName
    except Exception:
        return None


def list_user_datasets(user_uid: str) -> list[tuple[str, str]]:
    user_dir = FACTOR_LIST_DIR / user_uid
    if not user_dir.exists():
        return []

    fl_ids = {f.name for f in user_dir.iterdir() if f.is_file()}

    factor_miner_dir = user_dir / "FactorMiner"
    if factor_miner_dir.exists():
        fl_ids |= {d.name for d in factor_miner_dir.iterdir() if d.is_dir() and any(d.glob("*.json"))}

    results = []
    for fl_id in sorted(fl_ids):
        name = _read_name_from_parquet(user_dir / fl_id)
        if not name:
            backup_dir = factor_miner_dir / fl_id
            if backup_dir.exists():
                backups = list(backup_dir.glob("*.parquet"))
                if backups:
                    name = _read_name_from_parquet(max(backups, key=lambda p: p.stem))
        results.append((fl_id, name or fl_id))
    return results


def update_fl_name_on_select(selected: str) -> None:
    if token := st.session_state.get("access_token"):
        try:
            st.session_state.fl_name = verify_factor_list_access(selected, token).get("name", selected)
        except PermissionError:
            st.session_state.access_token = None
