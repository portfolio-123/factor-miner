import os
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

from src.core.context import get_state
from src.core.types import DatasetConfig
from src.services.readers import ParquetDataReader

load_dotenv()

FACTOR_LIST_DIR = Path(os.getenv("FACTOR_LIST_DIR"))
INTEGRATIONS_DIR = FACTOR_LIST_DIR / "factor-miner"


def get_file_version(path: str) -> str:
    return str(int(os.path.getmtime(path)))


def get_dataset_file_path(fl_id: str, dataset_version: str) -> Path:
    return INTEGRATIONS_DIR / fl_id / dataset_version / "dataset_metadata.parquet"


def get_active_dataset_metadata(dataset_path: str) -> DatasetConfig:
    metadata = ParquetDataReader(dataset_path).get_dataset_info()
    metadata.version = get_file_version(dataset_path)
    return metadata


def get_dataset_metadata(
    fl_id: str, version: str, active_dataset_path: str | None
) -> DatasetConfig | None:
    """Load metadata for a single version - either from active path or backup."""
    if active_dataset_path and get_file_version(active_dataset_path) == version:
        return get_active_dataset_metadata(active_dataset_path)

    backup_path = get_dataset_file_path(fl_id, version)
    if backup_path.exists():
        metadata = ParquetDataReader(str(backup_path)).get_dataset_info()
        metadata.version = version
        return metadata
    return None


def get_next_version_number(fl_id: str) -> str:
    fl_dir = INTEGRATIONS_DIR / fl_id
    if not fl_dir.exists():
        return "1"
    existing = [int(d.name) for d in fl_dir.iterdir() if d.is_dir() and d.name.isdigit()]
    return str(max(existing, default=0) + 1)


def find_version_for_timestamp(fl_id: str, timestamp: str) -> str | None:
    fl_dir = INTEGRATIONS_DIR / fl_id
    if not fl_dir.exists():
        return None
    for d in fl_dir.iterdir():
        if d.is_dir() and d.name.isdigit():
            backup_path = get_dataset_file_path(fl_id, d.name)
            if backup_path.exists():
                metadata = ParquetDataReader(str(backup_path)).get_dataset_info()
                if metadata.sourceTimestamp == timestamp:
                    return d.name
    return None


def get_dataset_formulas(ds_ver: str) -> Optional[pd.DataFrame]:

    state = get_state()
    path: Optional[str] = None

    # check if we are looking at the current dataset, and if so, use its path
    if state.dataset_path and os.path.exists(state.dataset_path):
        if get_file_version(state.dataset_path) == ds_ver:
            path = state.dataset_path

    # if not, fallback to the backup path
    if not path:
        backup_path = get_dataset_file_path(state.factor_list_uid, ds_ver)
        if backup_path.exists():
            path = str(backup_path)

    return ParquetDataReader(path).get_formulas() if path else None
