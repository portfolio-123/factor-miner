import os
from pathlib import Path
from typing import Optional, Dict, Tuple

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


def get_current_dataset_info(dataset_path: str) -> Tuple[str, DatasetConfig]:
    current_version = get_file_version(dataset_path)
    dataset_info = ParquetDataReader(dataset_path).get_dataset_info()
    return current_version, dataset_info


def get_past_analyses_metadata(fl_id: str, versions: list[str]) -> Dict[str, DatasetConfig]:
    metadata_by_version: Dict[str, DatasetConfig] = {}
    for ver in versions:
        backup_path = get_dataset_file_path(fl_id, ver)
        metadata_by_version[ver] = ParquetDataReader(str(backup_path)).get_dataset_info()
    return metadata_by_version


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
