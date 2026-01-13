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
INTEGRATIONS_DIR = FACTOR_LIST_DIR / "factor-eval"


def get_file_version(path: str) -> str:
    return str(int(os.path.getmtime(path)))


def get_dataset_file_path(fl_id: str, dataset_version: str) -> Path:
    return INTEGRATIONS_DIR / fl_id / dataset_version / "dataset_metadata.parquet"


def get_current_dataset_info(
    dataset_path: str,
) -> Tuple[Optional[str], Optional[DatasetConfig]]:
    try:
        current_version = get_file_version(dataset_path)
        reader = ParquetDataReader(dataset_path)
        dataset_info = reader.get_dataset_info()

        return current_version, dataset_info
    except (ValueError, Exception):
        return None, None


def get_all_dataset_info(
    fl_id: str,
    versions: list[str],
    current_version: Optional[str],
    current_info: Optional[DatasetConfig],
) -> Dict[str, DatasetConfig]:
    ds_info_map: Dict[str, DatasetConfig] = {}

    for ver in versions:
        # Always try backup first - it may have edited name/description
        backup_path = get_dataset_file_path(fl_id, ver)
        if backup_path.exists():
            info = ParquetDataReader(str(backup_path)).get_dataset_info()
            if info:
                ds_info_map[ver] = info
                continue

        # Fall back to live file info for current version (no backup = no jobs yet)
        if ver == current_version and current_info:
            ds_info_map[ver] = current_info

    return ds_info_map


def get_history_page_data(fl_id: str, dataset_path: str):
    from src.workers.manager import get_grouped_jobs, sort_dataset_versions

    # get the active dataset info and the jobs grouped by version
    active_version, active_info = get_current_dataset_info(dataset_path)
    jobs_by_version = get_grouped_jobs(fl_id)

    # get all versions from the jobs and add the active version if it exists
    all_versions = set(jobs_by_version.keys())
    if active_version:
        all_versions.add(active_version)
    versions = sort_dataset_versions(list(all_versions))

    # get the metadata for all versions
    version_metadata = get_all_dataset_info(fl_id, versions, active_version, active_info)

    return active_version, versions, version_metadata, jobs_by_version


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
