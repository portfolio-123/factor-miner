import os
from pathlib import Path
import streamlit as st

import pandas as pd

from src.core.context import get_state
from src.core.environment import FACTOR_LIST_DIR, FACTORMINER_DIR
from src.core.types import DatasetConfig
from src.core.utils import format_date
from src.services.readers import ParquetDataReader


def get_file_mtime(path: str) -> str:
    return str(int(os.path.getmtime(path)))


def parse_version_dir(name: str) -> tuple[int, str]:
    parts = name.rsplit("_", 1)
    return int(parts[0]), parts[1]


def get_dataset_file_path(fl_id: str, dataset_version: str) -> Path:
    return FACTORMINER_DIR / fl_id / dataset_version / "dataset_metadata.parquet"


def get_active_dataset_metadata(fl_id: str) -> DatasetConfig:
    return ParquetDataReader(str(FACTOR_LIST_DIR / fl_id)).get_dataset_info()

def get_backup_dataset_metadata(fl_id: str, version: str) -> DatasetConfig:
    metadata = ParquetDataReader(
        str(get_dataset_file_path(fl_id, version))
    ).get_dataset_info()
    metadata.version = version
    return metadata


def create_version_dir_name(fl_id: str, timestamp: str) -> str:
    fl_dir = FACTORMINER_DIR / fl_id
    if not fl_dir.exists():
        return f"1_{timestamp}"
    existing = []
    for d in fl_dir.iterdir():
        parsed = parse_version_dir(d.name)
        if parsed:
            existing.append(parsed[0])
    next_num = max(existing, default=0) + 1
    # find max number and set the version as max+1
    return f"{next_num}_{timestamp}"


def find_version_for_timestamp(fl_id: str, timestamp: str) -> str | None:
    fl_dir = FACTORMINER_DIR / fl_id
    return next((dir.name for dir in fl_dir.glob(f"*_{timestamp}")), None)


def list_versions(fl_id: str) -> list[str]:
    fl_dir = FACTORMINER_DIR / fl_id
    if not fl_dir.exists():
        return []
    return [d.name for d in fl_dir.iterdir()]


def get_dataset_review_data() -> tuple[pd.DataFrame, dict]:
    state = get_state()
    reader = ParquetDataReader(state.active_dataset_file)
    preview_df = reader.read_preview(num_rows=10)

    metadata = reader.get_review_metadata()

    dates = pd.to_datetime(preview_df["Date"])
    stats = {
        "num_rows": metadata.get("num_rows"),
        "num_columns": len(preview_df.columns),
        "num_dates": metadata.get("unique_dates"),
        "min_date": format_date(dates.min()),
        "max_date": format_date(dates.max()),
    }
    return preview_df, stats
