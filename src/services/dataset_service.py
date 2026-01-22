import os
from pathlib import Path

import pandas as pd

from src.core.environment import FACTOR_LIST_DIR, FACTORMINER_DIR
from src.core.types import DatasetConfig
from src.core.utils import format_date
from src.services.readers import ParquetDataReader


def get_file_mtime(path: str) -> str:
    return str(int(os.path.getmtime(path)))


def get_dataset_file_path(fl_id: str, dataset_version: str) -> Path:
    return FACTORMINER_DIR / fl_id / f"{dataset_version}.parquet"


def get_active_dataset_path(fl_id: str) -> str:
    return str(FACTOR_LIST_DIR / fl_id)


def get_active_dataset_metadata(fl_id: str) -> DatasetConfig:
    return ParquetDataReader(str(FACTOR_LIST_DIR / fl_id)).get_dataset_info()

def get_backup_dataset_metadata(fl_id: str, version: str) -> DatasetConfig:
    metadata = ParquetDataReader(
        str(get_dataset_file_path(fl_id, version))
    ).get_dataset_info()
    metadata.version = version
    return metadata

def list_versions(fl_id: str) -> list[str]:
    fl_dir = FACTORMINER_DIR / fl_id
    if not fl_dir.exists():
        return []
    # Get version from parquet filenames (without .parquet extension)
    return [f.stem for f in fl_dir.glob("*.parquet")]


def get_dataset_review_data(fl_id: str) -> tuple[pd.DataFrame, dict]:
    reader = ParquetDataReader(get_active_dataset_path(fl_id))
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
