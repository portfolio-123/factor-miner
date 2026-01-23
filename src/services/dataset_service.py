import os
from pathlib import Path

import pandas as pd

from src.core.environment import FACTOR_LIST_DIR, FACTORMINER_DIR
from src.core.types import DatasetConfig
from src.services.readers import ParquetDataReader


def get_file_mtime(path: str) -> str:
    return str(int(os.path.getmtime(path)))


def get_dataset_file_path(fl_id: str, dataset_version: str) -> Path:
    return FACTORMINER_DIR / fl_id / f"{dataset_version}.parquet"

def get_dataset_metadata(fl_id: str, version: str | None = None) -> DatasetConfig:
    path = str(get_dataset_file_path(fl_id, version) if version else FACTOR_LIST_DIR / fl_id)
    metadata = ParquetDataReader(path).get_dataset_info()
    metadata.version = version or get_file_mtime(path)
    return metadata

def list_versions(fl_id: str) -> list[str]:
    fl_dir = FACTORMINER_DIR / fl_id
    if not fl_dir.exists():
        return []
    # Get version from parquet filenames (without .parquet extension)
    return [f.stem for f in fl_dir.glob("*.parquet")]


def get_dataset_review_data(fl_id: str) -> tuple[pd.DataFrame, dict]:
    reader = ParquetDataReader(str(FACTOR_LIST_DIR / fl_id))
    preview_df = reader.read_preview(num_rows=10)

    metadata = reader.get_review_metadata()

    stats = {
        "num_rows": metadata.get("num_rows"),
        "num_columns": len(preview_df.columns),
        "num_dates": metadata.get("unique_dates"),
    }
    return preview_df, stats
