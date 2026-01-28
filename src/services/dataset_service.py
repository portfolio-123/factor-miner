import os
from functools import cached_property
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.environment import FACTOR_LIST_DIR, FACTORMINER_DIR
from src.core.types import DatasetConfig
from src.services.readers import ParquetDataReader


class DatasetService:
    def __init__(self, fl_id: str):
        self.fl_id = fl_id

    @property
    def base_path(self) -> Path:
        return FACTOR_LIST_DIR / self.fl_id

    @property
    def exists(self) -> bool:
        return self.base_path.exists()

    @property
    def backup_dir(self) -> Path:
        return FACTORMINER_DIR / self.fl_id

    @cached_property
    def _reader(self) -> ParquetDataReader:
        return ParquetDataReader(str(self.base_path))

    @property
    def current_version(self) -> str | None:
        if not self.base_path.exists():
            return None
        return str(int(os.path.getmtime(self.base_path)))

    @property
    def column_names(self) -> list[str]:
        return self._reader.column_names

    def get_backup_path(self, version: str) -> Path:
        return self.backup_dir / f"{version}.parquet"

    def get_metadata(self, version: str | None = None) -> DatasetConfig:
        if version:
            reader = ParquetDataReader(str(self.get_backup_path(version)))
        else:
            if not self.base_path.exists():
                raise FileNotFoundError(f"Dataset file not found: {self.base_path}")
            reader = self._reader
            version = self.current_version

        metadata = reader.get_dataset_info()
        metadata.version = version
        return metadata

    def get_preview(self, num_rows: int = 10) -> pd.DataFrame:
        return self._reader.read_preview(num_rows)

    def get_review_data(self) -> tuple[pd.DataFrame, dict]:
        preview_df = self.get_preview(num_rows=10)
        metadata = self._reader.get_review_metadata()

        stats = {
            "num_rows": metadata.get("num_rows"),
            "num_columns": len(preview_df.columns),
            "num_dates": metadata.get("unique_dates"),
        }
        return preview_df, stats

    def read_columns(self, columns: list) -> pd.DataFrame:
        return self._reader.read_columns(columns)

    def load_all_versions(self) -> dict[str, DatasetConfig]:
        if not self.backup_dir.exists():
            return {}

        active = self.current_version
        result = {}
        for f in self.backup_dir.glob("*.parquet"):
            version = f.stem
            dataset = self.get_metadata(version)
            dataset.active = active is not None and version == active
            result[version] = dataset
        return result

    def backup_metadata(self, dest_path: Path) -> None:
        try:
            source_file = pq.ParquetFile(self.base_path)
            source_metadata = source_file.schema_arrow.metadata
            if source_metadata:
                table = pa.table({}).replace_schema_metadata(source_metadata)
                pq.write_table(table, dest_path)
        except Exception as e:
            print(f"Error backing up parquet metadata: {e}")


_cache: dict[str, DatasetService] = {}


def dataset_service(fl_id: str) -> DatasetService:
    if fl_id not in _cache:
        _cache[fl_id] = DatasetService(fl_id)
    return _cache[fl_id]
