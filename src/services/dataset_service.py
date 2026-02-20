import json
import os
from pathlib import Path
from typing import Self

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.types.models import DatasetConfig
from src.services.readers import ParquetDataReader

from src.core.config.environment import FACTOR_LIST_DIR, FACTORMINER_DIR


class DatasetService:
    def __init__(self, fl_id: str):
        self.fl_id = fl_id
        self._reader: ParquetDataReader | None = None

    def __enter__(self) -> Self:
        self._reader = ParquetDataReader(self.base_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._reader.close()

    @property
    def base_path(self) -> Path:
        return FACTOR_LIST_DIR / self.fl_id

    @property
    def exists(self) -> bool:
        return self.base_path.exists()

    @property
    def current_version(self) -> str | None:
        if not self.base_path.exists():
            return None
        return str(int(os.path.getmtime(self.base_path)))

    @property
    def column_names(self) -> list[str]:
        return self._reader.column_names

    def get_metadata(self) -> DatasetConfig:
        if not self.base_path.exists():
            raise FileNotFoundError(f"Dataset file not found: {self.base_path}")
        metadata = self._reader.get_dataset_info()
        metadata.version = self.current_version
        metadata.active = True
        return metadata

    def get_review_data(self) -> tuple[pd.DataFrame, dict]:
        preview_df = self._reader.read_preview(num_rows=10)
        metadata = self._reader.get_review_metadata()

        stats = {
            "num_rows": metadata.get("num_rows"),
            "num_columns": len(preview_df.columns),
            "num_dates": metadata.get("unique_dates"),
        }
        return preview_df, stats

    def read_columns(self, columns: list) -> pd.DataFrame:
        return self._reader.read_columns(columns)

    def backup_metadata(self, dest_path: Path) -> None:
        source_metadata = self._reader.get_schema_metadata()
        if source_metadata:
            num_rows = self._reader._parquet_file.metadata.num_rows
            dataset_metadata_raw = source_metadata.get(b"datasetMetadata")
            if dataset_metadata_raw:
                dataset_metadata = json.loads(dataset_metadata_raw.decode("utf-8"))
                dataset_metadata["numRows"] = num_rows
                updated_metadata = {
                    **source_metadata,
                    b"datasetMetadata": json.dumps(dataset_metadata).encode("utf-8"),
                }
            else:
                updated_metadata = source_metadata

            table = pa.table({}).replace_schema_metadata(updated_metadata)
            pq.write_table(table, dest_path)

class BackupDatasetService:
    def __init__(self, fl_id: str):
        self.fl_id = fl_id

    @property
    def backup_dir(self) -> Path:
        return FACTORMINER_DIR / self.fl_id

    def get_backup_path(self, version: str) -> Path:
        return self.backup_dir / f"{version}.parquet"

    def get_metadata(self, version: str) -> DatasetConfig:
        with ParquetDataReader(self.get_backup_path(version)) as reader:
            metadata = reader.get_dataset_info()
        metadata.version = version
        current = DatasetService(self.fl_id).current_version
        metadata.active = current is not None and version == current
        return metadata

    def load_all_versions(self) -> dict[str, DatasetConfig]:
        if not self.backup_dir.exists():
            return {}

        current = DatasetService(self.fl_id).current_version
        result = {}
        for f in self.backup_dir.glob("*.parquet"):
            version = f.stem
            dataset = self.get_metadata(version)
            dataset.active = current is not None and version == current
            result[version] = dataset
        return result
