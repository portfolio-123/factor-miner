import json
import os
from pathlib import Path
from typing import Self

import polars as pl

from src.core.config.environment import DATASET_DIR
from src.core.types.models import DatasetConfig
from src.core.utils.common import format_version_timestamp
from src.services.readers import ParquetDataReader


class DatasetService:
    def __init__(self, dataset_details: DatasetDetails):
        self.dataset_details = dataset_details

    def __enter__(self) -> Self:
        self._reader = ParquetDataReader(self.base_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._reader.close()

    @property
    def base_path(self) -> Path:
        return self.dataset_details.get_base_path()

    @staticmethod
    def list_datasets() -> list[str]:
        if not DATASET_DIR.exists():
            return []
        return sorted(f.stem for f in DATASET_DIR.glob("*.parquet"))

    @property
    def exists(self) -> bool:
        return self.base_path.exists()

    @property
    def current_version(self) -> str | None:
        if not self.base_path.exists():
            return None
        return format_version_timestamp(os.path.getmtime(self.base_path))

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

    def get_review_data(self) -> tuple[pl.DataFrame, dict]:
        preview_df = self._reader.read_preview(num_rows=10)
        metadata = self._reader.get_review_metadata()

        stats = {
            "num_rows": metadata.get("num_rows"),
            "num_columns": len(preview_df.columns),
            "num_dates": metadata.get("unique_dates"),
        }
        return preview_df, stats

    def read_columns(self, columns: list) -> pl.DataFrame:
        return self._reader.read_columns(columns)

    def backup_metadata(self, dest_path: Path) -> None:
        source_metadata = self._reader.get_schema_metadata()
        if source_metadata:
            num_rows = self._reader._parquet_file.metadata.num_rows
            dataset_metadata_raw = source_metadata.get(b"datasetMetadata")
            if dataset_metadata_raw:
                dataset_metadata = json.loads(dataset_metadata_raw.decode("utf-8"))
                dataset_metadata["numRows"] = num_rows
                dataset_metadata["version"] = str(dataset_metadata.get("version", ""))
                with open(dest_path, "w") as f:
                    json.dump(dataset_metadata, f, indent=2)


class BackupDatasetService:
    def __init__(self, dataset_details: DatasetDetails):
        self.dataset_details = dataset_details

    @property
    def backup_dir(self) -> Path:
        return self.dataset_details.get_backup_dir()

    def get_backup_path(self, version: str) -> Path:
        return self.backup_dir / f"dataset_{version}.json"

    def get_metadata(self, version: str) -> DatasetConfig:
        with open(self.get_backup_path(version)) as f:
            data = json.load(f)
        if data.get("normalization") is True and "preprocessor" in data:
            data["normalization"] = data["preprocessor"]
        data.pop("preprocessor", None)
        metadata = DatasetConfig(**data)
        metadata.version = version
        current = DatasetService(self.dataset_details).current_version
        metadata.active = current is not None and version == current
        return metadata

    def load_all_versions(self) -> dict[str, DatasetConfig]:
        if not self.backup_dir.exists():
            return {}

        current = DatasetService(self.dataset_details).current_version
        result = {}
        for f in self.backup_dir.glob("dataset_*.json"):
            version = f.stem.removeprefix("dataset_")
            dataset = self.get_metadata(version)
            dataset.active = current is not None and version == current
            result[version] = dataset
        return result
