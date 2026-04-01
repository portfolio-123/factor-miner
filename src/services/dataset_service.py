import json
from os import stat
from pathlib import Path
from types import TracebackType

import polars as pl
import pyarrow as pa

from src.core.config.environment import DATASET_DIR, INTERNAL_MODE
from src.core.types.models import DatasetConfig, DatasetDetails
from src.core.utils.common import find_files, format_version_timestamp
from src.services.readers import ParquetDataReader


class DatasetService:
    def __init__(self, dataset_details: DatasetDetails):
        self.dataset_details = dataset_details

    def __enter__(self):
        self._reader = ParquetDataReader(self.base_path)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException,
        exc_tb: TracebackType | None,
    ) -> None:
        self._reader.close()

    @property
    def base_path(self) -> Path:
        return self.dataset_details.get_base_path()

    @staticmethod
    def list_datasets():
        return sorted(
            f.name[:-8]  # Remove ".parquet" suffix
            for f in find_files(DATASET_DIR, suffix=".parquet")
        )

    @staticmethod
    def fetch_benchmark(
        ticker: str,
        start_date: str,
        end_date: str,
        access_token: str | None = None,
    ) -> pl.DataFrame:
        if INTERNAL_MODE:
            from src.internal.p123_client import fetch_benchmark_data

            assert access_token is not None
            return fetch_benchmark_data(
                benchmark_ticker=ticker,
                access_token=access_token,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            from src.services.benchmark_service import fetch_benchmark_external

            return fetch_benchmark_external(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
            )

    @property
    def current_version(self) -> str:
        return format_version_timestamp(stat(self.base_path).st_mtime)

    @property
    def column_names(self):
        return self._reader.column_names

    def get_metadata(self) -> DatasetConfig:
        metadata = self._reader.get_dataset_info()
        metadata.version = self.current_version
        metadata.active = True
        return metadata

    def get_preview_data(self):
        return self._reader.read_preview(num_rows=10)

    def read_columns_pl(self, columns: list[str]) -> pl.DataFrame:
        return self._reader.read_columns_pl(columns)

    def read_column_pa(self, column: str) -> pa.ChunkedArray:
        return self._reader.read_column_pa(column)

    def back_up_metadata(self, dest_path: Path) -> None:
        source_metadata = self._reader.get_schema_metadata()
        if source_metadata:
            num_rows = self._reader.get_metadata().num_rows
            dataset_metadata_raw = source_metadata.get(b"datasetMetadata")
            if dataset_metadata_raw:
                dataset_metadata = json.loads(dataset_metadata_raw)
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
        return Path(self.backup_dir, f"dataset_{version}.json")

    def get_metadata(self, version: str) -> DatasetConfig:
        raw = self.get_backup_path(version).read_bytes()
        metadata = DatasetConfig.model_validate_json(raw)
        metadata.version = version
        current = DatasetService(self.dataset_details).current_version
        metadata.active = version == current
        return metadata

    def load_latest_version(self) -> DatasetConfig | None:
        files = find_files(self.backup_dir, prefix="dataset_", suffix=".json")
        latest_file = max(files, key=lambda f: f.name, default=None)
        if not latest_file:
            return None
        version = latest_file.name[8:-5]  # slice "dataset_" and ".json"
        return self.get_metadata(version)

    def load_all_versions(self):
        files = list(find_files(self.backup_dir, prefix="dataset_", suffix=".json"))

        result: dict[str, DatasetConfig] = {}
        if files:
            current = DatasetService(self.dataset_details).current_version
            for f in files:
                version = f.name[8:-5]  # slice "dataset_" and ".json"
                dataset = self.get_metadata(version)
                dataset.active = version == current
                result[version] = dataset

        return result
