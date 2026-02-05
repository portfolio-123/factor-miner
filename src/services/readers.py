import json
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.core.types.models import DatasetConfig


class ParquetDataReader:
    def __init__(self, file_path: Path | str):
        self._parquet_file = pq.ParquetFile(str(file_path))

    def close(self) -> None:
        self._parquet_file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    @property
    def column_names(self) -> list[str]:
        return self._parquet_file.schema_arrow.names

    def _get_dataset_metadata_raw(self) -> str | None:
        metadata = self._parquet_file.schema_arrow.metadata
        if metadata is None:
            return None
        raw = metadata.get(b"datasetMetadata")
        if raw is None:
            return None
        return raw.decode("utf-8")

    def read_columns(self, columns: list) -> pd.DataFrame:
        df = self._parquet_file.read(columns=columns).to_pandas()
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
        return df

    def read_preview(self, num_rows: int = 10) -> pd.DataFrame:
        pf = self._parquet_file
        total_rows = pf.metadata.num_rows

        # if the whole file is less than N*2 (for example, 10 first and 10 last rows), return the whole file
        if total_rows <= num_rows * 2:
            return pf.read().to_pandas()

        first_batch = next(pf.iter_batches(batch_size=num_rows))
        first_rows = first_batch.to_pandas()

        # read last N rows from last row group
        last_row_group_idx = pf.num_row_groups - 1
        last_group = pf.read_row_group(last_row_group_idx)

        offset = max(0, last_group.num_rows - num_rows)
        last_rows_slice = last_group.slice(offset=offset, length=num_rows)
        last_rows = last_rows_slice.to_pandas()

        last_rows.index = range(total_rows - len(last_rows), total_rows)

        return pd.concat([first_rows, last_rows], ignore_index=False)

    def get_review_metadata(self) -> dict[str, Any]:
        return {
            "num_rows": self._parquet_file.metadata.num_rows,
            "unique_dates": self.read_columns(["Date"])["Date"].nunique(),
        }

    def get_dataset_info(self) -> DatasetConfig:
        raw = self._get_dataset_metadata_raw()
        if raw is None:
            raise ValueError("Missing datasetMetadata in parquet file")

        dataset_info = json.loads(raw)

        dataset_info["version"] = str(dataset_info["version"])

        if dataset_info.get("normalization") is True and "preprocessor" in dataset_info:
            dataset_info["normalization"] = dataset_info["preprocessor"]
        else:
            dataset_info["normalization"] = None
        dataset_info.pop("preprocessor", None)

        return DatasetConfig(**dataset_info)

    def get_schema_metadata(self) -> dict[bytes, bytes] | None:
        return self._parquet_file.schema_arrow.metadata
