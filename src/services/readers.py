import json
from pathlib import Path
from typing import Any

import polars as pl
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

    def read_columns(self, columns: list) -> pl.DataFrame:
        arrow_table = self._parquet_file.read(columns=columns)
        df = pl.from_arrow(arrow_table)
        return df

    def read_preview(self, num_rows: int = 10) -> pl.DataFrame:
        pf = self._parquet_file
        total_rows = pf.metadata.num_rows

        # if the whole file is less than N*2 (for example, 10 first and 10 last rows), return the whole file
        if total_rows <= num_rows * 2:
            return pl.from_arrow(pf.read())

        first_batch = next(pf.iter_batches(batch_size=num_rows))
        first_rows = pl.from_arrow(first_batch)

        # read last N rows from last row group
        last_row_group_idx = pf.num_row_groups - 1
        last_group = pf.read_row_group(last_row_group_idx)

        offset = max(0, last_group.num_rows - num_rows)
        last_rows_slice = last_group.slice(offset=offset, length=num_rows)
        last_rows = pl.from_arrow(last_rows_slice)

        # add row index column to track original positions
        first_rows = first_rows.with_row_index("_row_idx")
        last_rows = last_rows.with_row_index("_row_idx").with_columns(
            (pl.col("_row_idx") + total_rows - len(last_rows)).alias("_row_idx")
        )

        return pl.concat([first_rows, last_rows])

    def get_review_metadata(self) -> dict[str, Any]:
        return {
            "num_rows": self._parquet_file.metadata.num_rows,
            "unique_dates": self.read_columns(["Date"])["Date"].n_unique(),
        }

    def get_dataset_info(self) -> DatasetConfig:
        raw = self._get_dataset_metadata_raw()
        if raw is None:
            raise ValueError("Missing datasetMetadata in parquet file")

        dataset_info = json.loads(raw)

        if dataset_info.get("normalization") is True and "preprocessor" in dataset_info:
            dataset_info["normalization"] = dataset_info["preprocessor"]
        dataset_info.pop("preprocessor", None)

        return DatasetConfig(**dataset_info)

    def get_schema_metadata(self) -> dict[bytes, bytes] | None:
        return self._parquet_file.schema_arrow.metadata
