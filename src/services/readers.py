from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.types.models import DatasetConfig


class ParquetDataReader:
    def __init__(self, file_path: Path | str):
        self.path = str(file_path)
        self._parquet_file = pq.ParquetFile(file_path)

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
        return raw

    def read_columns_pl(self, columns: list[str]) -> pl.DataFrame:
        return pl.read_parquet(self.path, columns=columns)

    def read_column_pa(self, column: str) -> pa.Array:
        return self._parquet_file.read([column]).column(0)

    def read_preview(self, num_rows=10) -> pl.DataFrame:
        pf = self._parquet_file
        total_rows = pf.metadata.num_rows

        # if the whole file is less than N*2 (for example, 10 first and 10 last rows), return the whole file
        if total_rows <= num_rows * 2:
            return pl.from_arrow(pf.read())  # type: ignore

        first_batch = next(pf.iter_batches(batch_size=num_rows))
        first_rows: pl.DataFrame = pl.from_arrow(first_batch)  # type: ignore

        # read last N rows from last row group
        last_row_group_idx = pf.num_row_groups - 1
        last_group = pf.read_row_group(last_row_group_idx)

        offset = max(0, last_group.num_rows - num_rows)
        last_rows_slice = last_group.slice(offset=offset, length=num_rows)
        last_rows: pl.DataFrame = pl.from_arrow(last_rows_slice)  # type: ignore

        # add row index column to track original positions
        first_rows = first_rows.with_row_index("_row_idx")
        last_rows = last_rows.with_row_index("_row_idx").with_columns(
            (pl.col("_row_idx") + (total_rows - last_rows.height)).alias("_row_idx")
        )

        return pl.concat([first_rows, last_rows])

    def get_review_metadata(self) -> dict[str, Any]:
        return {
            "numRows": self._parquet_file.metadata.num_rows,
            "unique_dates": self.read_columns_pl(["Date"])["Date"].n_unique(),
        }

    def get_dataset_info(self) -> DatasetConfig:
        raw = self._get_dataset_metadata_raw()
        if raw is None:
            raise ValueError("Missing datasetMetadata in parquet file")
        return DatasetConfig.model_validate_json(raw)

    def get_schema_metadata(self) -> dict[bytes, bytes] | None:
        return self._parquet_file.schema_arrow.metadata
