from functools import cached_property
import json
from typing import Optional, Dict, Any, Tuple
import os

import pandas as pd
import pyarrow.parquet as pq

from src.core.constants import REQUIRED_COLUMNS
from src.core.types import DatasetConfig


class ParquetDataReader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    @cached_property
    def _parquet_file(self) -> pq.ParquetFile:
        return pq.ParquetFile(self.file_path)

    @cached_property
    def _custom_metadata(self) -> Dict[str, str]:
        try:
            schema_metadata = self._parquet_file.schema_arrow.metadata
            if schema_metadata is None:
                return {}
            return {
                k.decode("utf-8"): v.decode("utf-8") for k, v in schema_metadata.items()
            }
        except Exception:
            return {}

    def read_full(self) -> Optional[pd.DataFrame]:
        try:
            return self._parquet_file.read().to_pandas()
        except Exception:
            return None

    def read_columns(self, columns: list) -> Optional[pd.DataFrame]:
        try:
            return self._parquet_file.read(columns=columns).to_pandas()
        except Exception:
            return None

    def read_preview(self, num_rows: int = 10) -> Optional[pd.DataFrame]:
        try:

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
        except Exception:
            return None

    def get_metadata(self) -> Dict[str, Any]:
        try:
            pf = self._parquet_file
            num_rows = pf.metadata.num_rows
            columns = pf.schema_arrow.names
            unique_dates = None

            if "Date" in columns:
                date_df = self.read_columns(["Date"])
                if date_df is not None:
                    unique_dates = pd.to_datetime(date_df["Date"]).nunique()
            return {
                "valid": True,
                "num_rows": num_rows,
                "num_columns": len(columns),
                "columns": columns,
                "unique_dates": unique_dates,
            }
        except Exception as e:
            return {"valid": False, "error": str(e)}

    def _get_custom_metadata_json(self, key: str) -> Optional[Any]:
        raw = self._custom_metadata.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def get_formulas_df(self) -> Optional[pd.DataFrame]:
        features_json = self._get_custom_metadata_json("features")
        if not features_json:
            return None

        try:
            df = pd.DataFrame(features_json)

            expected_cols = ["formula", "name", "tag", "Normalization"]
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = "" if col != "Normalization" else "Raw"

            # normalize missing/empty values
            df["Normalization"] = df["Normalization"].replace("", "Raw").fillna("Raw")

            return df[expected_cols]
        except Exception:
            return None

    def get_dataset_info(self) -> DatasetConfig:
        dataset_info = self._get_custom_metadata_json("dataset")

        norm = dataset_info.get("normalization")
        if isinstance(norm, str):
            try:
                loaded = json.loads(norm)
            except Exception:
                loaded = None
            dataset_info["normalization"] = loaded if isinstance(loaded, dict) else None

        features_json = self._get_custom_metadata_json("features")
        if features_json and isinstance(features_json, list):
            dataset_info["factorCount"] = len(features_json)

        return DatasetConfig(**dataset_info)


def get_current_dataset_info(
    dataset_path: str,
) -> Tuple[Optional[str], Optional[DatasetConfig]]:
    try:
        # get modification timestamp as id
        ts = os.path.getmtime(dataset_path)
        current_version = str(int(ts))

        reader = ParquetDataReader(dataset_path)
        dataset_info = reader.get_dataset_info()

        return current_version, dataset_info
    except (ValueError, Exception):
        return None, None
