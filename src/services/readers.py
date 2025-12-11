from functools import cached_property
import json
from typing import Optional, Dict, Any, Tuple

import pandas as pd
import pyarrow.parquet as pq

from src.core.constants import REQUIRED_COLUMNS


class ParquetDataReader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    @cached_property
    def _parquet_file(self) -> pq.ParquetFile:
        return pq.ParquetFile(self.file_path)


    def validate(self) -> Tuple[bool, Optional[str]]:
        try:
            columns = self._parquet_file.schema_arrow.names
            missing = [col for col in REQUIRED_COLUMNS if col not in columns]
            if missing:
                return False, f"Missing required columns: {', '.join(missing)}"
            return True, None
        except Exception as e:
            return False, str(e)

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

            if 'Date' in columns:
                date_df = self.read_columns(['Date'])
                if date_df is not None:
                    unique_dates = pd.to_datetime(date_df['Date']).nunique()
            return {
                'valid': True,
                'num_rows': num_rows,
                'num_columns': len(columns),
                'columns': columns,
                'unique_dates': unique_dates
            }
        except Exception as e:
            return {'valid': False, 'error': str(e)}

    def get_custom_metadata(self) -> Optional[Dict[str, str]]:
        try:
            schema_metadata = self._parquet_file.schema_arrow.metadata
            if schema_metadata is None:
                return None
            return {
                k.decode('utf-8'): v.decode('utf-8')
                for k, v in schema_metadata.items()
            }
        except Exception:
            return None

    def get_metadata_bundle(self) -> Tuple[Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
        try:
            metadata = self.get_custom_metadata()
                
            formulas_df = None
            if 'features' in metadata:
                features_json = json.loads(metadata['features'])
                df = pd.DataFrame(features_json)

                expected_cols = ['formula', 'name', 'tag', 'Normalization']
                for col in expected_cols:
                    if col not in df.columns:
                        df[col] = '' if col != 'Normalization' else 'Raw'
                
                if 'Normalization' in df.columns:
                        df['Normalization'] = df['Normalization'].replace('', 'Raw').fillna('Raw')

                formulas_df = df[['formula', 'name', 'tag', 'Normalization']]

            dataset_info = None
            if 'dataset' in metadata:
                dataset_info = json.loads(metadata['dataset'])

            return formulas_df, dataset_info
        except Exception:
            return None, None

