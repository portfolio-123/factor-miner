import json
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, Union
import io
from collections import deque

import pandas as pd
import pyarrow.parquet as pq

from src.core.constants import REQUIRED_COLUMNS, FileType


class CSVDataReader:
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)

    def validate(self) -> Tuple[bool, Optional[str]]:
        try:
            # read only header to validate required columns
            header_df = pd.read_csv(str(self.file_path), nrows=0)
            missing = [col for col in REQUIRED_COLUMNS if col not in header_df.columns]
            if missing:
                return False, f"Missing required columns: {', '.join(missing)}"
            return True, None
        except Exception as e:
            return False, str(e)

    def read_full(self) -> Optional[pd.DataFrame]:
        #read entire csv file into memory
        try:
            return pd.read_csv(str(self.file_path))
        except Exception:
            return None

    def read_preview(self, num_rows: int = 10) -> Optional[pd.DataFrame]:
        # read preview of csv file (first and last N rows) using deque for tail
        try:
            first_rows = pd.read_csv(str(self.file_path), nrows=num_rows)
            with self.file_path.open('r', newline='') as f:
                header_line = f.readline().rstrip('\r\n')
                tail_lines = deque(f, num_rows)
            if not tail_lines:
                return first_rows
            tail_csv = header_line + '\n' + ''.join(tail_lines)
            last_rows = pd.read_csv(io.StringIO(tail_csv))
            return pd.concat([first_rows, last_rows], ignore_index=True)
        except Exception:
            return None

    def get_metadata(self) -> Dict[str, Any]:
        # get metadata like number of rows, columns, etc.
        try:
            df = self.read_full()
            if df is None:
                return {'valid': False, 'error': 'Failed to read file'}
            unique_dates = None
            if 'Date' in df.columns:
                unique_dates = pd.to_datetime(df['Date']).nunique()
            return {
                'valid': True,
                'num_rows': len(df),
                'num_columns': len(df.columns),
                'columns': df.columns.tolist(),
                'unique_dates': unique_dates
            }
        except Exception as e:
            return {'valid': False, 'error': str(e)}


class ParquetDataReader:
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)

    def validate(self) -> Tuple[bool, Optional[str]]:
        try:
            parquet_file = pq.ParquetFile(str(self.file_path))
            columns = parquet_file.schema_arrow.names
            missing = [col for col in REQUIRED_COLUMNS if col not in columns]
            if missing:
                return False, f"Missing required columns: {', '.join(missing)}"
            return True, None
        except Exception as e:
            return False, str(e)

    def read_full(self) -> Optional[pd.DataFrame]:
        try:
            return pd.read_parquet(str(self.file_path))
        except Exception:
            return None

    def read_columns(self, columns: list) -> Optional[pd.DataFrame]:
        try:
            return pd.read_parquet(str(self.file_path), columns=columns)
        except Exception:
            return None

    def read_preview(self, num_rows: int = 10) -> Optional[pd.DataFrame]:
        try:
            parquet_file = pq.ParquetFile(str(self.file_path))
            total_rows = parquet_file.metadata.num_rows

            # if the whole file is less than N*2 (for example, 10 first and 10 last rows), return the whole file
            if total_rows <= num_rows * 2:
                return parquet_file.read().to_pandas()

            first_rows = next(parquet_file.iter_batches(num_rows=num_rows))

            # read last N rows from last row group
            last_row_group_idx = parquet_file.num_row_groups - 1
            last_batch = parquet_file.read_row_group(last_row_group_idx).to_pandas()
            last_rows = last_batch.tail(num_rows)
            last_rows.index = range(total_rows - num_rows, total_rows)
            return pd.concat([first_rows, last_rows], ignore_index=False)
        except Exception:
            return None

    def get_metadata(self) -> Dict[str, Any]:
        try:
            parquet_file = pq.ParquetFile(str(self.file_path))
            num_rows = parquet_file.metadata.num_rows
            columns = parquet_file.schema_arrow.names
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

    def get_column_names(self) -> list:
        try:
            parquet_file = pq.ParquetFile(str(self.file_path))
            return parquet_file.schema_arrow.names
        except Exception:
            return []

    def get_custom_metadata(self) -> Optional[Dict[str, str]]:
        try:
            parquet_file = pq.ParquetFile(str(self.file_path))
            schema_metadata = parquet_file.schema_arrow.metadata
            if schema_metadata is None:
                return None
            return {
                k.decode('utf-8'): v.decode('utf-8')
                for k, v in schema_metadata.items()
            }
        except Exception:
            return None

    def get_formulas_from_metadata(self) -> Optional[pd.DataFrame]:
        try:
            metadata = self.get_custom_metadata()
            if metadata is None or 'features' not in metadata:
                return None
            features_json = json.loads(metadata['features'])
            df = pd.DataFrame(features_json)
            for col in ['formula', 'name', 'tag']:
                if col not in df.columns:
                    df[col] = ''
            if 'Normalization' not in df.columns:
                df['Normalization'] = 'Raw'
            return df[['formula', 'name', 'tag', 'Normalization']]
        except Exception:
            return None


def get_data_reader(file_path: Union[str, Path], file_type: FileType) -> Union[CSVDataReader, ParquetDataReader]:
    reader_map = {FileType.CSV: CSVDataReader, FileType.PARQUET: ParquetDataReader}
    return reader_map[file_type](Path(file_path))


