import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, Union
import pyarrow.parquet as pq


class CSVDataReader:
    def __init__(self, file_path: Union[str, Path]):
        self.file_path = Path(file_path)

    def validate(self) -> Tuple[bool, Optional[str]]:
        try:
            # read first row to check if required columns are present
            df = pd.read_csv(str(self.file_path), nrows=1)
            required_columns = ['Date', 'Ticker', 'Last Close']
            missing = [col for col in required_columns if col not in df.columns]

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
        # read preview of csv file (first and last N rows). currently using this in step 2, on the dataset inspect
        try:
            df = self.read_full()
            if df is None or len(df) <= num_rows * 2:
                return df

            first_rows = df.head(num_rows)
            last_rows = df.tail(num_rows)
            return pd.concat([first_rows, last_rows], ignore_index=False)
        except Exception:
            return None

    def get_metadata(self) -> Dict[str, Any]:
        # get metadata like number of rows, columns, etc. using in step 2
        try:
            df = self.read_full()
            if df is None:
                return {'valid': False, 'error': 'Failed to read file'}

            # Calculate unique dates from full dataset
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
            # read metadata only and get columns
            parquet_file = pq.ParquetFile(str(self.file_path))
            columns = parquet_file.schema_arrow.names

            required_columns = ['Date', 'Ticker', 'Last Close']
            missing = [col for col in required_columns if col not in columns]

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

            # read first N rows from first row group
            first_batch = parquet_file.read_row_group(0).to_pandas()
            first_rows = first_batch.head(num_rows)

            # read last N rows from last row group
            last_row_group_idx = parquet_file.num_row_groups - 1
            last_batch = parquet_file.read_row_group(last_row_group_idx).to_pandas()
            last_rows = last_batch.tail(num_rows)
            last_rows.index = range(total_rows - num_rows, total_rows)

            return pd.concat([first_rows, last_rows], ignore_index=False)
        except Exception:
            return None

    def get_metadata(self) -> Dict[str, Any]:
        # get metadata like number of rows, columns, etc. using in step 2
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
        # get list of column names from metadata only
        try:
            parquet_file = pq.ParquetFile(str(self.file_path))
            return parquet_file.schema_arrow.names
        except Exception:
            return []

    def get_custom_metadata(self) -> Optional[Dict[str, str]]:
        """Get custom key-value metadata from parquet file schema."""
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


def get_data_reader(file_path: Union[str, Path], file_type: Optional[str] = None):
    path = Path(file_path)

    # Use explicit file_type if provided, otherwise detect from extension
    if file_type is None:
        suffix = path.suffix.lower()
        if suffix == '.csv':
            file_type = 'csv'
        elif suffix == '.parquet':
            file_type = 'parquet'

    if file_type == 'csv':
        return CSVDataReader(path)
    elif file_type == 'parquet':
        return ParquetDataReader(path)
    else:
        raise ValueError(f"Unsupported file type: {path}")
