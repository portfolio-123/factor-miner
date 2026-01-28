from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.core.environment import FACTOR_LIST_DIR


def backup_parquet_metadata(fl_id: str, dest_path: Path) -> None:
    source_path = FACTOR_LIST_DIR / fl_id
    try:
        source_file = pq.ParquetFile(source_path)
        source_metadata = source_file.schema_arrow.metadata
        if source_metadata:
            table = pa.table({}).replace_schema_metadata(source_metadata)
            pq.write_table(table, dest_path)
    except Exception as e:
        print(f"Error backing up parquet metadata: {e}")
