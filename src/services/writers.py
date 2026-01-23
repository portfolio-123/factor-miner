import json
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa
import pyarrow.parquet as pq

from src.core.environment import FACTOR_LIST_DIR


def update_parquet_metadata(path: Path, key: bytes, updates: Dict[str, Any]) -> None:
    table = pq.read_table(path)
    existing_meta = table.schema.metadata or {}

    current = json.loads(existing_meta.get(key, b"{}"))
    current.update(updates)

    new_meta = {**existing_meta, key: json.dumps(current).encode("utf-8")}
    pq.write_table(table.replace_schema_metadata(new_meta), path)


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
