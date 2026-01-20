import json
import os
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa
import pyarrow.parquet as pq


def update_parquet_metadata(path: Path, key: bytes, updates: Dict[str, Any]) -> None:
    table = pq.read_table(path)
    existing_meta = table.schema.metadata or {}

    current = json.loads(existing_meta.get(key, b"{}"))
    current.update(updates)

    new_meta = {**existing_meta, key: json.dumps(current).encode("utf-8")}
    pq.write_table(table.replace_schema_metadata(new_meta), path)

# when updating the description of the active dataset, don't change the modification time, it's used for matching the versioning with the backups
def update_active_dataset_metadata(
    path: Path, key: bytes, updates: Dict[str, Any]
) -> None:
    stat = os.stat(path)
    original_atime = stat.st_atime
    original_mtime = stat.st_mtime

    update_parquet_metadata(path, key, updates)

    os.utime(path, (original_atime, original_mtime))


def backup_parquet_metadata(source_path: str, dest_path: Path) -> None:
    # if backup already exists, don't back it up
    if dest_path.exists():
        return

    try:
        source_file = pq.ParquetFile(source_path)
        source_metadata = source_file.schema_arrow.metadata

        if source_metadata:
            table = pa.table({}).replace_schema_metadata(source_metadata)
            pq.write_table(table, dest_path)

    except Exception as e:
        print(f"Error backing up parquet metadata: {e}")
