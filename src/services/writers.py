import json
from pathlib import Path
from typing import Any, Dict

import pyarrow.parquet as pq


def update_parquet_metadata(path: Path, key: bytes, updates: Dict[str, Any]) -> None:
    """Read parquet, update a JSON metadata key, and write back."""
    table = pq.read_table(path)
    existing_meta = table.schema.metadata or {}

    current = json.loads(existing_meta.get(key, b"{}"))
    current.update(updates)

    new_meta = {**existing_meta, key: json.dumps(current).encode("utf-8")}
    pq.write_table(table.replace_schema_metadata(new_meta), path)
