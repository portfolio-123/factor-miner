"""Analysis-related utility functions for grouping and sorting."""

import logging
from collections import defaultdict
from typing import List, Dict

from src.core.types import Analysis

logger = logging.getLogger(__name__)


def sort_dataset_versions(versions: List[str]) -> List[str]:
    return sorted(versions, key=lambda v: int(v) if v.isdigit() else float('inf'))


def group_analyses_by_version(analyses: List[Dict]) -> Dict[str, List[Analysis]]:
    grouped: Dict[str, List[Analysis]] = defaultdict(list)

    for analysis in analyses:
        ds_ver = analysis.get("dataset_version")
        if ds_ver:
            try:
                grouped[ds_ver].append(Analysis(**analysis))
            except Exception:
                logger.warning(f"Failed to parse analysis: {analysis.get('id')}")
                continue

    return dict(grouped)
