from src.core.types import HistoryPageData
from src.services.parquet_utils import get_file_version
from src.services.analysis_utils import group_analyses_by_version, sort_dataset_versions
from src.workers.manager import list_analyses


def get_history_data(fl_id: str, dataset_path: str | None) -> HistoryPageData | None:
    analyses_by_version = group_analyses_by_version(list_analyses(fl_id))

    all_versions = set(analyses_by_version.keys())
    active_version = get_file_version(dataset_path) if dataset_path else None
    if active_version:
        all_versions.add(active_version)

    versions = sort_dataset_versions(list(all_versions))

    if not versions:
        return None

    return HistoryPageData(
        versions=versions,
        analyses_by_version=analyses_by_version,
        active_version=active_version,
    )
