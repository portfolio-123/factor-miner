from src.core.context import get_state
from src.services.dataset_service import list_versions


def _get_version_sort_key(version: str) -> int:
    if "_" in version:
        parts = version.rsplit("_", 1)
        if parts[0].isdigit():
            return int(parts[0])
    return float("inf")


def _sort_dataset_versions(versions: list[str]) -> list[str]:
    return sorted(versions, key=_get_version_sort_key)


def get_history_data() -> list[str] | None:
    state = get_state()
    versions = list_versions(state.factor_list_uid)

    if not versions and not state.active_dataset_file:
        return None

    return (["active"] if state.active_dataset_file and not state.active_backup_version else []) + _sort_dataset_versions(versions)
