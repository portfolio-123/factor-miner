from typing import Optional

from src.core.context import get_state
from src.services.readers import ParquetDataReader
from src.services.parquet_utils import get_dataset_file_path, get_file_version
from src.ui.components import render_dataset_header, render_analysis_params


def render_current_dataset_header() -> None:
    state = get_state()

    path: Optional[str] = None
    ds_ver: Optional[str] = None

    # if this is a results or new analysis page, try to load from job backup
    if state.current_job_id:
        try:
            parts = state.current_job_id.split("/")
            if len(parts) >= 2:
                ds_ver = parts[1]
                backup_path = get_dataset_file_path(state.factor_list_uid, ds_ver)
                if backup_path.exists():
                    path = str(backup_path)
        except Exception:
            pass

    # fallback to live dataset
    if not path and state.dataset_path:
        try:
            path = state.dataset_path
            ds_ver = get_file_version(path)
        except Exception:
            pass

    dataset_info = ParquetDataReader(path).get_dataset_info()
    render_dataset_header(dataset_info, ds_ver)

    if state.page == "results":
        analysis_params = {
            "min_alpha": state.min_alpha,
            "top_x_pct": state.top_x_pct,
            "bottom_x_pct": state.bottom_x_pct,
        }
        render_analysis_params(analysis_params)
