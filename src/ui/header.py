import os
from typing import Optional

from src.core.context import get_state
from src.services.readers import ParquetDataReader
from src.workers.manager import get_dataset_info_from_backup
from src.ui.components import render_dataset_header, render_analysis_params
from src.core.types import DatasetConfig


def render_current_dataset_header() -> None:
    state = get_state()

    dataset_info: Optional[DatasetConfig] = None
    ds_ver: Optional[str] = None
    fl_id: Optional[str] = None

    # try to load from existing job backup
    if state.current_job_id:
        try:
            parts = state.current_job_id.split("/")
            if len(parts) >= 2:
                fl_id = parts[0]
                ds_ver = parts[1]
                dataset_info = get_dataset_info_from_backup(fl_id, ds_ver)
        except Exception:
            pass

    # fallback to live dataset if no backup info found
    if not dataset_info and state.dataset_path:
        try:
            ds_ver = str(int(os.path.getmtime(state.dataset_path)))
            fl_id = state.factor_list_uid
            dataset_info = ParquetDataReader(state.dataset_path).get_dataset_info()
        except Exception:
            pass

    if dataset_info and ds_ver:
        # Render dataset header (reusable card)
        render_dataset_header(dataset_info, ds_ver, fl_id)

        # Render analysis params separately (only on results page)
        if state.page == "results":
            analysis_params = {
                "min_alpha": state.min_alpha,
                "top_x_pct": state.top_x_pct,
                "bottom_x_pct": state.bottom_x_pct,
            }
            render_analysis_params(analysis_params)
