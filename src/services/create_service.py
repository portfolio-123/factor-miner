import uuid
import streamlit as st
from core.environment import FACTOR_LIST_DIR
from src.core.types import AnalysisParams
from src.workers.manager import start_analysis
from src.services.dataset_service import get_file_mtime


def submit_analysis_creation() -> None:
    fl_id = st.query_params.get("fl_id")

    dataset_path = str(FACTOR_LIST_DIR / fl_id)
    dataset_version = get_file_mtime(dataset_path)
    analysis_id = uuid.uuid4().hex[:8]

    try:
        params = AnalysisParams(
            benchmark_ticker=st.session_state.get("benchmark_ticker"),
            min_alpha=st.session_state.get("min_alpha"),
            top_pct=st.session_state.get("top_pct"),
            bottom_pct=st.session_state.get("bottom_pct"),
            active_dataset_file=dataset_path,
            access_token=st.session_state.get("access_token"),
        )
        start_analysis(fl_id, analysis_id, dataset_version, params)
        st.session_state["_redirect_to_results"] = {
            "fl_id": fl_id,
            "analysis_id": analysis_id,
        }
    except Exception as e:
        st.toast(f"Error starting analysis: {e}")
