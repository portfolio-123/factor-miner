import os
import streamlit as st
from dotenv import load_dotenv

from src.services.readers import get_current_dataset_info
from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import locate_factor_list_file
from src.ui.components import render_breadcrumb
from src.ui.styles import apply_custom_styles
from src.ui.pages import history_page, analysis_page
from src.core.job_restore import restore_job_state
from src.core.auth import authenticate_user

load_dotenv()

st.set_page_config(
    page_title="Factor Miner - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)


def initialize_app() -> None:
    state = get_state()
    fl_id = st.query_params.get("fl_id", None)

    if not fl_id or state.dataset_path:
        return

    update_state(factor_list_uid=fl_id)

    try:
        dataset_path = locate_factor_list_file(fl_id)
        update_state(dataset_path=dataset_path)

        qp_job_id = st.query_params.get("job_id")
        qp_step = st.query_params.get("step")
        is_new_analysis = st.query_params.get("new_analysis")

        restored = False

        if qp_job_id:
            if restore_job_state(qp_job_id):
                restored = True
                update_state(page="analysis")

                if qp_step:
                    try:
                        update_state(current_step=int(qp_step))
                    except ValueError:
                        pass
        elif is_new_analysis:
            update_state(page="analysis", current_step=1)
            restored = True

        if not restored:
            update_state(page="history")

    except (ValueError, FileNotFoundError):
        update_state(page="history")


def main() -> None:

    # this displays error messages and stops the execution below if unauthorized
    authenticate_user()
    
    apply_custom_styles()

    initialize_app()

    state = get_state()

    steps = [
        (
            "Factor List",
            f"{os.getenv("P123_BASE_URL")}/sv/factorList/{st.query_params.get("fl_id")}/download",
        ),
        ("FactorMiner", None),
    ]

    _, dataset_info = get_current_dataset_info(st.query_params.get("fl_id"))

    render_breadcrumb(steps)
    st.title(
        f"{dataset_info.flName if dataset_info else 'Unknown'} ({st.query_params.get("fl_id")})"
    )

    if state.page == "history":
        history_page()
    else:
        analysis_page()


if __name__ == "__main__":
    main()
