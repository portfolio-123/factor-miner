import os

import streamlit as st
from dotenv import load_dotenv

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import locate_factor_list_file
from src.core.job_restore import restore_job_state
from src.ui.components import header_with_navigation
from src.ui.steps import render_step
from src.ui.styles import apply_custom_styles

load_dotenv()

st.set_page_config(
    page_title="Factor Evaluator - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)

def initialize_app() -> None:
    if 'initialized' in st.session_state:
        return

    add_debug_log("Initializing application...")

    # check env variable to see if it's internal or external app
    is_internal_app = os.getenv('INTERNAL_APP', 'false').lower() == 'true'
    update_state(is_internal_app=is_internal_app)

    if is_internal_app:
        add_debug_log("Running in internal app mode")

        fl_id = st.query_params.get('fl_id', None)

        if fl_id:
            add_debug_log(f"Factor list ID from URL: {fl_id}")
            update_state(factor_list_uid=fl_id)

            try:
                dataset_path = locate_factor_list_file(fl_id)
                update_state(dataset_path=dataset_path)
                restore_job_state(fl_id)
            except (ValueError, FileNotFoundError) as e:
                add_debug_log(f"File verification failed: {e}")
    else:
        #TODO: think how to handle external app. we don't know the dataset path until they add it in step 1 form. wait until the results redesign to support multiple results.
        add_debug_log("Running in external app mode")

    # to avoid re-initialization if it has already been done
    st.session_state.initialized = True
    add_debug_log("Application initialized")


def main() -> None:
    apply_custom_styles()

    initialize_app()

    state = get_state()

    header_with_navigation()

    render_step(state.current_step)


if __name__ == "__main__":
    main()
