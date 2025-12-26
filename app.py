import os
import streamlit as st
from dotenv import load_dotenv

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import locate_factor_list_file
from src.ui.styles import apply_custom_styles
from src.ui.pages import history_page, analysis_page
from src.core.job_restore import restore_job_state
from src.core.auth import authenticate_user

load_dotenv()

st.set_page_config(
    page_title="Factor Evaluator - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)

st.markdown('''
<style>
.stApp [data-testid="stToolbar"]{
    display:none;
}
</style>
''', unsafe_allow_html=True)


def initialize_app() -> None:
    if "initialized" in st.session_state:
        return

    add_debug_log("Initializing application...")

    # check env variable to see if it's internal or external app.
    is_internal_app = os.getenv("INTERNAL_APP", "false").lower() == "true"
    state = get_state()
    state.is_internal_app = is_internal_app

    if is_internal_app:
        add_debug_log("Running in internal app mode")

        fl_id = st.query_params.get("fl_id", None)

        if fl_id:
            add_debug_log(f"Factor list ID from URL: {fl_id}")
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

            except (ValueError, FileNotFoundError) as e:
                update_state(page="history")
    else:
        # TODO: think how to handle external app. we don't know the dataset path until they add it in step 1 form. wait until the results redesign to support multiple results.
        add_debug_log("Running in external app mode")

    # to avoid re-initialization if it has already been done
    st.session_state.initialized = True
    add_debug_log("Application initialized")


def main() -> None:
    user_claims = authenticate_user()

    if not user_claims:
        st.stop()


    apply_custom_styles()

    initialize_app()

    state = get_state()

    if state.page == "history":
        history_page()
    else:
        analysis_page()


if __name__ == "__main__":
    main()
