import os

import streamlit as st
from dotenv import load_dotenv

from src.core.context import get_state, update_state, add_debug_log
from src.services.processing import load_formulas_data
from src.core.utils import get_url_params, locate_factor_list_files, deserialize_dataframe
from src.workers.manager import read_job
from src.ui.components import header_with_navigation
from src.ui.steps import render_step1, render_step2, render_step3
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

        fl_id, benchmark = get_url_params('fl_id', 'benchmark')

        if fl_id:
            add_debug_log(f"Factor list ID from URL: {fl_id}")
            update_state(factor_list_uid=fl_id)

            job_data = read_job(fl_id)
            if job_data:
                params = job_data['params']
                state = get_state()

                dataset_path, _, file_error, file_type = locate_factor_list_files(fl_id)

                if job_data['status'] in ('pending', 'running'):
                    # Job still running - restore step 2 state that shows progress
                    add_debug_log(f"Found running job for {fl_id}, status: {job_data['status']}")

                    formulas_data = load_formulas_data(
                        str(dataset_path),
                        file_type,
                        fl_id
                    ) if dataset_path else None

                    state.completed_steps.add(1)
                    update_state(
                        current_job_id=fl_id,
                        current_step=2,
                        dataset_path=dataset_path,
                        file_type=file_type,
                        benchmark_ticker=params.get('benchmark_ticker'),
                        formulas_data=formulas_data,
                    )

                elif job_data['status'] == 'completed':
                    add_debug_log(f"Found completed job for {fl_id}, loading results")

                    try:
                        results = job_data['results']
                        metrics_df = deserialize_dataframe(results['all_metrics'])
                        corr_matrix = deserialize_dataframe(results['all_corr_matrix'])

                        # Also restore formulas_data for step 2 navigation
                        formulas_data = load_formulas_data(
                            str(dataset_path),
                            file_type,
                            fl_id
                        ) if dataset_path else None

                        state.completed_steps.add(1)
                        state.completed_steps.add(2)
                        state.completed_steps.add(3)
                        update_state(
                            current_step=3,
                            dataset_path=dataset_path,
                            file_type=file_type,
                            benchmark_ticker=params.get('benchmark_ticker'),
                            formulas_data=formulas_data,
                            all_metrics=metrics_df,
                            all_corr_matrix=corr_matrix,
                            min_alpha=params.get('min_alpha', 0.5),
                            top_x_pct=params.get('top_pct', 20.0),
                            bottom_x_pct=params.get('bottom_pct', 20.0),
                        )
                    except Exception as e:
                        add_debug_log(f"Error loading completed job results: {e}")

            dataset_path, formulas_path, error, file_type = locate_factor_list_files(fl_id)

            if error:
                add_debug_log(f"File verification error: {error}")
                update_state(
                    files_verified=False,
                    files_verification_error=error
                )
            else:
                add_debug_log(f"Files verified - Dataset: {dataset_path}, Formulas: {formulas_path}")
                update_state(
                    auto_dataset_path=dataset_path,
                    auto_formulas_path=formulas_path,
                    auto_dataset_file_type=file_type,
                    files_verified=True,
                    files_verification_error=None
                )

        if benchmark:
            add_debug_log(f"Benchmark from URL: {benchmark}")
            update_state(benchmark_ticker=benchmark)
    else:
        add_debug_log("Running in external app mode")

    st.session_state.initialized = True
    add_debug_log("Application initialized")


def main() -> None:
    apply_custom_styles()

    initialize_app()

    state = get_state()

    selected_step = header_with_navigation()

    if selected_step != state.current_step:
        available_steps = [1]
        if 1 in state.completed_steps:
            available_steps.append(2)
        if 2 in state.completed_steps:
            available_steps.append(3)

        if selected_step in available_steps:
            update_state(current_step=selected_step)
            st.rerun()

    if state.current_step == 1:
        render_step1()
    elif state.current_step == 2:
        render_step2()
    elif state.current_step == 3:
        render_step3()


if __name__ == "__main__":
    main()
