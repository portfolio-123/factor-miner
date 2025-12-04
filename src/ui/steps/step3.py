import streamlit as st
import streamlit.components.v1 as components

from src.core.context import get_state, update_state, add_debug_log
from src.ui.components import section_header, render_results_table
from src.core.calculations import select_best_features
from src.workers.manager import delete_job


def render() -> None:
    state = get_state()

    section_header("Filter Parameters")

    # initialize widget keys with state values if not set
    if "filter_correlation" not in st.session_state:
        st.session_state.filter_correlation = state.correlation_threshold
    if "filter_n_features" not in st.session_state:
        st.session_state.filter_n_features = state.n_features

    col1, col2, _ = st.columns([1, 1, 2])

    with col1:
        correlation_threshold = st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            key="filter_correlation",
            step=0.05,
        )

    with col2:
        n_features = st.number_input(
            "Number of Features",
            min_value=1,
            max_value=100,
            key="filter_n_features",
            step=1,
        )

    if (correlation_threshold != state.correlation_threshold or
            n_features != state.n_features):
        update_state(
            correlation_threshold=correlation_threshold,
            n_features=n_features
        )
        add_debug_log(f"Parameters updated - Correlation: {correlation_threshold}, N: {n_features}")

    # calculate best features again with new filter parameters
    best_features = select_best_features(
        state.all_metrics,
        state.all_corr_matrix,
        N=n_features,
        correlation_threshold=correlation_threshold,
        a_min=state.min_alpha
    )

    section_header("Best Performing Factors")

    display_df = render_results_table(best_features, state.all_metrics, state.formulas_data)

    if display_df is not None and not display_df.empty:
        col1, _, col3, col4 = st.columns([1, 1, 1, 1])

        # tab delimited for copy to clipboard
        csv_to_copy = display_df.to_csv(index=False, sep='\t')
        # comma delimited for file download
        csv_to_download = display_df.to_csv(index=False)

        with col1:
            if  st.button("New Analysis", type="tertiary", use_container_width=True) and state.factor_list_uid:
                delete_job(state.factor_list_uid)
                add_debug_log(f"Deleted job {state.factor_list_uid} for new analysis")

                # Reset state to step 1
                state.completed_steps.clear()
                state.completed_steps.add(1)
                update_state(
                    current_step=1,
                    current_job_id=None,
                    all_metrics=None,
                    all_corr_matrix=None,
                )
                st.rerun()
        
        with col3:
            components.html(f"""
                <button onclick="navigator.clipboard.writeText('{csv_to_copy}')">
                    Copy to Clipboard
                </button>
            """, height=0, width=0)

        with col4:
            st.download_button(
                type="primary",
                label="Download CSV",
                data=csv_to_download,
                file_name=f"{state.factor_list_uid}_best_features.csv",
                mime="text/csv",
                use_container_width=True
            )
        
       
