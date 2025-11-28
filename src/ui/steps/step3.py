import streamlit as st

from src.core.context import get_state, update_state, add_debug_log
from src.ui.components import section_header, render_results_table
from src.logic.calculations import select_best_features


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

    # update app state if widget values changed
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

    render_results_table(best_features, state.all_metrics)
