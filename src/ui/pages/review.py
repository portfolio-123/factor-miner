import streamlit as st
import pandas as pd

from src.core.context import get_state, update_state
from src.core.utils import format_date
from src.ui.components import (
    render_formulas_grid,
    render_dataset_preview,
    render_dataset_statistics
)
from src.services.readers import ParquetDataReader
from src.services.processing import start_step2_analysis


def _on_run_analysis() -> None:
    update_state(step2_error=None)
    _, error = start_step2_analysis()
    if error:
        update_state(step2_error=error)


def render() -> None:
    state = get_state()

    reader = ParquetDataReader(state.dataset_path)
    preview_df = reader.read_preview(num_rows=10)
    metadata = reader.get_metadata()

    if preview_df is not None and not preview_df.empty:
        actual_row_count = metadata.get('num_rows', len(preview_df))
        unique_dates = metadata.get('unique_dates')
        dates = pd.to_datetime(preview_df['Date'])
        stats = {
            'num_rows': actual_row_count,
            'num_columns': len(preview_df.columns),
            'num_dates': unique_dates if unique_dates is not None else dates.nunique(),
            'min_date': format_date(dates.min()),
            'max_date': format_date(dates.max()),
        }
        render_dataset_statistics(stats, state.benchmark_ticker)
    else:
        st.error("Unable to load dataset preview and statistics. Showing available metadata only.")

    tab1, tab2 = st.tabs(["Formulas", "Dataset Preview"])

    with tab1:
        if state.formulas_data is not None:
            render_formulas_grid(state.formulas_data)
        else:
            st.info("No formulas data available")

    with tab2:
        if preview_df is not None and not preview_df.empty:
            render_dataset_preview(preview_df)
        else:
            st.warning("Preview unavailable")

    state = get_state()
    if state.step2_error:
        st.error(state.step2_error)

    with st.columns([4, 1])[1]:
        st.button("Run Analysis", type="primary", use_container_width=True, on_click=_on_run_analysis)
