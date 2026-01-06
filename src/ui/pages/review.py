import streamlit as st
import pandas as pd

from src.core.context import get_state
from src.core.utils import format_date
from src.ui.components import (
    render_dataset_preview,
    render_dataset_statistics
)
from src.services.readers import ParquetDataReader
from src.services.processing import start_step2_analysis


def render() -> None:
    state = get_state()

    reader = ParquetDataReader(state.dataset_path)
    preview_df = reader.read_preview(num_rows=10)
    metadata = reader.get_metadata()

    if preview_df is not None and not preview_df.empty:
        dates = pd.to_datetime(preview_df['Date'])
        stats = {
            'num_rows': metadata.get('num_rows'),
            'num_columns': len(preview_df.columns),
            'num_dates': metadata.get('unique_dates'),
            'min_date': format_date(dates.min()),
            'max_date': format_date(dates.max()),
        }
        render_dataset_statistics(stats, state.benchmark_ticker)
        render_dataset_preview(preview_df)
    else:
        st.error("Unable to load dataset preview")

    with st.columns([4, 1])[1]:
        st.button("Run Analysis", type="primary", width="stretch", on_click=start_step2_analysis)
