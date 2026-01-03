import streamlit as st
import pandas as pd

from src.core.context import update_state
from src.ui.components.tables import render_formulas_grid


def show_formulas_modal(formulas_df: pd.DataFrame) -> None:
    title = f"Dataset Formulas ({len(formulas_df)})"

    @st.dialog(title, width="large")
    def _render() -> None:
        render_formulas_grid(formulas_df)

    _render()

    update_state(formulas_ds_ver=None)
