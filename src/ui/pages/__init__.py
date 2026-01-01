from src.core.context import get_state, update_state
from src.ui.pages.history import render as render_history
from src.ui.pages.settings import render as render_settings
from src.ui.pages.review import render as render_review
from src.ui.pages.results import render as _render_results_content
from src.ui.components import (
    header_with_navigation,
    header_simple_back,
    show_formulas_modal,
)
from src.ui.header import render_current_dataset_header
from src.workers.manager import get_formulas_df_for_version


def _render_formulas_modal_if_needed() -> None:
    state = get_state()
    if state.formulas_ds_ver:
        formulas_df = get_formulas_df_for_version(
            state.factor_list_uid, state.formulas_ds_ver
        )
        if formulas_df is not None:
            show_formulas_modal(formulas_df)
        else:
            update_state(formulas_ds_ver=None)


def render_new_analysis() -> None:
    state = get_state()

    _render_formulas_modal_if_needed()

    if state.current_job_id:
        header_simple_back()
    else:
        header_with_navigation()

    render_current_dataset_header()

    if state.current_step == 2:
        render_review()
    else:
        render_settings()


def render_results() -> None:
    _render_formulas_modal_if_needed()

    header_simple_back()
    render_current_dataset_header()

    _render_results_content()


__all__ = ['render_new_analysis', 'render_results', 'render_history']
