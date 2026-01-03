from src.core.context import get_state
from src.ui.pages.history import render as _render_history_content
from src.ui.pages.settings import render as render_settings
from src.ui.pages.review import render as render_review
from src.ui.pages.results import render as _render_results_content
from src.ui.components import header_analysis, header_back, render_current_dataset


def render_history() -> None:
    _render_history_content()


def render_new_analysis() -> None:
    state = get_state()

    header_analysis()
    render_current_dataset()

    if state.current_step == 2:
        render_review()
    else:
        render_settings()


def render_results() -> None:
    header_back()
    render_current_dataset()
    _render_results_content()


__all__ = ['render_new_analysis', 'render_results', 'render_history']
