from src.ui.steps.step0 import render as render_step0
from src.ui.steps.step1 import render as render_step1
from src.ui.steps.step2 import render as render_step2
from src.ui.steps.step3 import render as render_step3

_RENDERERS = {0: render_step0, 1: render_step1, 2: render_step2, 3: render_step3}


def render_step(step: int) -> None:
    if step in _RENDERERS:
        _RENDERERS[step]()
    else:
        _RENDERERS[1]()


__all__ = ['render_step', 'render_step0', 'render_step1', 'render_step2', 'render_step3']
