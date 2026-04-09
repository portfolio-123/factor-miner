import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.internal.links import p123_link


def format_analysis_error(error: str) -> str:
    fl_id = st.query_params.get("fl_id")
    assert fl_id
    msg = error.split("\n")[0]

    if "missing required columns" in msg:
        if INTERNAL_MODE:
            return f'Dataset is not prepared for analyses. [Generate a new dataset]({p123_link(fl_id, "generate")}) toggling on "Prepare for analysis".'

    if "Single-date" in msg:
        if INTERNAL_MODE:
            return f'Single-date is not supported. Please [generate a new dataset]({p123_link(fl_id, "generate")}) using "Period".'
        return msg

    return f"Analysis failed: {msg}"
