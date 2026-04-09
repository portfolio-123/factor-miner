import streamlit as st

from core.types.models import ErrorType
from src.core.config.environment import INTERNAL_MODE
from src.internal.links import p123_link


def format_analysis_error(error: str | None, error_type: ErrorType | None) -> str:
    fl_id = st.query_params["fl_id"]
    msg = error.split("\n")[0] if error else "Analysis failed"

    match error_type:
        case "missing-column":
            if INTERNAL_MODE:
                return f'Dataset is not prepared for analysis. [Generate a new dataset]({p123_link(fl_id, "generate")}) toggling on "Prepare for analysis".'
        case "single-date":
            if INTERNAL_MODE:
                return f'Single-date is not supported. Please [generate a new dataset]({p123_link(fl_id, "generate")}) using "Period".'
    return f"Analysis failed: {msg}"
