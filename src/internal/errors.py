import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import ErrorType
from src.internal.links import p123_link


def format_analysis_error(error: str | None, error_type: ErrorType | None) -> str:
    fl_id = st.query_params["fl_id"]
    msg = error.split("\n")[0] if error else "Analysis failed"

    if INTERNAL_MODE:
        match error_type:
            case "missing-column":
                return f"Dataset is incompatible due to recent updates. Please [regenerate]({p123_link(fl_id, "generate")}) to analyze."
            case "single-date":
                return f'Single-date is not supported. Please [generate a new dataset]({p123_link(fl_id, "generate")}) using "Period".'
    return f"Analysis failed: {msg}"
