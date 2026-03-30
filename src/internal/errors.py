import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.internal.links import p123_link


def format_analysis_error(error: str) -> str:
    fl_id = st.query_params.get("fl_id")
    msg = error.split("\n")[0]

    if "[column-not-found]" in msg:
        if INTERNAL_MODE:
            return f"{msg}\n\n[Add Missing]({p123_link(fl_id, "factors")}) | [Regenerate]({p123_link(fl_id, "generate")})"
        return f"{msg}\n\nEnsure your parquet file contains this column."

    if "[single-date]" in msg:
        if INTERNAL_MODE:
            return f"{msg}\n\nPlease [generate a new dataset]({p123_link(fl_id, "generate")}) using Period."
        return f"{msg}\n\nSingle-date datasets are not supported. Use a multi-period dataset."

    return f"Analysis failed: {msg}"
