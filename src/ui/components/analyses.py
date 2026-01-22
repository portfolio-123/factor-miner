import streamlit as st

from src.core.utils import format_date
from src.core.types import Analysis, AnalysisParams
from src.ui.constants import ANALYSIS_STATUS_COLORS, ANALYSIS_STATUS_COLORS_DEFAULT
from src.ui.components.common import section_header, render_info_item


def render_analysis_param(label: str, value: str) -> str:
    return f'<div class="analysis-card-param"><span class="label">{label}</span><span class="value">{value}</span></div>'


def render_analysis_params(params: AnalysisParams) -> None:
    param_config = [
        ("Min Alpha", params.min_alpha),
        ("Top X", params.top_pct),
        ("Bottom X", params.bottom_pct),
    ]
    items = [
        render_info_item(label, f"{value}%")
        for label, value in param_config
    ]
    section_header("Analysis Parameters")
    st.html(f'<div class="dataset-info-group">{"".join(items)}</div>')


def render_analysis_card(analysis: Analysis) -> None:
    formatted_date = format_date(analysis.created_at, "%b %d, %Y %H:%M:%S")
    status_bg, status_color = ANALYSIS_STATUS_COLORS.get(analysis.status, ANALYSIS_STATUS_COLORS_DEFAULT)

    params_html = "".join((
        render_analysis_param("Min Alpha", str(analysis.params.min_alpha)),
        render_analysis_param("Top X", f"{analysis.params.top_pct}%"),
        render_analysis_param("Bottom X", f"{analysis.params.bottom_pct}%"),
    ))

    card_html = f"""
    <div class="analysis-card-content">
        <div class="analysis-card-name">{analysis.name or "Untitled Analysis"}</div>
        <div class="analysis-card-params">{params_html}</div>
        <div class="analysis-card-right">
            <span class="analysis-card-date">{formatted_date}</span>
            <span class="analysis-card-status" style="background-color:{status_bg};color:{status_color};">{analysis.status}</span>
        </div>
    </div>
    <span class="analysis-card-trigger"></span>
    """
    st.html(card_html)

    if st.button("Open Analysis", key=f"analysis_btn_{analysis.id}", width="stretch"):
        st.switch_page("pages/results.py", query_params={"fl_id": analysis.fl_id, "id": analysis.id})
