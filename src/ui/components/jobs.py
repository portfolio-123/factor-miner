import streamlit as st

from src.core.utils import format_date
from src.core.types import Job
from src.core.job_restore import restore_job_state
from src.ui.constants import JOB_STATUS_COLORS, JOB_STATUS_COLORS_DEFAULT
from src.ui.components.common import section_header, render_info_item


def render_job_param(label: str, value: str) -> str:
    return f'<div class="job-card-param"><span class="label">{label}</span><span class="value">{value}</span></div>'


def render_analysis_params(analysis_params: dict) -> None:
    param_config = [
        ("Min Alpha", "min_alpha"),
        ("Top X", "top_x_pct"),
        ("Bottom X", "bottom_x_pct"),
    ]
    items = [
        render_info_item(label, f"{analysis_params[key]}%")
        for label, key in param_config
    ]

    section_header("Analysis Parameters")
    st.markdown(f'<div class="dataset-info-group">{"".join(items)}</div>', unsafe_allow_html=True)


def render_job_card(job: Job) -> None:
    formatted_date = format_date(job.created_at, "%b %d, %Y %H:%M:%S")
    status_bg, status_color = JOB_STATUS_COLORS.get(job.status, JOB_STATUS_COLORS_DEFAULT)

    params_html = "".join((
        render_job_param("Min Alpha", str(job.params.min_alpha)),
        render_job_param("Top X", f"{job.params.top_pct}%"),
        render_job_param("Bottom X", f"{job.params.bottom_pct}%"),
    ))

    card_html = f"""
    <div class="job-card-content">
        <div class="job-card-name">{job.name or "Untitled Analysis"}</div>
        <div class="job-card-params">{params_html}</div>
        <div class="job-card-right">
            <span class="job-card-date">{formatted_date}</span>
            <span class="job-card-status" style="background-color:{status_bg};color:{status_color};">{job.status}</span>
        </div>
    </div>
    <span class="job-card-trigger"></span>
    """
    st.markdown(card_html, unsafe_allow_html=True)

    if st.button("Open Analysis", key=f"job_btn_{job.id}", width="stretch"):
        if restore_job_state(job.id):
            st.rerun()
        st.error(f"Failed to load job {job.id}")
