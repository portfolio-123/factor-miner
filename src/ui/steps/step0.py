import streamlit as st
from datetime import datetime
from collections import defaultdict
from src.core.context import get_state, update_state
from src.workers.manager import list_jobs
from src.core.job_restore import restore_job_state


def format_timestamp(ts_str: str) -> str:
    try:
        ts = float(ts_str)
        return datetime.fromtimestamp(ts).strftime("%b %d, %Y at %I:%M %p")
    except (ValueError, TypeError):
        return f"Version: {ts_str}"


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    # Compact header row with title and button vertically centered
    h_left, _, h_right = st.columns([3, 2, 1], vertical_alignment="center")
    with h_left:
        st.markdown(
            "<div style='font-size:24px;font-weight:700;color:#212529;margin:0;padding:0;'>"
            "Analysis History"
            "</div>",
            unsafe_allow_html=True,
        )
    with h_right:
        st.button(
            "New Analysis",
            type="primary",
            use_container_width=True,
            on_click=lambda: update_state(current_step=1),
        )
    
    # if not fl_id:
    #     st.info("No Factor List ID found. Starting new analysis.")
    #     if st.button("Start New Analysis"):
    #         update_state(current_step=1)
    #         st.rerun()
    #     return

    jobs = list_jobs(fl_id)
    
    if not jobs:
        st.info("No past analysis found for this Factor List.")
        return

    # group jobs by dataset version
    grouped_jobs = defaultdict(list)
    for job in jobs:
        grouped_jobs[job['dataset_version']].append(job)

    # newest dataset first
    sorted_datasets = sorted(grouped_jobs.keys(), key=lambda x: float(x) if x.replace('.','',1).isdigit() else 0, reverse=True)

    for ds_ver in sorted_datasets:
        ds_jobs = grouped_jobs[ds_ver]
        
        ds_label = format_timestamp(ds_ver)
        
        st.markdown(
            f"""
            <div style="
                background-color: #f8f9fa; 
                padding: 10px 15px; 
                border-radius: 6px; 
                border-left: 4px solid #2196F3;
                margin-top: 20px;
                margin-bottom: 15px;
            ">
                <span style="font-weight: 600; color: #333; font-size: 16px;">Dataset Version:</span>
                <span style="color: #555; font-size: 16px; margin-left: 8px;">{ds_label}</span>
            </div>
            """, 
            unsafe_allow_html=True
        )
        
        for job in ds_jobs:
            render_job_card(job)
        
        st.markdown("<div style='margin-bottom: 30px;'></div>", unsafe_allow_html=True)


def render_job_card(job: dict) -> None:
    created_at = datetime.fromisoformat(job["created_at"])
    formatted_date = created_at.strftime("%b %d, %Y %H:%M:%S")
    status = job["status"]
    job_id = job["id"]
    params = job.get("params", {})

    min_alpha = params.get("min_alpha", "N/A")
    top_pct = params.get("top_pct", "N/A")
    bottom_pct = params.get("bottom_pct", "N/A")

    if status == "completed":
        status_bg, status_color = "#e6f4ea", "#1e8e3e"
    elif status in ("running", "pending"):
        status_bg, status_color = "#fff0b3", "#b06000"
    else:
        status_bg, status_color = "#fce8e6", "#c5221f"

    with st.container(border=True):
        h_left, h_right = st.columns([4, 1])
        with h_left:
            st.markdown(
                f"<div style='font-size:16px;font-weight:600;color:#212529;'>{formatted_date}</div>",
                unsafe_allow_html=True,
            )
        with h_right:
            st.markdown(
                f"""
                <div style="text-align:right;">
                    <span style="
                        background-color:{status_bg};
                        color:{status_color};
                        padding:3px 9px;
                        border-radius:12px;
                        font-size:11px;
                        font-weight:600;
                        text-transform:uppercase;
                        letter-spacing:0.4px;
                    ">
                        {status}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        m1, m2, m3, _ = st.columns([2, 2, 2, 3])
        with m1:
            st.markdown(
                "<div style='font-size:11px;color:#888;text-transform:uppercase;'>Min Alpha</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:14px;font-weight:500;color:#333;'>{min_alpha}%</div>",
                unsafe_allow_html=True,
            )
        with m2:
            st.markdown(
                "<div style='font-size:11px;color:#888;text-transform:uppercase;'>Top X</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:14px;font-weight:500;color:#333;'>{top_pct}%</div>",
                unsafe_allow_html=True,
            )
        with m3:
            st.markdown(
                "<div style='font-size:11px;color:#888;text-transform:uppercase;'>Bottom X</div>",
                unsafe_allow_html=True,
            )
            st.markdown(
                f"<div style='font-size:14px;font-weight:500;color:#333;'>{bottom_pct}%</div>",
                unsafe_allow_html=True,
            )

        _, btn_col = st.columns([4, 1])
        with btn_col:
            if st.button("Open", key=f"open_{job_id}", type="primary", use_container_width=True):
                if restore_job_state(job_id):
                    st.rerun()
                else:
                    st.error("Failed.")
