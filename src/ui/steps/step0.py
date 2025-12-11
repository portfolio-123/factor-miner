import streamlit as st
from datetime import datetime
from collections import defaultdict
from src.core.context import get_state, update_state
from src.workers.manager import list_jobs, get_dataset_info_from_backup
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

    if not fl_id:
        st.warning("No Factor List selected. Please select a Factor List to view analysis history.")
        return

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
            on_click=lambda: update_state(page="analysis", current_step=1, current_job_id=None),
        )

    # Reduce vertical padding on bordered containers
    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlockBorderWrapper"] > div {
            padding-top: 0.5rem;
            padding-bottom: 0.5rem;
        }
        /* Reduce divider margins */
        div[data-testid="stElementContainer"]:has(hr) {
            margin-top: 0 !important;
            margin-bottom: 0 !important;
        }
        hr {
            margin: 0.25rem 0 0.5rem 0 !important;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

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
        
        dataset_info = get_dataset_info_from_backup(fl_id, ds_ver)

        dataset_info = {
            "universeName": "SP500 Universe",
            "frequency": 1,  # Weekly
            "scaling": "Z-Score",
            "currency": "USD",
            "benchmark": "SPY:USA",
            "normalization": {
                "scaling": "Z-Score",
                "scope": "Dataset",
                "trimPct": 2.5,
                "outliers": True,
                "outlierLimit": 3.0
            },
        }

        if dataset_info:
            universe = dataset_info.get("universeName", "Unknown Universe")
            
            freq_val = dataset_info.get("frequency", 1)
            freq_map = {1: "Weekly", 4: "Monthly", 252: "Daily"}
            frequency = freq_map.get(freq_val, f"Freq: {freq_val}")

            normalization = dataset_info.get("normalization", {})
            has_normalization = bool(normalization) and isinstance(normalization, dict)

            # Extract normalization details
            norm_scaling = normalization.get("scaling", "None") if has_normalization else "None"
            norm_scope = normalization.get("scope", "N/A") if has_normalization else "N/A"
            norm_trim_pct = normalization.get("trimPct", 0.0) if has_normalization else 0.0
            norm_outliers = normalization.get("outliers", False) if has_normalization else False
            norm_outlier_limit = normalization.get("outlierLimit", 0.0) if has_normalization else 0.0

            currency = dataset_info.get("currency", "USD")
            precision = dataset_info.get("precision", "2")
            ds_label = format_timestamp(ds_ver)

            with st.container(border=True):
                st.markdown(
                    f'''<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                        <div style="display: flex; align-items: center; gap: 8px; font-size: 20px; font-weight: 600;">
                            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#60646A" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                            {universe}
                        </div>
                        <span style="font-size: 14px; color: #888; font-weight: 400;">{ds_label}</span>
                    </div>''',
                    unsafe_allow_html=True
                )

                col_main, col_norm = st.columns([3, 2])

                with col_main:
                    st.markdown(
                        f"""
                        <div style="display: flex; gap: 32px;">
                            <div>
                                <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Frequency</div>
                                <div style="font-size: 14px; font-weight: 500; color: #212529;">{frequency}</div>
                            </div>
                            <div>
                                <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Currency</div>
                                <div style="font-size: 14px; font-weight: 500; color: #212529;">{currency}</div>
                            </div>
                            <div>
                                <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Precision</div>
                                <div style="font-size: 14px; font-weight: 500; color: #212529;">{precision}</div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                with col_norm:
                    if has_normalization:
                        outlier_text = f" · Outlier Limit: {norm_outlier_limit}" if norm_outliers else ""
                        st.markdown(
                            f"""
                            <div style="background: #f8f9fa; border-radius: 6px; padding: 8px 12px;">
                                <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Normalization</div>
                                <div style="font-size: 12px; color: #495057; line-height: 1.4;">
                                    {norm_scaling} · {norm_scope.title()} · Trim: {norm_trim_pct}%{outlier_text}
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                    else:
                        st.markdown(
                            """
                            <div style="background: #f8f9fa; border-radius: 6px; padding: 8px 12px; border-left: 3px solid #adb5bd;">
                                <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Normalization</div>
                                <div style="font-size: 12px; color: #6c757d;">None</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                st.divider()

                st.markdown("<div style='font-size: 15px; font-weight: 400; color: #60646A; margin-bottom: 10px;'>PAST ANALYSES</div>", unsafe_allow_html=True)

                for job in ds_jobs:
                    render_job_card(job)


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
                <div style="text-align:right; margin-bottom: 25px;">
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

        col_metrics, col_btn = st.columns([5, 1], vertical_alignment="bottom")
        
        with col_metrics:
            st.markdown(
                f"""
                <div style="display: flex; gap: 40px; align-items: flex-end; padding-bottom: 12px;">
                    <div>
                        <div style="font-size:11px;color:#888;text-transform:uppercase;margin-bottom:6px;">Min Alpha</div>
                        <div style="font-size:14px;font-weight:500;color:#333;line-height:1;">{min_alpha}%</div>
                    </div>
                    <div>
                        <div style="font-size:11px;color:#888;text-transform:uppercase;margin-bottom:6px;">Top X</div>
                        <div style="font-size:14px;font-weight:500;color:#333;line-height:1;">{top_pct}%</div>
                    </div>
                    <div>
                        <div style="font-size:11px;color:#888;text-transform:uppercase;margin-bottom:6px;">Bottom X</div>
                        <div style="font-size:14px;font-weight:500;color:#333;line-height:1;">{bottom_pct}%</div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
        with col_btn:
            if st.button("Open", key=f"open_{job_id}", type="primary", use_container_width=True):
                if restore_job_state(job_id):
                    st.rerun()
                else:
                    st.error("Failed.")
