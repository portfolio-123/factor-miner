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

    h_left, _, h_right = st.columns([3, 2, 1], vertical_alignment="center")
    with h_left:
        st.markdown(
            "<div style='font-size:24px;font-weight:700;color:#212529;margin:0;padding:0;'>"
            "Factor Evaluator - Analysis History"
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
        
        /* Job card link styling */
        a.job-card-link {
            display: block !important;
            text-decoration: none !important;
            color: inherit !important;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            margin-bottom: 8px;
            transition: all 0.2s ease;
            background-color: white;
        }
        a.job-card-link:hover {
            border-color: #2196F3 !important;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
            transform: translateY(-2px);
            text-decoration: none !important;
            color: inherit !important;
        }
        /* Ensure content inside link inherits color */
        a.job-card-link * {
            color: inherit;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # Check for job selection via query param (handling click from card)
    if "select_job_id" in st.query_params:
        job_id = st.query_params["select_job_id"]
        # Clear the param to prevent re-triggering
        del st.query_params["select_job_id"]
        _handle_job_click(job_id)
        st.rerun()

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
            "startDate": "2020-01-01",
            "endDate": "2024-12-31",
            "precision": 2,
            "normalization": {
                "scaling": "Z-Score",
                "scope": "Dataset",
                "trimPct": 2.5,
                "outliers": True,
                "outlierLimit": 3.0,
                "precision": 2
            },
        }

        if dataset_info:
            universe = dataset_info.get("universeName", "Unknown Universe")
            
            freq_val = dataset_info.get("frequency", 1)
            freq_map = {1: "Weekly", 4: "Monthly", 252: "Daily"}
            frequency = freq_map.get(freq_val, f"Freq: {freq_val}")

            normalization = dataset_info.get("normalization", None)
            if normalization and isinstance(normalization, dict):
                norm_scaling = normalization.get("scaling")
                norm_scope = normalization.get("scope")
                norm_trim_pct = normalization.get("trimPct")
                norm_outliers = normalization.get("outliers", False)
                norm_outlier_limit = normalization.get("outlierLimit")
                norm_precision = normalization.get("precision")

            currency = dataset_info.get("currency", "USD")
            ds_label = format_timestamp(ds_ver)

            start_date = dataset_info.get("startDate") or "N/A"
            end_date = dataset_info.get("endDate") or "N/A"

            # Get benchmark
            benchmark = dataset_info.get("benchmark", "N/A") or "N/A"

            with st.container(border=True):
                # Header row: Universe name with currency badge + date label
                st.markdown(
                    f'''<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <div style="display: flex; align-items: center; gap: 10px;">
                            <div style="display: flex; align-items: center; gap: 8px; font-size: 20px; font-weight: 600;">
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#2196F3" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                                {universe}
                            </div>
                            <span style="background: #dbeafe; color: #1d4ed8; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;">{currency}</span>
                        </div>
                        <span style="font-size: 14px; color: #888; font-weight: 400;">{ds_label}</span>
                    </div>''',
                    unsafe_allow_html=True
                )

                # Format normalization values with N/A handling
                if normalization:
                    scaling_val = norm_scaling if norm_scaling else "N/A"
                    scope_val = (norm_scope.title() if isinstance(norm_scope, str) else str(norm_scope)) if norm_scope else "N/A"
                    trim_val = f"{norm_trim_pct}%" if norm_trim_pct is not None else "N/A"
                    precision_val = str(norm_precision) if norm_precision is not None else "N/A"
                    outlier_val = str(norm_outlier_limit) if (norm_outliers and norm_outlier_limit is not None) else "N/A"

                    st.markdown(
                        f"""
                        <div style="display: flex; align-items: flex-start; gap: 24px; margin-bottom: 12px;">
                            <div style="display: flex; gap: 24px;">
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Benchmark</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{benchmark}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Frequency</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{frequency}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Start Date</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{start_date}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">End Date</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{end_date}</div>
                                </div>
                            </div>
                            <div style="width: 1px; background: #dee2e6; align-self: stretch; margin: 0 8px;"></div>
                            <div style="display: flex; gap: 24px;">
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Scaling</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{scaling_val}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Scope</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{scope_val}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Trim</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{trim_val}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Precision</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{precision_val}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Outlier</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{outlier_val}</div>
                                </div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(
                        f"""
                        <div style="display: flex; align-items: flex-start; gap: 24px; margin-bottom: 12px;">
                            <div style="display: flex; gap: 24px;">
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Benchmark</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{benchmark}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Frequency</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{frequency}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Start Date</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{start_date}</div>
                                </div>
                                <div>
                                    <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">End Date</div>
                                    <div style="font-size: 14px; font-weight: 500; color: #212529;">{end_date}</div>
                                </div>
                            </div>
                            <div style="width: 1px; background: #dee2e6; align-self: stretch; margin: 0 8px;"></div>
                            <div>
                                <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">Normalization</div>
                                <div style="font-size: 14px; font-weight: 500; color: #6c757d;">None</div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )

                st.divider()

                st.markdown("<div style='font-size: 15px; font-weight: 400; color: #60646A; margin-bottom: 10px;'>PAST ANALYSES</div>", unsafe_allow_html=True)

                for job in ds_jobs:
                    render_job_card(job, fl_id)

                # Bottom padding for the dataset card
                st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)


def render_job_card(job: dict, fl_id: str) -> None:
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

    # HTML Link Card
    link_content = f"""
    <a href="?select_job_id={job_id}&fl_id={fl_id}" target="_self" class="job-card-link">
        <div style="display:flex;align-items:center;gap:24px;padding:12px 16px;">
            <div style="font-size:14px;font-weight:600;color:#212529;white-space:nowrap;">{formatted_date}</div>
            <div style="display:flex;gap:24px;align-items:center;flex:1;">
                <div style="display:flex;align-items:center;gap:6px;">
                    <span style="font-size:11px;color:#64748b;text-transform:uppercase;">Min Alpha</span>
                    <span style="font-size:13px;font-weight:500;color:#333;">{min_alpha}</span>
                </div>
                <div style="display:flex;align-items:center;gap:6px;">
                    <span style="font-size:11px;color:#64748b;text-transform:uppercase;">Top X</span>
                    <span style="font-size:13px;font-weight:500;color:#333;">{top_pct}%</span>
                </div>
                <div style="display:flex;align-items:center;gap:6px;">
                    <span style="font-size:11px;color:#64748b;text-transform:uppercase;">Bottom X</span>
                    <span style="font-size:13px;font-weight:500;color:#333;">{bottom_pct}%</span>
                </div>
            </div>
            <span style="background-color:{status_bg};color:{status_color};padding:3px 9px;border-radius:12px;font-size:11px;font-weight:600;text-transform:capitalize;letter-spacing:0.4px;white-space:nowrap;">{status}</span>
        </div>
    </a>
    """

    st.markdown(link_content, unsafe_allow_html=True)


def _handle_job_click(job_id: str) -> None:
    """Handle job card click - restore job state and trigger rerun."""
    if not restore_job_state(job_id):
        st.session_state["_job_restore_error"] = job_id
