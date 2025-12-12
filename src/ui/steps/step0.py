import streamlit as st
from datetime import datetime
from collections import defaultdict
from src.core.context import get_state, update_state
from src.core.utils import format_timestamp
from src.workers.manager import list_jobs, get_dataset_info_from_backup
from src.core.job_restore import restore_job_state
import json


def _info_item(label: str, value: str, muted: bool = False) -> str:
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def _job_param(label: str, value: str) -> str:
    return f'<div class="job-card-param"><span class="label">{label}</span><span class="value">{value}</span></div>'


def _render_dataset_info(
    benchmark: str,
    frequency: str,
    start_date: str,
    end_date: str,
    normalization: dict | None,
    precision: str,
) -> None:

    scaling_map = {"normal": "Z-Score", "minmax": "Min/Max", "rank": "Rank"}

    base_items = [
        _info_item("Benchmark", benchmark),
        _info_item("Frequency", frequency),
        _info_item("Start Date", start_date),
        _info_item("End Date", end_date),
    ]

    normalization = json.loads(normalization)
    if normalization:

        norm_scaling = scaling_map[normalization.get("scaling")] or "N/A"
        norm_scope = normalization.get("scope")
        scope_val = (
            (norm_scope.title() if isinstance(norm_scope, str) else str(norm_scope))
            if norm_scope
            else "N/A"
        )
        trim_val = (
            f"{normalization.get('trimPct')}%"
            if normalization.get("trimPct") is not None
            else "N/A"
        )
        outlier_val = (
            str(normalization.get("outlierLimit"))
            if (
                normalization.get("outliers")
                and normalization.get("outlierLimit") is not None
            )
            else "N/A"
        )
        ml_training_end = (
            normalization.get("mlTrainingEnd")
            if normalization.get("mlTrainingEnd")
            else "N/A"
        )

        na_fill = normalization.get("naFill") if normalization.get("naFill") else "N/A"
        if na_fill:
            na_fill = "Middle"
        else:
            na_fill = "None"
        norm_items = [
            _info_item("Scaling", norm_scaling),
            _info_item("Scope", scope_val),
            _info_item("Trim", trim_val),
            _info_item("Precision", precision),
            _info_item("Outlier", outlier_val),
            _info_item("N/A Handling", na_fill),
            *(
                [_info_item("ML Training End", ml_training_end)]
                if norm_scope == "dataset"
                else []
            ),
        ]
    else:
        norm_items = [_info_item("Normalization", "None", muted=True)]

    html = f"""
    <div class="dataset-info-row">
        <div class="dataset-info-group">{"".join(base_items)}</div>
        <div class="dataset-info-divider"></div>
        <div class="dataset-info-group">{"".join(norm_items)}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
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
            on_click=lambda: update_state(
                page="analysis", current_step=1, current_job_id=None
            ),
        )

    # handle card click. have to do this because the card is not a button and streamlit is limited
    if "select_job_id" in st.query_params:
        job_id = st.query_params["select_job_id"]
        # clear the param to prevent re-triggering
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
        grouped_jobs[job["dataset_version"]].append(job)

    # newest dataset first
    sorted_datasets = sorted(
        grouped_jobs.keys(),
        key=lambda x: float(x) if x.replace(".", "", 1).isdigit() else 0,
        reverse=True,
    )

    for ds_ver in sorted_datasets:
        ds_jobs = grouped_jobs[ds_ver]

        dataset_info = get_dataset_info_from_backup(fl_id, ds_ver)
        print(dataset_info)
        # dataset_info = {
        #     "universeName": "SP500 Universe",
        #     "frequency": 1,  # Weekly
        #     "currency": "USD",
        #     "benchmark": "SPY:USA",
        #     "startDate": "2020-01-01",
        #     "endDate": "2024-12-31",
        #     "precision": 2,
        #     "normalization": {
        #         "naFill": True,
        #         "scaling": "Z-Score",
        #         "scope": "Dataset",
        #         "trimPct": 2.5,
        #         "mlTrainingEnd": None,
        #         "outliers": True,
        #         "outlierLimit": 3.0,
        #         "precision": 2,
        #     },
        # }

        frequency_map = {
            "WEEKLY": "Every week",
            "WEEKS2": "Every 2 weeks",
            "WEEKS4": "Every 4 weeks",
            "WEEKS8": "Every 8 weeks",
            "WEEKS13": "Every 13 weeks",
            "WEEKS26": "Every 26 weeks",
            "WEEKS52": "Every 52 weeks",
        }

        if dataset_info:
            universe = dataset_info.get("universeName", "Unknown Universe")

            frequency = frequency_map[dataset_info.get("frequency")]

            normalization = dataset_info.get("normalization", None)
            currency = dataset_info.get("currency", "USD")
            ds_label = format_timestamp(ds_ver)

            start_date = dataset_info.get("startDt") or "N/A"
            end_date = dataset_info.get("endDt") or "N/A"

            benchmark = dataset_info.get("benchName", "N/A")

            with st.container(border=True):
                st.markdown(
                    f"""<div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <div style="display: flex; align-items: center; gap: 10px;">
                            <div style="display: flex; align-items: center; gap: 8px; font-size: 20px; font-weight: 600;">
                                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#2196F3" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                                {universe}
                            </div>
                            <span style="background: #dbeafe; color: #1d4ed8; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;">{currency}</span>
                        </div>
                        <span style="font-size: 14px; color: #888; font-weight: 400;">{ds_label}</span>
                    </div>""",
                    unsafe_allow_html=True,
                )

                _render_dataset_info(
                    benchmark,
                    frequency,
                    start_date,
                    end_date,
                    normalization,
                    dataset_info.get("precision"),
                )

                st.divider()

                st.markdown(
                    "<div style='font-size: 15px; font-weight: 400; color: #60646A; margin-bottom: 10px;'>PAST ANALYSES</div>",
                    unsafe_allow_html=True,
                )

                for job in ds_jobs:
                    render_job_card(job, fl_id)

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

    params_html = "".join(
        [
            _job_param("Min Alpha", str(min_alpha)),
            _job_param("Top X", f"{top_pct}%"),
            _job_param("Bottom X", f"{bottom_pct}%"),
        ]
    )

    link_content = f"""
    <a href="?select_job_id={job_id}&fl_id={fl_id}" target="_self" class="job-card-link">
        <div class="job-card-content">
            <div class="job-card-date">{formatted_date}</div>
            <div class="job-card-params">{params_html}</div>
            <span class="job-card-status" style="background-color:{status_bg};color:{status_color};">{status}</span>
        </div>
    </a>
    """

    st.markdown(link_content, unsafe_allow_html=True)


def _handle_job_click(job_id: str) -> None:
    if not restore_job_state(job_id):
        st.session_state["_job_restore_error"] = job_id
