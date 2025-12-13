import streamlit as st
from datetime import datetime
from collections import defaultdict
import os
import json
from src.core.context import get_state, update_state
from src.core.utils import format_timestamp, locate_factor_list_file
from src.workers.manager import list_jobs, get_dataset_info_from_backup
from src.core.job_restore import restore_job_state
from src.services.readers import ParquetDataReader


def _info_item(label: str, value: str, muted: bool = False) -> str:
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def _job_param(label: str, value: str) -> str:
    return f'<div class="job-card-param"><span class="label">{label}</span><span class="value">{value}</span></div>'


def _get_current_dataset_info(fl_id: str) -> tuple[str | None, dict | None]:
    """
    Get the current dataset version and metadata from the source parquet file.

    Returns:
        tuple: (dataset_version, dataset_info) or (None, None) on error
    """
    try:
        dataset_path = locate_factor_list_file(fl_id)

        # Get modification timestamp as version (same logic as processing.py)
        ts = os.path.getmtime(dataset_path)
        current_version = str(int(ts))

        # Read metadata from source parquet
        reader = ParquetDataReader(dataset_path)
        _, dataset_info = reader.get_metadata_bundle()

        return current_version, dataset_info
    except (FileNotFoundError, ValueError, Exception):
        return None, None


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


def _render_dataset_card(
    dataset_info: dict,
    ds_ver: str,
    jobs: list,
    fl_id: str,
    show_new_analysis_button: bool = False,
) -> None:
    """
    Render a dataset card with its metadata and optional job list.
    """
    frequency_map = {
        "WEEKLY": "Every week",
        "WEEKS2": "Every 2 weeks",
        "WEEKS4": "Every 4 weeks",
        "WEEKS8": "Every 8 weeks",
        "WEEKS13": "Every 13 weeks",
        "WEEKS26": "Every 26 weeks",
        "WEEKS52": "Every 52 weeks",
    }

    universe = dataset_info.get("universeName", "Unknown Universe")
    frequency = frequency_map.get(dataset_info.get("frequency"), "Unknown")
    normalization = dataset_info.get("normalization", None)
    currency = dataset_info.get("currency", "USD")
    ds_label = format_timestamp(ds_ver)
    start_date = dataset_info.get("startDt") or "N/A"
    end_date = dataset_info.get("endDt") or "N/A"
    benchmark = dataset_info.get("benchName", "N/A")

    with st.container(border=True):
        # Card header with optional button
        if show_new_analysis_button:
            h_left, h_right = st.columns([4, 1], vertical_alignment="center")
        else:
            h_left = st.container()
            h_right = None

        with h_left:
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

        if h_right:
            with h_right:
                st.button(
                    "New Analysis",
                    type="primary",
                    use_container_width=True,
                    on_click=lambda: update_state(
                        page="analysis", current_step=1, current_job_id=None
                    ),
                )

        _render_dataset_info(
            benchmark,
            frequency,
            start_date,
            end_date,
            normalization,
            dataset_info.get("precision"),
        )

        # Only show job section if there are jobs
        if jobs:
            st.divider()
            st.markdown(
                "<div style='font-size: 15px; font-weight: 400; color: #60646A; margin-bottom: 10px;'>PAST ANALYSES</div>",
                unsafe_allow_html=True,
            )
            for job in jobs:
                render_job_card(job, fl_id)
        else:
            st.divider()
            st.markdown(
                "<div style='font-size: 14px; color: #9ca3af; font-style: italic; padding: 8px 0;'>No analyses yet for this dataset version</div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    # Page header (no standalone button - it's now in the dataset card)
    st.markdown(
        "<div style='font-size:24px;font-weight:700;color:#212529;margin:0 0 16px 0;padding:0;'>"
        "Factor Evaluator - Analysis History"
        "</div>",
        unsafe_allow_html=True,
    )

    # Handle card click
    if "select_job_id" in st.query_params:
        job_id = st.query_params["select_job_id"]
        del st.query_params["select_job_id"]
        _handle_job_click(job_id)
        st.rerun()

    # Get current dataset version and metadata from source file
    current_version, current_dataset_info = _get_current_dataset_info(fl_id)

    # Get all existing jobs
    jobs = list_jobs(fl_id)

    # Group jobs by dataset version
    grouped_jobs = defaultdict(list)
    for job in jobs:
        grouped_jobs[job["dataset_version"]].append(job)

    # Check if current version already has jobs
    current_version_has_jobs = current_version and current_version in grouped_jobs

    # Get sorted dataset versions (newest first)
    sorted_datasets = sorted(
        grouped_jobs.keys(),
        key=lambda x: float(x) if x.replace(".", "", 1).isdigit() else 0,
        reverse=True,
    )

    # Render current dataset card if it doesn't have jobs yet
    if current_version and current_dataset_info and not current_version_has_jobs:
        _render_dataset_card(
            dataset_info=current_dataset_info,
            ds_ver=current_version,
            jobs=[],
            fl_id=fl_id,
            show_new_analysis_button=True,
        )

    # Render existing dataset cards
    for ds_ver in sorted_datasets:
        ds_jobs = grouped_jobs[ds_ver]

        # Determine if this is the current dataset version
        is_current_version = ds_ver == current_version

        # Get dataset info (from current source for matching version, otherwise from backup)
        if is_current_version and current_dataset_info:
            dataset_info = current_dataset_info
        else:
            dataset_info = get_dataset_info_from_backup(fl_id, ds_ver)

        if dataset_info:
            _render_dataset_card(
                dataset_info=dataset_info,
                ds_ver=ds_ver,
                jobs=ds_jobs,
                fl_id=fl_id,
                show_new_analysis_button=is_current_version,
            )

    # Show message if no data at all
    if not jobs and not current_dataset_info:
        st.info("No past analysis found for this Factor List.")


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
