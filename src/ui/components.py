import streamlit as st
from datetime import datetime
import pandas as pd
from typing import Optional
import re
from src.core.context import (
    get_state,
    update_state,
    clear_debug_logs,
    reset_analysis_state,
)
from src.core.utils import format_timestamp
from src.services.readers import ParquetDataReader
from src.workers.manager import get_dataset_info_from_backup
import os
from src.ui.constants import SCALING_LABELS, frequency_map
from src.core.types import DatasetConfig, NormalizationConfig, ScopeType, Job
from src.core.job_restore import restore_job_state


def header_simple_back(create_columns: bool = True) -> None:
    if create_columns:
        col_back, _ = st.columns([1, 11])
        container = col_back
    else:
        container = st.container()

    with container:
        # Create a nested layout to center the button vertically/horizontally if needed,
        # but primarily to constrain width if not using columns
        if not create_columns:
            # If we're already in a small column, just render the button
            st.button(
                "Back",
                type="secondary",
                key="back_btn_simple",
                use_container_width=True,
                on_click=lambda: update_state(page="history", current_job_id=None),
            )
        else:
            if st.button("Back", type="secondary", key="back_btn_simple"):
                update_state(page="history", current_job_id=None)
                st.rerun()


def header_with_navigation() -> None:
    state = get_state()

    if state.page == "history":

        col_brand, col_nav, col_logs = st.columns([2.5, 4.5, 0.8])
        with col_brand:
            st.markdown(
                """
                <div style="padding: 5px 0; display: flex; flex-direction: column;">
                    <span style="font-size: 24px; font-weight: 700; color: #333;">Portfolio123</span>
                    <span style="font-size: 16px; font-weight: 400; color: #666;">Factor Evaluator</span>
                </div>
            """,
                unsafe_allow_html=True,
            )

        with col_logs:
            if st.button("Logs", key="debug_btn", width="stretch"):
                st.session_state.show_debug_modal = True

    else:
        col_brand, col_nav, col_logs = st.columns(
            [1, 2, 1], vertical_alignment="center"
        )

        with col_brand:
            btn_col, _ = st.columns([1, 1])
            with btn_col:
                header_simple_back(create_columns=False)

        with col_nav:
            btn_cols = st.columns([1, 1, 1])

            nav_steps = [(1, "Settings"), (2, "Review"), (3, "Results")]

            for i, (step_num, step_name) in enumerate(nav_steps):
                is_current = step_num == state.current_step
                is_available = True

                if step_num > 1:
                    is_available = (step_num - 1) in state.completed_steps

                if step_num == 1:
                    is_available = True

                with btn_cols[i]:
                    btn_type = "primary" if is_current else "secondary"
                    st.button(
                        step_name,
                        key=f"step_btn_{step_num}",
                        type=btn_type,
                        disabled=not is_available,
                        use_container_width=True,
                        on_click=update_state if is_available else None,
                        kwargs={"current_step": step_num} if is_available else None,
                    )

        with col_logs:
            _, btn_col = st.columns([1, 1])
            with btn_col:
                if st.button(
                    "Logs",
                    key="debug_btn_analysis",
                    use_container_width=True,
                    type="primary",
                ):
                    st.session_state.show_debug_modal = True

    if st.session_state.get("show_debug_modal", False):
        _show_debug_modal()


@st.dialog("Debug Logs", width="large")
def _show_debug_modal():
    # reset flag immediately - dialog is already open, so dismissing it won't retrigger
    st.session_state.show_debug_modal = False

    @st.fragment
    def _logs_content():
        state = get_state()

        if state.debug_logs:
            log_lines = []
            for log in state.debug_logs[-100:]:
                # color the timestamp in blue
                formatted = re.sub(
                    r"(\[.*?\])", r'<span style="color: #2196F3;">\1</span>', log
                )
                log_lines.append(formatted)
            log_html = "<br>".join(log_lines)
            st.markdown(
                f'<div style="font-family: monospace; font-size: 13px; '
                f"background: #f5f5f5; color: #333; padding: 12px; "
                f"border-radius: 4px; max-height: 400px; overflow-y: auto; "
                f'margin-bottom: 16px;">'
                f"{log_html}</div>",
                unsafe_allow_html=True,
            )

        _, col1 = st.columns([6, 1])
        with col1:
            if st.button(
                "Clear Logs", key="modal_clear_logs", width="stretch", type="primary"
            ):
                clear_debug_logs()
                st.rerun()

    _logs_content()


def show_formulas_modal(formulas_df: pd.DataFrame) -> None:
    st.session_state.show_formulas_modal = False

    total = int(len(formulas_df)) if formulas_df is not None else 0
    title = f"Dataset Formulas ({total})"

    dialog = st.dialog(title, width="large")

    @dialog
    def _render() -> None:
        if formulas_df is not None and not formulas_df.empty:
            render_formulas_grid(formulas_df)
        else:
            st.info("No formulas available for this dataset")

    _render()


def section_header(title: str) -> None:
    st.markdown(
        f"""
        <div style="font-size: 14px; font-weight: 600; color: #2196F3;
                    margin: 15px 0 8px 0; padding-bottom: 5px;
                    border-bottom: 2px solid #2196F3;">
            {title}
        </div>
    """,
        unsafe_allow_html=True,
    )


def render_dataset_statistics(stats: dict, benchmark: str) -> None:
    section_header("Dataset Statistics")

    cols = st.columns([1, 1, 1, 2, 1], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"

    stat_items = [
        ("Rows", stats["num_rows"]),
        ("Dates", stats["num_dates"]),
        ("Columns", stats["num_columns"]),
        ("Period", f"{stats['min_date']} - {stats['max_date']}"),
        ("Benchmark", benchmark or "N/A"),
    ]

    for col, (label, value) in zip(cols, stat_items):
        with col:
            st.badge(label)
            st.markdown(f"<p style='{stat_style}'>{value}</p>", unsafe_allow_html=True)


def render_formulas_grid(formulas_df: pd.DataFrame) -> None:
    display_df = formulas_df[["formula", "name", "tag"]].copy()

    st.dataframe(
        display_df,
        height=400,
        width="stretch",
        column_config={
            "formula": st.column_config.TextColumn("Formula", width="large"),
            "name": st.column_config.TextColumn("Name", width="medium"),
            "tag": st.column_config.TextColumn("Tag", width="small"),
        },
        hide_index=True,
    )


def render_dataset_preview(df: pd.DataFrame) -> None:
    if len(df) > 20:
        first_10 = df.head(10)
        last_10 = df.tail(10)
        preview_df = pd.concat([first_10, last_10], ignore_index=False)
    else:
        preview_df = df

    st.caption(f"Showing first and last 10 rows")

    # Reset index to make it a regular column for better width control
    display_df = preview_df.reset_index()
    display_df.rename(columns={"index": "Row"}, inplace=True)

    st.dataframe(
        display_df,
        height=500,
        width="stretch",
        hide_index=True,
        column_config={"Row": st.column_config.NumberColumn("Row", width=85)},
    )


def render_results_table(
    best_features: list,
    metrics_df: pd.DataFrame,
    formulas_df: Optional[pd.DataFrame] = None,
) -> Optional[pd.DataFrame]:
    if best_features:
        best_metrics_df = metrics_df[metrics_df["column"].isin(best_features)].copy()
        best_metrics_df = best_metrics_df.sort_values(
            by="annualized alpha %", key=abs, ascending=False
        )

        # Join with formulas if available
        if (
            formulas_df is not None
            and "name" in formulas_df.columns
            and "formula" in formulas_df.columns
        ):
            formulas_lookup = formulas_df[["name", "formula"]].drop_duplicates(
                subset=["name"]
            )
            best_metrics_df = best_metrics_df.merge(
                formulas_lookup, left_on="column", right_on="name", how="left"
            )
        else:
            best_metrics_df["formula"] = ""

        # Rename columns for display
        display_df = best_metrics_df.rename(
            columns={
                "column": "Factor",
                "annualized alpha %": "Ann. Alpha %",
                "T Statistic": "T-Statistic",
                "p-value": "P-Value",
                "formula": "Formula",
            }
        )

        display_df["Ann. Alpha %"] = display_df["Ann. Alpha %"].apply(
            lambda x: f"{x:.2f}%"
        )
        display_df["T-Statistic"] = display_df["T-Statistic"].apply(
            lambda x: f"{x:.4f}"
        )
        display_df["P-Value"] = display_df["P-Value"].apply(lambda x: f"{x:.6f}")

        display_df = display_df[["Factor", "Ann. Alpha %", "T-Statistic", "P-Value"]]

        st.dataframe(
            display_df,
            height=400,
            width="stretch",
            hide_index=True,
            column_config={
                "Factor": st.column_config.TextColumn("Factor", width="medium"),
                "Ann. Alpha %": st.column_config.TextColumn(
                    "Ann. Alpha %", width="small"
                ),
                "T-Statistic": st.column_config.TextColumn(
                    "T-Statistic", width="small"
                ),
                "P-Value": st.column_config.TextColumn("P-Value", width="small"),
            },
        )
        return display_df
    else:
        st.warning(
            "No features found matching the current criteria. "
            "Try adjusting the correlation threshold or minimum alpha."
        )
        return None


def render_info_item(label: str, value: str, muted: bool = False) -> str:
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def render_dataset_info_row(
    benchmark: str,
    frequency: int,
    start_date: str,
    end_date: str,
    normalization: NormalizationConfig | None,
    precision: str | None,
) -> None:

    base_items = [
        render_info_item("Benchmark", benchmark),
        render_info_item("Frequency", frequency_map[frequency]),
        render_info_item("Start Date", start_date),
        render_info_item("End Date", end_date),
    ]

    if normalization:
        norm_items = [
            render_info_item(
                "Scaling",
                (
                    SCALING_LABELS.get(
                        normalization.scaling, str(normalization.scaling)
                    )
                    if normalization.scaling
                    else "None"
                ),
            ),
            render_info_item(
                "Scope", normalization.scope.title() if normalization.scope else "None"
            ),
            render_info_item(
                "Trim",
                (
                    f"{normalization.trimPct}%"
                    if normalization.trimPct is not None
                    else "N/A"
                ),
            ),
            render_info_item("Precision", precision or "N/A"),
            render_info_item(
                "Outlier",
                (
                    str(normalization.outlierLimit)
                    if normalization.outlierLimit is not None
                    else "None"
                ),
            ),
            render_info_item(
                "N/A Handling", "Middle" if normalization.naFill else "None"
            ),
        ]

        if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
            norm_items.append(
                render_info_item("ML Training End", normalization.mlTrainingEnd)
            )
    else:
        norm_items = [render_info_item("Normalization", "None", muted=True)]

    html = f"""
    <div class="dataset-info-row">
        <div class="dataset-info-group">{"".join(base_items)}</div>
        <div class="dataset-info-divider"></div>
        <div class="dataset-info-group">{"".join(norm_items)}</div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)


def handle_view_formulas(fl_id: str, ds_ver: str) -> None:
    st.session_state.show_formulas_modal = True
    st.session_state.formulas_fl_id = fl_id
    st.session_state.formulas_ds_ver = ds_ver


def render_dataset_header(
    dataset_info: dict,
    dataset_version: str,
    analysis_params: Optional[dict] = None,
    fl_id: Optional[str] = None,
) -> None:
    """Render the dataset header with universe name, metadata, and info row."""
    config = DatasetConfig.model_validate(dataset_info)

    universe = config.universeName
    currency = config.currency
    ds_label = format_timestamp(dataset_version)

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

        render_dataset_info_row(
            config.benchmark or "N/A",
            config.frequency,
            config.startDt or "N/A",
            config.endDt or "N/A",
            config.normalization,
            config.precision,
        )

        if analysis_params:
            st.divider()

            items = []
            if "min_alpha" in analysis_params:
                items.append(
                    render_info_item("Min Alpha", f"{analysis_params['min_alpha']}")
                )
            if "top_x_pct" in analysis_params:
                items.append(
                    render_info_item("Top X", f"{analysis_params['top_x_pct']}%")
                )
            if "bottom_x_pct" in analysis_params:
                items.append(
                    render_info_item("Bottom X", f"{analysis_params['bottom_x_pct']}%")
                )

            params_html = f'<div class="dataset-info-group">{"".join(items)}</div>'

            col_params, col_btn = st.columns([5, 1], vertical_alignment="bottom")

            with col_params:
                st.markdown(
                    f"""
                <div class="analysis-params-row" style="padding-bottom: 0;">
                    {params_html}
                </div>
                """,
                    unsafe_allow_html=True,
                )

            with col_btn:
                if fl_id:
                    st.markdown(
                        '<span class="view-formulas-trigger">&nbsp;</span>',
                        unsafe_allow_html=True,
                    )
                    st.button(
                        "View Formulas",
                        key="view_formulas_btn_header",
                        type="tertiary",
                        on_click=handle_view_formulas,
                        args=(fl_id, dataset_version),
                    )

            st.markdown('<div style="height: 12px;"></div>', unsafe_allow_html=True)


def render_current_dataset_header() -> None:
    state = get_state()

    analysis_params = None

    if state.current_step == 3:
        analysis_params = {
            "min_alpha": state.min_alpha,
            "top_x_pct": state.top_x_pct,
            "bottom_x_pct": state.bottom_x_pct,
        }

    dataset_version = None
    if state.dataset_path and os.path.exists(state.dataset_path):
        try:
            ts = os.path.getmtime(state.dataset_path)
            dataset_version = str(int(ts))
        except Exception:
            pass

    if state.current_job_id and not dataset_version:
        try:
            parts = state.current_job_id.split("/")
            if len(parts) >= 2:
                dataset_version = parts[1]
        except:
            pass

    if state.current_job_id:
        try:
            parts = state.current_job_id.split("/")
            if len(parts) >= 2:
                fl_id = parts[0]
                ds_ver = parts[1]
                dataset_info = get_dataset_info_from_backup(fl_id, ds_ver)
                if dataset_info:
                    render_dataset_header(dataset_info, ds_ver, analysis_params, fl_id)
                    return
        except Exception:
            pass

    if not state.dataset_path:
        return

    try:
        ts = os.path.getmtime(state.dataset_path)
        ds_ver = str(int(ts))
        reader = ParquetDataReader(state.dataset_path)
        dataset_info = reader.get_dataset_info()
        if dataset_info:
            render_dataset_header(
                dataset_info, ds_ver, analysis_params, state.factor_list_uid
            )
    except (FileNotFoundError, Exception):
        pass


def render_job_param(label: str, value: str) -> str:
    return f'<div class="job-card-param"><span class="label">{label}</span><span class="value">{value}</span></div>'


def handle_job_selection(job_id: str) -> None:
    if restore_job_state(job_id):
        st.rerun()
    else:
        st.error(f"Failed to load job {job_id}")


def render_job_card(job: Job) -> None:
    created_at = datetime.fromisoformat(job.created_at)
    formatted_date = created_at.strftime("%b %d, %Y %H:%M:%S")
    status = job.status
    job_id = job.id
    job_name = job.name or "Untitled Analysis"

    min_alpha = job.params.min_alpha
    top_pct = job.params.top_pct
    bottom_pct = job.params.bottom_pct

    if status == "completed":
        status_bg, status_color = "#e6f4ea", "#1e8e3e"
    elif status in ("running", "pending"):
        status_bg, status_color = "#fff0b3", "#b06000"
    else:
        status_bg, status_color = "#fce8e6", "#c5221f"

    params_html = "".join(
        [
            render_job_param("Min Alpha", str(min_alpha)),
            render_job_param("Top X", f"{top_pct}%"),
            render_job_param("Bottom X", f"{bottom_pct}%"),
        ]
    )

    card_html = f"""
    <div class="job-card-content">
        <div class="job-card-name">{job_name}</div>
        <div class="job-card-params">{params_html}</div>
        <div class="job-card-right">
            <span class="job-card-date">{formatted_date}</span>
            <span class="job-card-status" style="background-color:{status_bg};color:{status_color};">{status}</span>
        </div>
    </div>
    <span class="job-card-trigger"></span>
    """
    st.markdown(card_html, unsafe_allow_html=True)

    if st.button("Open Analysis", key=f"job_btn_{job_id}", use_container_width=True):
        handle_job_selection(job_id)


def render_dataset_history_card(
    dataset_info: dict,
    ds_ver: str,
    jobs: list[Job],
    fl_id: str,
    is_current: bool = False,
) -> None:
    config = DatasetConfig.model_validate(dataset_info)

    universe = config.universeName
    currency = config.currency
    ds_label = format_timestamp(ds_ver)

    with st.container(border=True):
        header_left, header_right = st.columns([5, 1])

        with header_left:
            st.markdown(
                f"""<div style="display: flex; align-items: center; gap: 10px;">
                    <div style="display: flex; align-items: center; gap: 8px; font-size: 20px; font-weight: 600;">
                        <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#2196F3" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>
                        {universe}
                    </div>
                    <span style="background: #dbeafe; color: #1d4ed8; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500;">{currency}</span>
                </div>""",
                unsafe_allow_html=True,
            )

        with header_right:
            st.markdown(
                f'<div style="text-align: right; font-size: 14px; color: #888;">{ds_label}</div>',
                unsafe_allow_html=True,
            )

        st.space(size=2)
        stats_col, formulas_col = st.columns([8, 1], vertical_alignment="bottom")
        with stats_col:
            render_dataset_info_row(
                config.benchmark or "N/A",
                config.frequency,
                config.startDt or "N/A",
                config.endDt or "N/A",
                config.normalization,
                config.precision,
            )
        with formulas_col:
            st.markdown(
                '<span class="view-formulas-trigger">&nbsp;</span>',
                unsafe_allow_html=True,
            )
            st.button(
                "View Formulas",
                key=f"view_formulas_{ds_ver}",
                type="tertiary",
                on_click=handle_view_formulas,
                args=(fl_id, ds_ver),
            )

        st.divider()

        title_text = (
            "PAST ANALYSES" if jobs else "No analyses yet for this dataset version"
        )
        title_style = (
            "font-size: 15px; font-weight: 400; color: #60646A;"
            if jobs
            else "font-size: 14px; color: #9ca3af; font-style: italic; margin-top: 0.25rem;"
        )

        if is_current:
            title_style += " padding-top: 4px;"
            col1, col2 = st.columns([4, 1])
            with col1:
                st.markdown(
                    f"<div style='{title_style}'>{title_text}</div>",
                    unsafe_allow_html=True,
                )
            with col2:
                st.button(
                    "New Analysis",
                    type="primary",
                    key="new_analysis_btn" if jobs else "new_analysis_btn_empty",
                    use_container_width=True,
                    on_click=reset_analysis_state,
                )
        else:
            title_style += " margin-bottom: 10px;" if jobs else " padding: 8px 0;"
            st.markdown(
                f"<div style='{title_style}'>{title_text}</div>",
                unsafe_allow_html=True,
            )

        for job in jobs:
            render_job_card(job)

        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
