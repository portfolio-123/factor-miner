import os

import streamlit as st
import streamlit.components.v1 as components
from streamlit_extras.stylable_container import stylable_container
from datetime import datetime
import pandas as pd
from typing import Optional
import re
from src.core.context import (
    get_state,
    update_state,
    clear_debug_logs,
)
from src.services.readers import get_current_dataset_info, get_dataset_formulas
from src.ui.constants import SCALING_LABELS, frequency_map
from src.core.types import DatasetConfig, ScopeType, Job
from src.core.job_restore import restore_job_state


def add_formula_column(
    df: pd.DataFrame,
    formulas_df: pd.DataFrame | None,
    factor_col: str = "Factor",
) -> pd.DataFrame:
    if formulas_df is None or "name" not in formulas_df.columns:
        return df

    formula_map = (
        formulas_df.drop_duplicates(subset=["name"]).set_index("name")["formula"]
    )
    result = df.copy()
    factor_idx = result.columns.get_loc(factor_col)
    result.insert(factor_idx + 1, "Formula", result[factor_col].map(formula_map))
    return result


def copy_to_clipboard_button(
    text: str,
    label: str = "Copy to Clipboard",
    key: str = "copy_btn",
    button_type: str = "secondary",
) -> None:
    escaped = text.replace('\\', '\\\\').replace('`', '\\`').replace('${', '\\${')
    container_key = f"{key}_container"

    with stylable_container(key=container_key, css_styles=""):
        st.button(label, key=key, type=button_type, use_container_width=True)

    components.html(
        f"""
        <script>
            const csv = `{escaped}`;
            const btn = window.parent.document.querySelector('[data-key="{container_key}"] button');
            if (btn && !btn._copyAttached) {{
                btn._copyAttached = true;
                btn.addEventListener('click', (e) => {{
                    e.preventDefault();
                    navigator.clipboard.writeText(csv);
                    const txt = btn.querySelector('p') || btn;
                    const orig = txt.textContent;
                    txt.textContent = 'Copied!';
                    setTimeout(() => txt.textContent = orig, 1000);
                }});
            }}
        </script>
        """,
        height=0,
    )


def render_session_expired(fl_id: str | None) -> None:
    """Render the session expired message. Does not call st.stop()."""
    st.markdown("<div style='height: 25vh'></div>", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.warning(
            "**Session expired or invalid**\n\n"
            "Access this tool via the main website."
        )
        if fl_id:
            base_url = os.getenv("P123_BASE_URL")
            st.markdown(
                f"<div style='text-align: center; margin-top: 10px;'>"
                f"<a href='{base_url}/sv/factorList/{fl_id}/download'>Return to Factor List</a>"
                f"</div>",
                unsafe_allow_html=True,
            )


def header_simple_back(create_columns: bool = True) -> None:
    if create_columns:
        col_back, _ = st.columns([1, 11])
        container = col_back
    else:
        container = st.container()

    with container:
        if not create_columns:
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


def render_breadcrumb(steps: list[tuple[str, str | None]]) -> None:
    html_code = """
    <div class="breadcrumb">
    """

    for i, (label, link) in enumerate(steps):
        if i > 0:
            html_code += " &gt; "

        if link:
            html_code += f"<a href='{link}' target='_blank'>{label}</a>"
        else:
            html_code += f"<span>{label}</span>"

    html_code += "</div>"

    st.markdown(html_code, unsafe_allow_html=True)


def render_page_header() -> None:
    state = get_state()
    fl_id = state.factor_list_uid
    base_url = os.getenv("P123_BASE_URL")
    steps = [
        ("Factor List", f"{base_url}/sv/factorList/{fl_id}/download"),
        ("FactorMiner", None),
    ]

    # TODO: ask marco, do we need to handle the case where the dataset info is not found but they have past results? consider cleanup, etc
    _, dataset_info = get_current_dataset_info(state.dataset_path)

    render_breadcrumb(steps)
    st.title(
        f"{dataset_info.flName if dataset_info else 'Unknown'} ({fl_id})"
    )


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
                update_state(show_debug_modal=True)

    else:
        col_brand, col_nav, col_logs = st.columns(
            [1, 2, 1], vertical_alignment="center"
        )

        with col_brand:
            btn_col, _ = st.columns([1, 1])
            with btn_col:
                header_simple_back(create_columns=False)

        with col_nav:
            btn_cols = st.columns([1, 1])

            nav_steps = [(1, "Settings"), (2, "Review")]

            for i, (step_num, step_name) in enumerate(nav_steps):
                is_current = step_num == state.current_step
                is_available = step_num == 1 or state.config_completed

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
                    update_state(show_debug_modal=True)

    if state.show_debug_modal:
        _show_debug_modal()


@st.dialog("Debug Logs", width="large")
def _show_debug_modal():
    # reset flag immediately - dialog is already open, so dismissing it won't retrigger
    update_state(show_debug_modal=False)

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

    # Clear the flag after dialog is created
    update_state(formulas_ds_ver=None)


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
) -> None:
    if not best_features:
        st.warning(
            "No features found matching the current criteria."
            "Try adjusting the correlation threshold or minimum alpha."
        )
        return

    # filter to best features and sort by absolute alpha
    best_metrics_df = metrics_df[metrics_df["column"].isin(best_features)].copy()
    best_metrics_df = best_metrics_df.sort_values(
        by="annualized alpha %", key=abs, ascending=False
    )

    display_df = best_metrics_df.rename(
        columns={
            "column": "Factor",
            "annualized alpha %": "Ann. Alpha %",
            "T Statistic": "T-Statistic",
            "p-value": "P-Value",
        }
    )[["Factor", "Ann. Alpha %", "T-Statistic", "P-Value"]]

    # format numeric columns as strings
    formatters = {
        "Ann. Alpha %": lambda x: f"{x:.2f}%",
        "T-Statistic": lambda x: f"{x:.4f}",
        "P-Value": lambda x: f"{x:.6f}",
    }
    for col, fmt in formatters.items():
        display_df[col] = display_df[col].apply(fmt)

    st.dataframe(
        display_df,
        height=400,
        width="stretch",
        hide_index=True,
        column_config={
            "Factor": st.column_config.TextColumn("Factor", width="medium"),
            "Ann. Alpha %": st.column_config.TextColumn("Ann. Alpha %", width="small"),
            "T-Statistic": st.column_config.TextColumn("T-Statistic", width="small"),
            "P-Value": st.column_config.TextColumn("P-Value", width="small"),
        },
    )


def render_info_item(label: str, value: str, muted: bool = False) -> str:
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def render_big_info_item(label: str, value: str) -> str:
    return f'<div class="dataset-info-item big"><div class="label">{label}</div><div class="value">{value}</div></div>'


def render_dataset_info_row(
    config: DatasetConfig,
    ds_ver: str | None = None,
    show_view_factors: bool = False,
    fl_id: str | None = None,
) -> None:

    c1, c2, c3, c4 = st.columns([1.5, 2, 1, 1], vertical_alignment="top")

    with c1:
        st.markdown(
            render_big_info_item("Universe", config.universeName),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            render_big_info_item(
                "Period", f"{config.startDt or 'N/A'} - {config.endDt or 'N/A'}"
            ),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            render_big_info_item(
                "Frequency", frequency_map.get(config.frequency, "N/A")
            ),
            unsafe_allow_html=True,
        )
    with c4:
        count = config.factorCount or 0
        button_key = f"view_factors_btn_{ds_ver}"
        st.markdown(
            '<div class="dataset-info-item big"><div class="label">FACTORS</div></div>',
            unsafe_allow_html=True,
        )
        with stylable_container(
            key=f"factors_btn_{ds_ver}",
            css_styles="""
                button {
                    background: none !important;
                    border: none !important;
                    color: #212529 !important;
                    font-size: 20px !important;
                    font-weight: 600 !important;
                    padding: 0 !important;
                    text-decoration: underline;
                    cursor: pointer;
                }
                button:hover { color: #2196F3 !important; }
            """,
        ):
            st.button(
                f"View ({count})",
                key=button_key,
                on_click=handle_view_formulas,
                args=(ds_ver,),
            )

        state = get_state()
        if state.formulas_ds_ver == ds_ver:
            formulas_df = get_dataset_formulas(fl_id, state.formulas_ds_ver)
            if formulas_df is not None:
                show_formulas_modal(formulas_df)
            else:
                update_state(formulas_ds_ver=None)

    st.markdown('<div style="margin-bottom: 16px;"></div>', unsafe_allow_html=True)
    st.markdown('<div style="height: 5px;"></div>', unsafe_allow_html=True)

    col_left, col_divider, col_right = st.columns(
        [1, 0.05, 1], vertical_alignment="top"
    )

    with col_left:
        st.markdown(
            '<div class="dataset-info-item"><div class="label" style="margin-bottom: 8px; font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; text-transform: none;">Other Settings</div></div>',
            unsafe_allow_html=True,
        )
        middle_items = [
            render_info_item("Currency", config.currency),
            render_info_item("Benchmark", config.benchmark or "N/A"),
            render_info_item("Precision", config.precision or "N/A"),
            render_info_item("Pit Method", config.pitMethod or "N/A"),
        ]
        st.markdown(
            f'<div class="dataset-info-group">{"".join(middle_items)}</div>',
            unsafe_allow_html=True,
        )

    with col_divider:
        st.markdown(
            '<div class="dataset-info-divider" style="height: 100%;"></div>',
            unsafe_allow_html=True,
        )

    with col_right:
        normalization = config.normalization
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
                    "Scope",
                    normalization.scope.title() if normalization.scope else "None",
                ),
                render_info_item(
                    "Trim",
                    (
                        f"{normalization.trimPct}%"
                        if normalization.trimPct is not None
                        else "N/A"
                    ),
                ),
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

            st.markdown(
                '<div class="dataset-info-item"><div class="label" style="margin-bottom: 8px; font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; text-transform: none;">Normalization</div></div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                f'<div class="dataset-info-group">{"".join(norm_items)}</div>',
                unsafe_allow_html=True,
            )


def handle_view_formulas(ds_ver: str) -> None:
    update_state(formulas_ds_ver=ds_ver)


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

    if items:
        params_html = f'<div class="dataset-info-group">{"".join(items)}</div>'
        section_header("Analysis Parameters")
        st.markdown(params_html, unsafe_allow_html=True)



def render_dataset_header(
    dataset_info: DatasetConfig | dict,
    dataset_version: str,
    fl_id: Optional[str] = None,
) -> None:
    config = DatasetConfig.model_validate(dataset_info)

    with st.container(border=True):
        render_dataset_info_row(
            config,
            dataset_version if fl_id else None,
            show_view_factors=bool(fl_id and config.factorCount),
            fl_id=fl_id,
        )

        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)


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
    dataset_info: DatasetConfig | None,
    ds_ver: str,
    jobs: list[Job],
    fl_id: str,
) -> None:

    config = dataset_info or DatasetConfig()

    with st.container(border=True):
        # Use the same render_dataset_info_row with clickable "View (6)"
        render_dataset_info_row(
            config,
            ds_ver,
            show_view_factors=bool(fl_id and config.factorCount),
            fl_id=fl_id,
        )

        st.markdown("<div style='height: 8px;'></div>", unsafe_allow_html=True)
