import streamlit as st
import pandas as pd
from typing import Optional
import re
from src.core.context import get_state, update_state, clear_debug_logs
from src.core.utils import format_timestamp
from src.services.readers import ParquetDataReader
import json
import os


def _navigate_to(step: int) -> None:
    update_state(current_step=step)


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
        col_brand, col_nav, col_logs = st.columns([2.5, 4.5, 0.8])

        with col_brand:
            if st.button("Back", type="secondary"):
                update_state(page="history", current_job_id=None)
                st.rerun()

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
                        width="stretch",
                        on_click=_navigate_to if is_available else None,
                        args=(step_num,) if is_available else None,
                    )

        with col_logs:
            if st.button("Logs", key="debug_btn_analysis", width="stretch"):
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


def _dataset_info_item(label: str, value: str, muted: bool = False) -> str:
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def render_dataset_header(dataset_info: dict, dataset_version: str) -> None:
    """Render the dataset header with universe name, metadata, and info row."""
    frequency_map = {
        "WEEKLY": "Every week",
        "WEEKS2": "Every 2 weeks",
        "WEEKS4": "Every 4 weeks",
        "WEEKS8": "Every 8 weeks",
        "WEEKS13": "Every 13 weeks",
        "WEEKS26": "Every 26 weeks",
        "WEEKS52": "Every 52 weeks",
    }
    scaling_map = {"normal": "Z-Score", "minmax": "Min/Max", "rank": "Rank"}

    universe = dataset_info.get("universeName", "Unknown Universe")
    frequency = frequency_map.get(dataset_info.get("frequency"), "Unknown")
    currency = dataset_info.get("currency", "USD")
    ds_label = format_timestamp(dataset_version)
    start_date = dataset_info.get("startDt") or "N/A"
    end_date = dataset_info.get("endDt") or "N/A"
    benchmark = dataset_info.get("benchName", "N/A")
    precision = dataset_info.get("precision", "N/A")
    normalization = dataset_info.get("normalization", None)

    with st.container(border=True):
        # Card header
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

        # Dataset info row
        base_items = [
            _dataset_info_item("Benchmark", benchmark),
            _dataset_info_item("Frequency", frequency),
            _dataset_info_item("Start Date", start_date),
            _dataset_info_item("End Date", end_date),
        ]

        if isinstance(normalization, str):
            normalization = json.loads(normalization)

        if normalization:
            norm_scaling = scaling_map.get(normalization.get("scaling"), "N/A")
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
            na_fill = "Middle" if normalization.get("naFill") else "None"

            norm_items = [
                _dataset_info_item("Scaling", norm_scaling),
                _dataset_info_item("Scope", scope_val),
                _dataset_info_item("Trim", trim_val),
                _dataset_info_item("Precision", str(precision)),
                _dataset_info_item("Outlier", outlier_val),
                _dataset_info_item("N/A Handling", na_fill),
            ]
        else:
            norm_items = [_dataset_info_item("Normalization", "None", muted=True)]

        html = f"""
        <div class="dataset-info-row">
            <div class="dataset-info-group">{"".join(base_items)}</div>
            <div class="dataset-info-divider"></div>
            <div class="dataset-info-group">{"".join(norm_items)}</div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)


def render_current_dataset_header() -> None:
    """Render the dataset header for the current dataset in state."""
    state = get_state()

    if not state.dataset_path:
        return

    try:
        # Get dataset version from file modification time
        ts = os.path.getmtime(state.dataset_path)
        dataset_version = str(int(ts))

        # Read metadata from parquet
        reader = ParquetDataReader(state.dataset_path)
        _, dataset_info = reader.get_metadata_bundle()

        if dataset_info:
            render_dataset_header(dataset_info, dataset_version)
    except (FileNotFoundError, Exception):
        pass
