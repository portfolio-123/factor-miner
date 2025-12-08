import streamlit as st
import pandas as pd
import time
from typing import Optional
import re
from src.core.context import get_state, clear_debug_logs

def header_with_navigation() -> int:
    state = get_state()

    steps = [
        (1, "Settings"),
        (2, "Review"),
        (3, "Results")
    ]

    selected_step = state.current_step

    # Create header row: brand on left, breadcrumb in middle, logs button on right
    col_brand, col_nav, col_logs = st.columns([2.5, 4, 0.8])

    with col_brand:
        st.markdown("""
            <div style="padding: 5px 0; display: flex; flex-direction: column;">
                <span style="font-size: 24px; font-weight: 700; color: #333;">Portfolio123</span>
                <span style="font-size: 16px; font-weight: 400; color: #666;">Factor Evaluator</span>
            </div>
        """, unsafe_allow_html=True)

    # Build arrow breadcrumb navigation with clickable buttons
    with col_nav:
        btn_cols = st.columns([1, 1, 1])

        for i, (step_num, step_name) in enumerate(steps):
            is_current = step_num == state.current_step
            is_available = step_num == 1 or (step_num - 1) in state.completed_steps

            with btn_cols[i]:
                btn_type = "primary" if is_current else "secondary"
                if st.button(step_name, key=f"step_btn_{step_num}", type=btn_type,
                           disabled=not is_available, width='stretch'):
                    if is_available:
                        selected_step = step_num

    with col_logs:
        if st.button("Logs", key="debug_btn", width='stretch'):
            st.session_state.show_debug_modal = True

    if st.session_state.get('show_debug_modal', False):
        _show_debug_modal()

    return selected_step


@st.dialog("Debug Logs", width="large")
def _show_debug_modal():
    # reset flag immediately - dialog is already open, so dismissing it won't retrigger
    st.session_state.show_debug_modal = False
    state = get_state()

    if state.debug_logs:
        log_lines = []
        for log in state.debug_logs[-100:]:
            # color the timestamp in blue
            formatted = re.sub(
                r'(\[.*?\])',
                r'<span style="color: #2196F3;">\1</span>',
                log
            )
            log_lines.append(formatted)
        log_html = "<br>".join(log_lines)
        st.markdown(
            f'<div style="font-family: monospace; font-size: 13px; '
            f'background: #f5f5f5; color: #333; padding: 12px; '
            f'border-radius: 4px; max-height: 400px; overflow-y: auto; '
            f'margin-bottom: 16px;">'
            f'{log_html}</div>',
            unsafe_allow_html=True
        )

    _, col1 = st.columns([6,1])
    with col1:
        if st.button("Clear Logs", key="modal_clear_logs", width='stretch', type="primary"):
            clear_debug_logs()
            st.session_state.show_debug_modal = True  # Keep dialog open
            st.rerun()


def section_header(title: str) -> None:
    st.markdown(f"""
        <div style="font-size: 14px; font-weight: 600; color: #2196F3;
                    margin: 15px 0 8px 0; padding-bottom: 5px;
                    border-bottom: 2px solid #2196F3;">
            {title}
        </div>
    """, unsafe_allow_html=True)


def render_job_progress(job_data: dict) -> None:
    progress = job_data.get('progress', {})
    completed = progress.get('completed', 0)
    total = progress.get('total', 0)
    current_factor = progress.get('current_factor', '')

    _, center_col, _ = st.columns([1, 2, 1])

    with center_col:
        st.space(100)
        st.subheader("Running Factor Analysis")

        if total > 0:
            st.progress(completed / total, text=f"{completed} / {total} factors analyzed")
        else:
            st.progress(0, text="Initializing...")

        if current_factor:
            st.info(f"Analyzing: **{current_factor}**")
        else:
            st.info("Starting worker process...")

    # show updates every .5 seconds
    time.sleep(0.5)
    st.rerun()


def render_dataset_statistics(stats: dict, benchmark: str) -> None:
    section_header("Dataset Statistics")

    cols = st.columns([1, 1, 1, 2, 1], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"

    stat_items = [
        ("Rows", stats['num_rows']),
        ("Dates", stats['num_dates']),
        ("Columns", stats['num_columns']),
        ("Period", f"{stats['min_date']} - {stats['max_date']}"),
        ("Benchmark", benchmark or "N/A"),
    ]

    for col, (label, value) in zip(cols, stat_items):
        with col:
            st.badge(label)
            st.markdown(f"<p style='{stat_style}'>{value}</p>", unsafe_allow_html=True)


def render_formulas_grid(formulas_df: pd.DataFrame) -> None:
    display_df = formulas_df[['formula', 'name', 'tag']].copy()

    st.dataframe(
            display_df,
            height=400,
            width='stretch',
            column_config={
                "formula": st.column_config.TextColumn(
                    "Formula",
                    width="large"
                ),
                "name": st.column_config.TextColumn(
                    "Name",
                    width="medium"
                ),
                "tag": st.column_config.TextColumn(
                    "Tag",
                    width="small"
                )
            },
            hide_index=True
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
    display_df.rename(columns={'index': 'Row'}, inplace=True)

    st.dataframe(
        display_df,
        height=500,
        width='stretch',
        hide_index=True,
        column_config={
            "Row": st.column_config.NumberColumn("Row", width=85)
        }
    )


def render_results_table(best_features: list, metrics_df: pd.DataFrame, formulas_df: Optional[pd.DataFrame] = None) -> Optional[pd.DataFrame]:
    if best_features:
        best_metrics_df = metrics_df[metrics_df['column'].isin(best_features)].copy()
        best_metrics_df = best_metrics_df.sort_values(
            by='annualized alpha %',
            key=abs,
            ascending=False
        )

        # Join with formulas if available
        if formulas_df is not None and 'name' in formulas_df.columns and 'formula' in formulas_df.columns:
            formulas_lookup = formulas_df[['name', 'formula']].drop_duplicates(subset=['name'])
            best_metrics_df = best_metrics_df.merge(
                formulas_lookup,
                left_on='column',
                right_on='name',
                how='left'
            )
        else:
            best_metrics_df['formula'] = ''

        # Rename columns for display
        display_df = best_metrics_df.rename(columns={
            'column': 'Factor',
            'annualized alpha %': 'Ann. Alpha %',
            'T Statistic': 'T-Statistic',
            'p-value': 'P-Value',
            'formula': 'Formula'
        })

        display_df['Ann. Alpha %'] = display_df['Ann. Alpha %'].apply(lambda x: f"{x:.2f}%")
        display_df['T-Statistic'] = display_df['T-Statistic'].apply(lambda x: f"{x:.4f}")
        display_df['P-Value'] = display_df['P-Value'].apply(lambda x: f"{x:.6f}")

        display_df = display_df[['Factor', 'Ann. Alpha %', 'T-Statistic', 'P-Value']]

        st.dataframe(
            display_df,
            height=400,
            width='stretch',
            hide_index=True,
            column_config={
                "Factor": st.column_config.TextColumn("Factor", width="medium"),
                "Ann. Alpha %": st.column_config.TextColumn("Ann. Alpha %", width="small"),
                "T-Statistic": st.column_config.TextColumn("T-Statistic", width="small"),
                "P-Value": st.column_config.TextColumn("P-Value", width="small")
            }
        )
        return display_df
    else:
        st.warning(
            "No features found matching the current criteria. "
            "Try adjusting the correlation threshold or minimum alpha."
        )
        return None
