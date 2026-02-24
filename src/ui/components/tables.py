import streamlit as st
import pandas as pd

from src.core.config.constants import CLASSIFICATION_COLORS
from src.core.types.models import AnalysisSummary, DatasetType
from src.core.utils.common import (
    add_formula_and_tag_columns,
    format_date,
    format_runtime,
    format_timestamp,
)
from src.services.dataset_service import BackupDatasetService
from src.ui.components.common import render_copy_download_buttons


def build_column_config(specs: list[tuple[str, str, str]]) -> dict:
    types = {
        "text": st.column_config.TextColumn,
        "number": st.column_config.NumberColumn,
        "link": st.column_config.LinkColumn,
    }
    return {name: types[t](name, width=width) for name, t, width in specs}


def render_correlation_matrix(
    corr_matrix_df: pd.DataFrame,
    title: str,
    file_prefix: str | None = None,
    key_suffix: str = "",
) -> None:
    st.subheader(title)
    rounded = corr_matrix_df.round(4)
    st.dataframe(
        rounded,
        height=min(400, 50 + len(corr_matrix_df) * 35),
        width="stretch",
    )

    file_name = f"{file_prefix}_correlation_matrix.csv" if file_prefix else "correlation_matrix.csv"
    render_copy_download_buttons(
        csv_copy=rounded.to_csv(sep="\t"),
        csv_download=rounded.to_csv(),
        file_name=file_name,
        key_prefix=f"corr_matrix{key_suffix}",
        toast_msg="Correlation matrix copied to clipboard",
    )


def show_factors_modal(
    formulas_df: pd.DataFrame,
    stats: dict | None = None,
    preview_df: pd.DataFrame | None = None,
    title: str = "Dataset Preview",
) -> None:
    @st.dialog(title, width="large")
    def _render() -> None:
        show_data = stats is not None and preview_df is not None

        if show_data:
            cols = st.columns(6, gap="small")
            stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"
            stat_items = [
                ("Rows", stats["num_rows"]),
                ("Dates", stats["num_dates"]),
                ("Columns", stats["num_columns"]),
            ]
            for col, (label, value) in zip(cols, stat_items):
                with col:
                    st.badge(label)
                    st.html(f"<p style='{stat_style}'>{value}</p>")

            if len(preview_df) > 20:
                first_10 = preview_df.head(10)
                last_10 = preview_df.tail(10)
                display_preview = pd.concat([first_10, last_10], ignore_index=False)
            else:
                display_preview = preview_df

            st.caption("Showing first and last 10 rows")

            display_df = display_preview.reset_index()
            display_df.rename(columns={"index": "Row"}, inplace=True)

            st.dataframe(
                display_df,
                height=500,
                width="stretch",
                hide_index=True,
                column_config={"Row": st.column_config.NumberColumn("Row", width=85)},
            )
        else:
            subset_cols = ["formula", "name", "tag"]
            subset = formulas_df[subset_cols]

            st.dataframe(
                subset,
                height=400,
                width="stretch",
                column_config=build_column_config([
                    ("formula", "text", "large"),
                    ("name", "text", "medium"),
                    ("tag", "text", "small"),
                ]),
                hide_index=True,
            )

            render_copy_download_buttons(
                csv_copy=subset.to_csv(index=False, sep="\t"),
                csv_download=subset.to_csv(index=False),
                file_name="dataset_factors.csv",
                key_prefix="factors_modal",
                toast_msg="Factors copied to clipboard",
            )

    _render()


def render_results_table(
    metrics: pd.DataFrame,
    factor_classifications: dict[str, str] | None = None,
    key: str = "results",
    rank_by: str = "Alpha",
) -> None:
    fl_id = st.query_params.get("fl_id")
    formulas_data = st.session_state.get("formulas_data")

    sort_col = "IC" if rank_by == "IC" else "annualized alpha %"
    sorted_metrics = metrics.sort_values(by=sort_col, key=abs, ascending=False).reset_index(drop=True)

    if 'rank' not in sorted_metrics.columns:
        sorted_metrics['rank'] = range(1, len(sorted_metrics) + 1)

    display = sorted_metrics.rename(
        columns={
            "column": "Factor",
            "annualized alpha %": "Ann. Alpha %",
            "annualized long %": "Ann. Long %",
            "annualized short %": "Ann. Short %",
            "NA %": "NA %",
            "T-Stat": "T-Stat",
            "beta": "Beta",
            "rank": "Rank",
            "IC": "IC",
            "IC t-stat": "IC t-stat",
        }
    )

    # Conditionally include columns based on rank_by selection
    if rank_by == "IC":
        display = display[
            ["Rank", "Factor", "IC", "IC t-stat", "Ann. Long %", "Ann. Short %", "NA %"]
        ]
    else:
        display = display[
            ["Rank", "Factor", "Ann. Alpha %", "Ann. Long %", "Ann. Short %", "Beta", "T-Stat", "NA %"]
        ]

    factor_names = display["Factor"].tolist()

    # format numeric columns as strings based on rank_by selection
    if rank_by == "IC":
        formatters = {
            "NA %": lambda x: f"{x:.1f}%",
            "IC": lambda x: f"{x:.4f}" if pd.notna(x) else "N/A",
            "IC t-stat": lambda x: f"{x:.2f}" if pd.notna(x) else "N/A",
            "Ann. Long %": lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A",
            "Ann. Short %": lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A",
        }
        column_config = build_column_config([
            ("Rank", "number", "small"),
            ("Factor", "text", "large"),
            ("IC", "text", "small"),
            ("IC t-stat", "text", "small"),
            ("Ann. Long %", "text", "small"),
            ("Ann. Short %", "text", "small"),
            ("NA %", "text", "small"),
        ])
    else:
        formatters = {
            "NA %": lambda x: f"{x:.1f}%",
            "Ann. Alpha %": lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A",
            "Ann. Long %": lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A",
            "Ann. Short %": lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A",
            "Beta": lambda x: f"{x:.4f}" if pd.notna(x) else "N/A",
            "T-Stat": lambda x: f"{x:.2f}" if pd.notna(x) else "N/A",
        }
        column_config = build_column_config([
            ("Rank", "number", "small"),
            ("Factor", "text", "large"),
            ("Ann. Alpha %", "text", "small"),
            ("Ann. Long %", "text", "small"),
            ("Ann. Short %", "text", "small"),
            ("Beta", "text", "small"),
            ("T-Stat", "text", "small"),
            ("NA %", "text", "small"),
        ])
    for col, fmt in formatters.items():
        display[col] = display[col].apply(fmt)

    if factor_classifications is not None:
        color_map = {k: v[0] for k, v in CLASSIFICATION_COLORS.items()}

        excluded_classification = "below_ic" if rank_by == "Alpha" else "below_alpha"

        legend_html = '<div style="display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;">'
        for key, (color, label) in CLASSIFICATION_COLORS.items():
            if key == excluded_classification:
                continue
            legend_html += f"""
            <div style="display: flex; align-items: center; gap: 6px;">
                <span style="
                    display: inline-block;
                    width: 16px;
                    height: 16px;
                    background-color: {color};
                    border: 1px solid #ccc;
                    border-radius: 3px;
                "></span>
                <span style="font-size: 13px; color: #555;">{label}</span>
            </div>
            """
        legend_html += "</div>"
        st.html(legend_html)

        def style_fn(row: pd.Series) -> list[str]:
            factor = factor_names[row.name]
            classification = factor_classifications.get(factor, "")
            color = color_map.get(classification, "")
            if color:
                return [f"background-color: {color}"] * len(row)
            return [""] * len(row)
    else:
        def style_fn(row: pd.Series) -> list[str]:
            color = "#f8f9fa" if row.name % 2 == 1 else "#ffffff"
            return [f"background-color: {color}"] * len(row)

    st.dataframe(
        display.style.apply(style_fn, axis=1),
        height=500,
        width="stretch",
        hide_index=True,
        column_config=column_config,
    )

    # Add formula and tag columns for both copy and download operations
    enriched_df = add_formula_and_tag_columns(display, formulas_data)
    csv_download = enriched_df.to_csv(index=False)

    file_name = f"{fl_id}_factors.csv" if fl_id else "factors.csv"
    render_copy_download_buttons(
        csv_copy=enriched_df.to_csv(index=False, sep="\t"),
        csv_download=csv_download,
        file_name=file_name,
        key_prefix=key,
        toast_msg="Factors copied to clipboard",
    )


def _format_params_json(params) -> str:
    rank_by = getattr(params, "rank_by", "Alpha")
    # if alpha is 0, avoid float rounding error
    clean_min_alpha = 0 if params.min_alpha < 1e-9 else params.min_alpha
    metric_str = f'"IC": {params.min_ic}' if rank_by == "IC" else f'"α": {clean_min_alpha}'
    return (
        f'{{"max.n": {params.n_factors}, {metric_str}, '
        f'"corr": {params.correlation_threshold}, '
        f'"top": "{int(params.top_pct)}%", "btm": "{int(params.bottom_pct)}%"}}'
    )


def render_history_table(analyses: list[AnalysisSummary]) -> None:
    fl_id = st.query_params.get("fl_id")
    datasets = BackupDatasetService(fl_id).load_all_versions()

    # Build rows data with version for alternating colors
    rows = []
    for a in analyses:
        dataset = datasets.get(a.dataset_version)

        if dataset:
            if dataset.type == DatasetType.DATE:
                period_value = (
                    format_date(dataset.asOfDt, "%Y-%m-%d") if dataset.asOfDt else "N/A"
                )
            else:
                period_value = f"{format_date(dataset.startDt, '%Y-%m-%d')} - {format_date(dataset.endDt, '%Y-%m-%d')}"
        else:
            period_value = "N/A"

        total_factors = len(dataset.formulas)
        best_factors = a.best_factors_count or 0
        factors_display = f"{best_factors}/{total_factors}"

        rows.append({
            "link": f"/results?fl_id={a.fl_id}&id={a.id}",
            "analysis_date": format_timestamp(a.created_at, "%Y-%m-%d %H:%M") + " UTC",
            "run_time": format_runtime(a.started_at, a.finished_at),
            "universe": dataset.universeName if dataset else "N/A",
            "factors": factors_display,
            "rows": f"{dataset.num_rows:,}" if dataset and dataset.num_rows else "N/A",
            "avg_abs_alpha": f"{a.avg_abs_alpha:.2f}%" if a.avg_abs_alpha is not None else "N/A",
            "period": period_value,
            "dataset_created": format_timestamp(a.dataset_version, "%Y-%m-%d %H:%M") + " UTC"
                + (" 🟢" if dataset and dataset.active else ""),
            "parameters": _format_params_json(a.params),
            "status": a.status.display,
            "notes": a.notes or "",
            "version": a.dataset_version,
        })

    # Alternate colors by dataset version
    unique_versions = list(dict.fromkeys(r["version"] for r in rows))
    version_colors = {v: "#f8f8f8" if i % 2 == 0 else "#ffffff" for i, v in enumerate(unique_versions)}

    # Build HTML table with clickable rows
    html = """
    <style>
        .history-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 12px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }
        .history-table th {
            background: #f0f2f6;
            padding: 8px 6px;
            text-align: left;
            font-weight: 600;
            border-bottom: 1px solid #ddd;
            border-right: 1px solid #ddd;
            white-space: nowrap;
        }
        .history-table th:last-child {
            border-right: none;
        }
        .history-table td {
            padding: 0;
            border-bottom: 1px solid #eee;
            border-right: 1px solid #eee;
        }
        .history-table td:last-child {
            border-right: none;
        }
        .history-table td a {
            display: block;
            padding: 6px;
            color: inherit;
            text-decoration: none;
        }
        .history-table tr:hover {
            filter: brightness(0.93);
        }
        .params-cell a, .date-cell a {
            font-family: monospace;
            font-size: 10px;
            color: #666;
        }
    </style>
    <div style="max-height: 400px; overflow-y: auto;">
    <table class="history-table">
        <thead>
            <tr>
                <th>Analysis Date</th>
                <th>Run Time</th>
                <th>Universe</th>
                <th>Best Factors</th>
                <th>Rows</th>
                <th>Avg|α|</th>
                <th>Period</th>
                <th>Dataset Created</th>
                <th>Parameters</th>
                <th>Status</th>
                <th>Notes</th>
            </tr>
        </thead>
        <tbody>
    """

    for row in rows:
        bg = version_colors[row["version"]]
        link = row["link"]
        html += f"""
            <tr style="background: {bg}">
                <td class="date-cell"><a href="{link}" target="_top">{row['analysis_date']}</a></td>
                <td><a href="{link}" target="_top">{row['run_time']}</a></td>
                <td><a href="{link}" target="_top">{row['universe']}</a></td>
                <td><a href="{link}" target="_top">{row['factors']}</a></td>
                <td><a href="{link}" target="_top">{row['rows']}</a></td>
                <td><a href="{link}" target="_top">{row['avg_abs_alpha']}</a></td>
                <td class="date-cell"><a href="{link}" target="_top">{row['period']}</a></td>
                <td class="date-cell"><a href="{link}" target="_top">{row['dataset_created']}</a></td>
                <td class="params-cell"><a href="{link}" target="_top">{row['parameters']}</a></td>
                <td><a href="{link}" target="_top">{row['status']}</a></td>
                <td><a href="{link}" target="_top">{row['notes'] or '&nbsp;'}</a></td>
            </tr>
        """

    html += "</tbody></table></div>"

    st.html(html)
