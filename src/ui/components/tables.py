import streamlit as st
import polars as pl
import polars.selectors as cs
import plotly.graph_objects as go

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


NARROW_COLUMNS = ("Rank", "#", "Row")


def render_table(
    df: pl.DataFrame,
    row_colors: list[str] | None = None,
    height: int | None = None,
    min_height: int = 200,
    max_height: int = 600,
    key: str = "table",
) -> None:
    headers = df.columns
    values = [df[col].to_list() for col in headers]

    if row_colors is None:
        row_colors = ["white"] * len(df)

    fig = go.Figure(data=[go.Table(
        columnwidth=[30 if col in NARROW_COLUMNS else 80 for col in headers],
        header=dict(
            values=[f"<b>{h}</b>" for h in headers],
            fill_color="#f8f9fa",
            font=dict(size=11, color="#333", family="system-ui, -apple-system, sans-serif"),
            align="left",
            height=32,
            line=dict(color="#dee2e6", width=1),
        ),
        cells=dict(
            values=values,
            fill_color=[row_colors],
            font=dict(size=11, color="#333", family="system-ui, -apple-system, sans-serif"),
            align="left",
            height=28,
            line=dict(color="#dee2e6", width=1),
        ),
    )])

    if height is None:
        height = max(min_height, min(max_height, 50 + len(df) * 28))

    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=height)
    st.plotly_chart(fig, use_container_width=True, key=f"plotly_{key}")


def render_correlation_matrix(
    corr_matrix_df: pl.DataFrame,
    title: str,
    file_prefix: str | None = None,
    key_suffix: str = "",
) -> None:
    st.subheader(title)

    rounded = corr_matrix_df.with_columns(cs.numeric().round(4))

    render_table(rounded, min_height=200, max_height=400, key=f"corr_matrix{key_suffix}")

    file_name = f"{file_prefix}_correlation_matrix.csv" if file_prefix else "correlation_matrix.csv"

    csv_content = rounded.write_csv()
    csv_copy = rounded.write_csv(separator="\t")
    render_copy_download_buttons(
        csv_copy=csv_copy,
        csv_download=csv_content,
        file_name=file_name,
        key_prefix=f"corr_matrix{key_suffix}",
        toast_msg="Correlation matrix copied to clipboard",
    )


def show_factors_modal(
    formulas_df: pl.DataFrame,
    stats: dict | None = None,
    preview_df: pl.DataFrame | None = None,
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
                display_preview = pl.concat([first_10, last_10])
            else:
                display_preview = preview_df

            st.caption("Showing first and last 10 rows")

            # Use _row_idx if present, otherwise create Row column
            if "_row_idx" in display_preview.columns:
                display_pl = display_preview.rename({"_row_idx": "Row"})
            else:
                display_pl = display_preview.with_row_index("Row")

            st.dataframe(
                display_pl,
                height=500,
                use_container_width=True,
                hide_index=True,
                column_config={"Row": st.column_config.NumberColumn("Row", width=85)},
            )
        else:
            subset = formulas_df.select(["formula", "name", "tag"])

            render_table(subset, min_height=300, max_height=400, key="factors_modal")

            render_copy_download_buttons(
                csv_copy=subset.write_csv(separator="\t"),
                csv_download=subset.write_csv(),
                file_name="dataset_factors.csv",
                key_prefix="factors_modal",
                toast_msg="Factors copied to clipboard",
            )

    _render()


def _get_row_colors(factors: list[str], classifications: dict[str, str]) -> list[str]:
    colors = []
    for factor in factors:
        classification = classifications.get(factor, "")
        if classification in CLASSIFICATION_COLORS:
            color, _ = CLASSIFICATION_COLORS[classification]
            colors.append(color)
        else:
            colors.append("")
    return colors


def render_results_table(
    metrics: pl.DataFrame,
    factor_classifications: dict[str, str] | None = None,
    key: str = "results",
    rank_by: str = "Alpha",
) -> None:
    fl_id = st.query_params.get("fl_id")
    formulas_data = st.session_state.get("formulas_data")

    sort_col = "IC" if rank_by == "IC" else "annualized alpha %"
    sorted_metrics = metrics.sort(pl.col(sort_col).abs(), descending=True)

    if 'rank' not in sorted_metrics.columns:
        sorted_metrics = sorted_metrics.with_row_index("rank", offset=1)

    display = sorted_metrics.rename({
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
    })

    display = display.select([
        "Rank", "Factor", "Ann. Alpha %", "Beta", "T-Stat", "IC", "IC t-stat", "Ann. Long %", "Ann. Short %", "NA %"
    ])

    # legend with classifications
    row_colors = None
    if factor_classifications is not None:
        excluded_classification = "below_ic" if rank_by == "Alpha" else "below_alpha"
        classification_counts = {}
        for classification in factor_classifications.values():
            classification_counts[classification] = classification_counts.get(classification, 0) + 1

        legend_html = '<div style="display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;">'
        for key, (color, label) in CLASSIFICATION_COLORS.items():
            if key == excluded_classification:
                continue
            count = classification_counts.get(key, 0)
            label_with_count = f"{label} ({count})"
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
                <span style="font-size: 13px; color: #555;">{label_with_count}</span>
            </div>
            """
        legend_html += "</div>"
        st.html(legend_html)

        row_colors = _get_row_colors(display["Factor"].to_list(), factor_classifications)
        row_colors = [c if c else "white" for c in row_colors]

    render_table(display, row_colors=row_colors, min_height=500, max_height=600, key=key)

    export_display = display
    enriched_df = add_formula_and_tag_columns(export_display, formulas_data)
    csv_download = enriched_df.write_csv()

    file_name = f"{fl_id}_factors.csv" if fl_id else "factors.csv"
    render_copy_download_buttons(
        csv_copy=enriched_df.write_csv(separator="\t"),
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
    datasets = BackupDatasetService(st.session_state.get("user_uid"), fl_id).load_all_versions()

    rows_data = []
    for a in analyses:
        dataset = datasets.get(a.dataset_version)

        if dataset:
            if dataset.type == DatasetType.DATE:
                period_value = (
                    format_date(dataset.asOfDt, "%Y-%m-%d") if dataset.asOfDt else "N/A"
                )
            else:
                period_value = f"{format_date(dataset.startDt, '%Y-%m-%d')} — {format_date(dataset.endDt, '%Y-%m-%d')}"
        else:
            period_value = "N/A"

        total_factors = len(dataset.formulas) if dataset else 0
        best_factors = a.best_factors_count or 0
        factors_display = f"{best_factors}/{total_factors}"

        link = f"/results?fl_id={a.fl_id}&id={a.id}"

        rows_data.append({
            "View": link,
            "Analysis Date": format_timestamp(a.created_at, "%Y-%m-%d %H:%M") + " UTC",
            "Run Time": format_runtime(a.started_at, a.finished_at),
            "Universe": dataset.universeName if dataset else "N/A",
            "Best Factors": factors_display,
            "Rows": f"{dataset.num_rows:,}" if dataset and dataset.num_rows else "N/A",
            "Avg|α|": f"{a.avg_abs_alpha:.2f}%" if a.avg_abs_alpha is not None else "N/A",
            "Period": period_value,
            "Dataset Created": format_timestamp(a.dataset_version, "%Y-%m-%d %H:%M") + " UTC"
                + (" 🟢" if dataset and dataset.active else ""),
            "Parameters": _format_params_json(a.params),
            "Status": a.status.display,
            "Notes": a.notes or "",
        })

    if not rows_data:
        st.info("No analyses found.")
        return

    df = pl.DataFrame(rows_data)

    st.dataframe(
        df,
        height=min(500, 50 + len(df) * 35),
        use_container_width=True,
        hide_index=True,
        column_config={
            "View": st.column_config.LinkColumn(
                " ",
                display_text="→",
                width=50,
            ),
            "Analysis Date": st.column_config.TextColumn("Analysis Date", width="medium"),
            "Run Time": st.column_config.TextColumn("Run Time", width="small"),
            "Universe": st.column_config.TextColumn("Universe", width="medium"),
            "Best Factors": st.column_config.TextColumn("Best Factors", width="small"),
            "Rows": st.column_config.TextColumn("Rows", width="small"),
            "Avg|α|": st.column_config.TextColumn("Avg|α|", width="small"),
            "Period": st.column_config.TextColumn("Period", width="medium"),
            "Dataset Created": st.column_config.TextColumn("Dataset Created", width="medium"),
            "Parameters": st.column_config.TextColumn("Parameters", width="medium"),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Notes": st.column_config.TextColumn("Notes", width="medium"),
        },
    )
