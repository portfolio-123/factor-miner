import streamlit as st
import polars as pl
import polars.selectors as cs

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
from src.ui.components.table_builder import render_table


def render_correlation_matrix(
    corr_matrix_df: pl.DataFrame,
    title: str,
    file_prefix: str | None = None,
    key_suffix: str = "",
) -> None:
    st.subheader(title)

    rounded = corr_matrix_df.with_columns(cs.numeric().round(4))

    if "factor" in rounded.columns:
        rounded = rounded.rename({"factor": ""})

    render_table(rounded, max_height=400, small_headers=True)

    file_name = f"{file_prefix}_correlation_matrix.csv" if file_prefix else "correlation_matrix.csv"

    render_copy_download_buttons(
        csv_copy=rounded.write_csv(separator="\t"),
        csv_download=rounded.write_csv(),
        file_name=file_name,
        key_prefix=f"corr_matrix{key_suffix}",
        toast_msg="Correlation matrix copied to clipboard",
    )


def show_preview_modal(
    preview_df: pl.DataFrame,
    stats: dict,
    title: str = "Dataset Preview",
) -> None:
    @st.dialog(title, width="large")
    def _render() -> None:
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

        st.caption("Showing first and last 10 rows")

        display_df = preview_df.rename({"_row_idx": "Row"})
        render_table(
            display_df,
            max_height=500,
            column_widths={"Row": "40px", "Date": "85px", "P123 ID": "60px", "Ticker": "55px"},
        )

    _render()


def show_formulas_modal(
    formulas_df: pl.DataFrame,
    title: str = "Dataset Formulas",
) -> None:
    @st.dialog(title, width="large")
    def _render() -> None:
        subset = formulas_df.select(["formula", "name", "tag"])

        render_table(
            subset,
            max_height=400,
            zebra=True,
            column_widths={"formula": "45%", "name": "35%", "tag": "20%"},
        )

        render_copy_download_buttons(
            csv_copy=subset.write_csv(separator="\t"),
            csv_download=subset.write_csv(),
            file_name="dataset_factors.csv",
            key_prefix="factors_modal",
            toast_msg="Factors copied to clipboard",
        )

    _render()


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

    row_colors = None
    if factor_classifications is not None:
        excluded_classification = "below_ic" if rank_by == "Alpha" else "below_alpha"
        classification_counts = {}
        for classification in factor_classifications.values():
            classification_counts[classification] = classification_counts.get(classification, 0) + 1

        legend_html = '<div style="display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;">'
        for cls_key, (color, label) in CLASSIFICATION_COLORS.items():
            if cls_key == excluded_classification:
                continue
            count = classification_counts.get(cls_key, 0)
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

        row_colors = []
        for factor in display["Factor"].to_list():
            classification = factor_classifications.get(factor, "")
            if classification in CLASSIFICATION_COLORS:
                color, _ = CLASSIFICATION_COLORS[classification]
                row_colors.append(color)
            else:
                row_colors.append("#ffffff")

    render_table(
        display,
        row_colors=row_colors,
        format_spec={
            "Ann. Alpha %": ".2f%",
            "Ann. Long %": ".2f%",
            "Ann. Short %": ".2f%",
            "NA %": ".1f%",
            "Beta": ".4f",
            "T-Stat": ".2f",
            "IC": ".4f",
            "IC t-stat": ".2f",
        },
        max_height=500,
        zebra=True,
    )

    enriched_df = add_formula_and_tag_columns(display, formulas_data)
    csv_download = enriched_df.write_csv()

    file_name = f"{fl_id}_factors.csv" if fl_id else "factors.csv"
    render_copy_download_buttons(
        csv_copy=enriched_df.write_csv(separator="\t"),
        csv_download=csv_download,
        file_name=file_name,
        key_prefix=key,
        toast_msg="Factors copied to clipboard",
    )


def render_history_table(analyses: list[AnalysisSummary]) -> None:
    fl_id = st.query_params.get("fl_id")
    datasets = BackupDatasetService(st.session_state.get("user_uid"), fl_id).load_all_versions()

    rows_data = []
    links = []
    version_list = []

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

        links.append(f"/results?fl_id={a.fl_id}&id={a.id}")
        version_list.append(a.dataset_version)

        rank_by = getattr(a.params, "rank_by", "Alpha")
        clean_min_alpha = 0 if a.params.min_alpha < 1e-9 else a.params.min_alpha
        metric_str = f'"IC": {a.params.min_ic}' if rank_by == "IC" else f'"α": {clean_min_alpha}'
        params_json = (
            f'{{"max.n": {a.params.n_factors}, {metric_str}, '
            f'"corr": {a.params.correlation_threshold}, '
            f'"top": "{int(a.params.top_pct)}%", "btm": "{int(a.params.bottom_pct)}%"}}'
        )

        rows_data.append({
            "Analysis Date": format_timestamp(a.created_at, "%Y-%m-%d %H:%M") + " UTC",
            "Run Time": format_runtime(a.started_at, a.finished_at),
            "Universe": dataset.universeName if dataset else "N/A",
            "Best Factors": factors_display,
            "Rows": f"{dataset.num_rows:,}" if dataset and dataset.num_rows else "N/A",
            "Avg|α|": f"{a.avg_abs_alpha:.2f}%" if a.avg_abs_alpha is not None else "N/A",
            "Period": period_value,
            "Dataset Created": format_timestamp(a.dataset_version, "%Y-%m-%d %H:%M") + " UTC"
                + (" 🟢" if dataset and dataset.active else ""),
            "Parameters": params_json,
            "Status": a.status.display,
            "Notes": a.notes or "",
        })

    if not rows_data:
        st.info("No analyses found.")
        return

    df = pl.DataFrame(rows_data)

    unique_versions = list(dict.fromkeys(version_list))
    version_color_map = {v: "#f8f8f8" if i % 2 == 0 else "#ffffff" for i, v in enumerate(unique_versions)}
    row_colors = [version_color_map[v] for v in version_list]

    render_table(
        df,
        row_colors=row_colors,
        row_links=links,
        max_height=400,
    )
