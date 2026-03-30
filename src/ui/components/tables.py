from collections import Counter
from itertools import cycle

import polars as pl
import streamlit as st

from src.core.config.constants import CLASSIFICATION_COLORS, RANK_CONFIG
from src.core.types.models import AnalysisSummary
from src.core.utils.common import (
    add_formula_column,
    format_date,
    format_runtime,
    format_timestamp,
)
from src.services.dataset_service import BackupDatasetService
from src.ui.components.common import render_copy_download_buttons
from src.ui.components.table_builder import render_table

COLUMN_RENAMES = {
    "column": "Factor",
    "annualized_alpha_pct": "Ann. Alpha %",
    "annualized_long_pct": "Ann. Long %",
    "annualized_short_pct": "Ann. Short %",
    "na_pct": "NA %",
    "t_stat": "T-Stat",
    "beta": "Beta",
    "rank": "Rank",
    "ic": "IC",
    "ic_t_stat": "IC t-stat",
}

DISPLAY_COLUMNS = [
    "Rank",
    "Factor",
    "Tag",
    "Ann. Alpha %",
    "Beta",
    "T-Stat",
    "IC",
    "IC t-stat",
    "Ann. Long %",
    "Ann. Short %",
    "NA %",
]

FORMAT_SPEC = {
    "Ann. Alpha %": ".2f%",
    "Ann. Long %": ".2f%",
    "Ann. Short %": ".2f%",
    "NA %": ".1f%",
    "Beta": ".4f",
    "T-Stat": ".2f",
    "IC": ".4f",
    "IC t-stat": ".2f",
}


def _build_legend_html(
    factor_classifications: dict[str, str],
    rank_by: str,
) -> str:
    counts = Counter(factor_classifications.values())
    items = []
    for cls_key, (color, label) in CLASSIFICATION_COLORS.items():
        if cls_key == f"below_{rank_by}":
            continue
        items.append(
            f'<div style="display: flex; align-items: center; gap: 6px;">'
            f'<span style="display: inline-block; width: 16px; height: 16px; '
            f"background-color: {color}; border: 1px solid #ccc; border-radius: 3px;"
            f'"></span>'
            f'<span style="font-size: 13px; color: #555;">{label} ({counts.get(cls_key, 0)})</span>'
            f"</div>"
        )
    return (
        '<div style="display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;">'
        + "".join(items)
        + "</div>"
    )


def render_correlation_matrix(
    corr_matrix_df: pl.DataFrame,
    title: str,
    file_prefix: str | None = None,
    key_suffix: str = "",
) -> None:
    st.subheader(title)

    if "factor" in corr_matrix_df.columns:
        corr_matrix_df = corr_matrix_df.rename({"factor": ""})

    render_table(
        corr_matrix_df,
        max_height=400,
        small_headers=True,
        column_widths={"": "180px"},
        format_spec={col: ".4f" for col in corr_matrix_df.columns[1:]},
    )

    render_copy_download_buttons(
        csv_copy=corr_matrix_df.write_csv(separator="\t"),
        csv_download=corr_matrix_df.write_csv(),
        file_name=(
            f"{file_prefix}_correlation_matrix.csv"
            if file_prefix
            else "correlation_matrix.csv"
        ),
        key_prefix=f"corr_matrix{key_suffix}",
        toast_msg="Correlation matrix copied to clipboard",
    )


def show_preview_modal(
    data: pl.DataFrame, num_rows: int, num_dates: int, formula_count: int
) -> None:
    @st.dialog("Dataset Preview", width="large")
    def _render() -> None:
        stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"
        stat_items = [
            ("Rows", num_rows),
            ("Dates", num_dates),
            ("Formulas", formula_count),
        ]
        for col, (label, value) in zip(st.columns(6, gap="small"), stat_items):
            with col:
                st.badge(label)
                st.html(f"<p style='{stat_style}'>{value}</p>")

        st.caption("Showing first and last 10 rows")
        render_table(
            data.rename({"_row_idx": "Row"}),
            max_height=500,
            column_widths={
                "Row": "40px",
                "Date": "95px",
                "P123 ID": "60px",
                "Ticker": "55px",
            },
        )

    _render()


@st.dialog("Dataset Formulas", width="large")
def show_formulas_modal(formulas_df: pl.DataFrame) -> None:
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


def render_results_table(
    metrics: pl.DataFrame,
    factor_classifications: dict[str, str] | None = None,
    key: str = "results",
    rank_by: str = "annualized_alpha_pct",
    sortable: bool = False,
) -> None:
    fl_id = st.query_params.get("fl_id")
    formulas_data = st.session_state.get("formulas_data")

    display = metrics.sort(pl.col(rank_by).abs(), descending=True).rename(
        COLUMN_RENAMES
    )

    if formulas_data is not None:
        tag_mapping = formulas_data.unique(subset=["name"]).select(["name", "tag"])
        display = display.join(
            tag_mapping, left_on="Factor", right_on="name", how="left"
        ).rename({"tag": "Tag"})
    else:
        display = display.with_columns(pl.lit("").alias("Tag"))

    display = display.select(DISPLAY_COLUMNS)

    row_colors = None
    if factor_classifications is not None:
        st.html(_build_legend_html(factor_classifications, rank_by))
        row_colors = [
            CLASSIFICATION_COLORS.get(factor_classifications[f], ("#ffffff", None))[0]
            for f in display["Factor"].to_list()
        ]

    render_table(
        display,
        row_colors=row_colors,
        format_spec=FORMAT_SPEC,
        max_height=500,
        zebra=True,
        sortable=sortable,
    )

    enriched_df = add_formula_column(display, formulas_data)
    render_copy_download_buttons(
        csv_copy=enriched_df.write_csv(separator="\t"),
        csv_download=enriched_df.write_csv(),
        file_name=f"{fl_id}_factors.csv",
        key_prefix=key,
        toast_msg="Factors copied to clipboard",
    )


def render_history_table(analyses: list[AnalysisSummary]) -> None:
    datasets = BackupDatasetService(
        st.session_state["dataset_details"]
    ).load_all_versions()

    rows_data = []
    links = []
    version_list = []

    for a in analyses:
        dataset = datasets.get(a.dataset_version)

        links.append(f"/results?fl_id={a.fl_id}&id={a.id}")
        version_list.append(a.dataset_version)

        rows_data.append(
            {
                "Analysis Date": format_timestamp(a.created_at, "%Y-%m-%d %H:%M UTC"),
                "Run Time": format_runtime(a.started_at, a.finished_at),
                "Universe": dataset.universeName,
                "Best Factors": f"{a.best_factors_count or 0}/{len(dataset.formulas)}",
                "Rows": f"{dataset.numRows:,}",
                "Avg|α|": f"{(a.avg_abs_alpha or 0):.2f}%",
                "Period": f"{format_date(dataset.startDt, '%Y-%m-%d')} — {format_date(dataset.endDt, '%Y-%m-%d')}",
                "Dataset Created": (
                    format_timestamp(a.dataset_version, "%Y-%m-%d %H:%M UTC")
                    + (" 🟢" if dataset and dataset.active else "")
                ),
                "Parameters": (
                    f'{{"max.n": {a.params.n_factors}, '
                    f'"{a.params.rank_by}": {getattr(a.params, f"min_{a.params.rank_by}")}, '
                    f'"corr": {a.params.correlation_threshold}, '
                    f'"top": "{int(a.params.top_pct)}%", "btm": "{int(a.params.bottom_pct)}%", '
                    f'"max.ret": "{int(a.params.max_return_pct)}%"}}'
                ),
                "Status": a.status.display,
                "Notes": a.notes or "",
            }
        )

    if not rows_data:
        st.info("No analyses found.")
        return

    df = pl.DataFrame(rows_data)

    unique_versions = list(dict.fromkeys(version_list))
    version_color_map = {
        v: c for v, c in zip(unique_versions, cycle(("#f8f8f8", "#ffffff")))
    }

    render_table(
        df,
        row_colors=[version_color_map[v] for v in version_list],
        row_links=links,
        max_height=400,
    )
