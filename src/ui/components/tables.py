from collections import Counter
from itertools import cycle
import polars as pl
import streamlit as st

from src.core.config.constants import CLASSIFICATION_COLORS
from src.core.types.models import AnalysisSummary, DatasetConfig
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
    "asc": "Asc",
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
    "Asc",
    "Tag",
    "Ann. Alpha %",
    "IC",
    "Beta",
    "T-Stat",
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
) -> str:
    counts = Counter(factor_classifications.values())
    items: list[str] = []
    for cls_key, (color, label) in CLASSIFICATION_COLORS.items():
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
    assert isinstance(formulas_data, pl.DataFrame)

    display = metrics.sort(pl.col(rank_by).abs(), descending=True).rename(
        COLUMN_RENAMES
    )

    tag_mapping = formulas_data.unique(subset=["name"]).select(["name", "tag"])
    display = display.join(
        tag_mapping, left_on="Factor", right_on="name", how="left"
    ).rename({"tag": "Tag"})

    display = display.with_columns(
        pl.when(pl.col("Asc") == 1).then(pl.lit("✓")).otherwise(pl.lit("")).alias("Asc")
    )

    display = display.select(DISPLAY_COLUMNS)

    row_colors = None
    if factor_classifications is not None:
        st.html(_build_legend_html(factor_classifications))
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


def _build_row(a: AnalysisSummary, ds: DatasetConfig) -> dict[str, str]:
    p = a.params
    active_marker = " 🟢" if ds.active else ""
    params_dict: dict[str, object] = {
        "max.n": p.n_factors,
        p.rank_by: p.min_rank_metric,
        "corr": p.correlation_threshold,
        "high": f"{int(p.high_quantile)}%",
        "low": f"{int(p.low_quantile)}%",
        "max.ret": f"{int(p.max_return_pct)}%",
    }
    return {
        "Analysis Date": format_timestamp(a.created_at, "%Y-%m-%d %H:%M UTC"),
        "Run Time": format_runtime(a.started_at, a.finished_at),
        "Universe": ds.universeName,
        "Best Factors": f"{a.best_factors_count or 0}/{len(ds.formulas)}",
        "Rows": f"{ds.numRows:,}",
        "Avg|α|": f"{(a.avg_abs_alpha or 0):.2f}%",
        "Period": f"{format_date(ds.startDt, '%Y-%m-%d')} — {format_date(ds.endDt, '%Y-%m-%d')}",
        "Dataset Created": format_timestamp(a.dataset_version, "%Y-%m-%d %H:%M UTC")
        + active_marker,
        "Parameters": str(params_dict),
        "Status": a.status.display,
        "Notes": a.notes or "",
    }


def render_history_table(analyses: list[AnalysisSummary]) -> None:
    if not analyses:
        st.info("No analyses found.")
        return

    datasets = BackupDatasetService(
        st.session_state["dataset_details"]
    ).load_all_versions()

    rows_data: list[dict[str, str]] = []
    row_colors: list[str] = []
    row_links: list[str] = []

    version_colors: dict[str, str] = {}
    colors = cycle(("#f8f8f8", "#ffffff"))

    for a in analyses:
        dataset = datasets[a.dataset_version]

        if a.dataset_version not in version_colors:
            version_colors[a.dataset_version] = next(colors)

        row_colors.append(version_colors[a.dataset_version])

        row_links.append(f"/results?fl_id={a.fl_id}&id={a.id}")
        rows_data.append(_build_row(a, dataset))

    df = pl.DataFrame(rows_data)

    render_table(
        df,
        row_colors=row_colors,
        row_links=row_links,
        max_height=400,
    )
