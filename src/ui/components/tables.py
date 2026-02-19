import streamlit as st
import pandas as pd
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured

from src.core.types.models import AnalysisSummary, DatasetType
from src.core.utils.common import (
    add_formula_and_tag_columns,
    format_date,
    format_runtime,
    format_timestamp,
)
from src.services.dataset_service import BackupDatasetService


def render_correlation_matrix(
    corr_matrix_df: pd.DataFrame,
    title: str,
    file_prefix: str | None = None,
    key_suffix: str = "",
) -> None:
    st.subheader(title)
    st.dataframe(
        corr_matrix_df.round(4),
        height=min(400, 50 + len(corr_matrix_df) * 35),
        width="stretch",
    )

    _, col1, col2 = st.columns([3, 1, 1])
    corr_csv_copy = corr_matrix_df.round(4).to_csv(sep="\t")
    corr_csv_download = corr_matrix_df.round(4).to_csv()

    with col1:
        if st.button(
            type="primary",
            label="Copy to Clipboard",
            width="stretch",
            key=f"corr_matrix_copy{key_suffix}",
        ):
            copy_to_clipboard_unsecured(corr_csv_copy)
            copy_to_clipboard(corr_csv_copy)
            st.toast("Correlation matrix copied to clipboard")

    with col2:
        file_name = (
            f"{file_prefix}_correlation_matrix.csv"
            if file_prefix
            else "correlation_matrix.csv"
        )
        st.download_button(
            type="primary",
            label="Download CSV",
            data=corr_csv_download,
            file_name=file_name,
            mime="text/csv",
            width="stretch",
            key=f"corr_matrix_download{key_suffix}",
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
            # Show data preview directly (no tabs)
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
            # Show formulas directly (no tabs)
            st.dataframe(
                formulas_df[["formula", "name", "tag"]],
                height=400,
                width="stretch",
                column_config={
                    "formula": st.column_config.TextColumn("Formula", width="large"),
                    "name": st.column_config.TextColumn("Name", width="medium"),
                    "tag": st.column_config.TextColumn("Tag", width="small"),
                },
                hide_index=True,
            )

            _, col1, col2 = st.columns([3, 1, 1])

            csv_to_copy = formulas_df[["formula", "name", "tag"]].to_csv(
                index=False, sep="\t"
            )
            csv_to_download = formulas_df[["formula", "name", "tag"]].to_csv(
                index=False
            )

            with col1:
                if st.button(
                    type="primary", label="Copy to Clipboard", width="stretch"
                ):
                    copy_to_clipboard_unsecured(csv_to_copy)
                    copy_to_clipboard(csv_to_copy)
                    st.toast("Factors copied to clipboard")

            with col2:
                st.download_button(
                    type="primary",
                    label="Download CSV",
                    data=csv_to_download,
                    file_name="dataset_factors.csv",
                    mime="text/csv",
                    width="stretch",
                )

    _render()


def render_results_table(
    metrics: pd.DataFrame,
    factor_classifications: dict[str, str] | None = None,
    key: str = "results",
) -> None:
    fl_id = st.query_params.get("fl_id")
    formulas_data = st.session_state.get("formulas_data")

    sorted_metrics = metrics.sort_values(
        by="annualized alpha %", key=abs, ascending=False
    ).reset_index(drop=True)

    sorted_metrics = sorted_metrics.copy()

    if "rank" not in sorted_metrics.columns:
        sorted_metrics["rank"] = range(1, len(sorted_metrics) + 1)

    display = sorted_metrics.rename(
        columns={
            "column": "Factor",
            "annualized alpha %": "Ann. Alpha %",
            "NA %": "NA %",
            "p-value": "P-Value",
            "beta": "Beta",
            "rank": "Rank",
            "IC": "IC",
            "IC t-stat": "IC t-stat",
        }
    )

    display = display[
        ["Rank", "Factor", "Ann. Alpha %", "Beta", "P-Value", "IC", "IC t-stat", "NA %"]
    ]

    factor_names = display["Factor"].tolist()

    # format numeric columns as strings
    formatters = {
        "NA %": lambda x: f"{x:.1f}%",
        "Ann. Alpha %": lambda x: f"{x:.2f}%",
        "Beta": lambda x: f"{x:.4f}",
        "P-Value": lambda x: f"{x:.6f}",
        "IC": lambda x: f"{x:.4f}" if pd.notna(x) else "N/A",
        "IC t-stat": lambda x: f"{x:.2f}" if pd.notna(x) else "N/A",
    }
    for col, fmt in formatters.items():
        display[col] = display[col].apply(fmt)

    column_config = {
        "Rank": st.column_config.NumberColumn("Rank", width="small"),
        "Factor": st.column_config.TextColumn("Factor", width="large"),
        "NA %": st.column_config.TextColumn("NA %", width="small"),
        "Ann. Alpha %": st.column_config.TextColumn("Ann. Alpha %", width="small"),
        "Beta": st.column_config.TextColumn("Beta", width="small"),
        "P-Value": st.column_config.TextColumn("P-Value", width="small"),
        "IC": st.column_config.TextColumn("IC", width="small"),
        "IC t-stat": st.column_config.TextColumn("IC t-stat", width="small"),
    }

    if factor_classifications is not None:
        color_map = {
            "best": "#a5d6a7",  # Green
            "correlation_conflict": "#ef9a9a",  # Red
            "n_limit": "#b0bec5",  # Blue-gray
            "below_alpha": "#ffcc80",  # Orange
            "high_na": "#fff59d",  # Yellow
            "below_ic": "#ce93d8",  # Purple
        }

        legend_items = [
            ("best", "#a5d6a7", "Best Factor"),
            ("correlation_conflict", "#ef9a9a", "Correlation Conflict"),
            ("high_na", "#fff59d", "High NA %"),
            ("below_alpha", "#ffcc80", "Below Min Alpha"),
            ("below_ic", "#ce93d8", "Below Min IC"),
            ("n_limit", "#b0bec5", "N Limit Reached"),
        ]
        legend_html = """
        <div style="display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap;">
        """
        for _, color, label in legend_items:
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

        def color_row(row: pd.Series) -> list[str]:
            factor = factor_names[row.name]
            classification = factor_classifications.get(factor, "")
            color = color_map.get(classification, "")
            if color:
                return [f"background-color: {color}"] * len(row)
            return [""] * len(row)

        styled_display = display.style.apply(color_row, axis=1)
        st.dataframe(
            styled_display,
            height=500,
            width="stretch",
            hide_index=True,
            column_config=column_config,
        )
    else:

        def alternate_row_colors(row: pd.Series) -> list[str]:
            color = "#f8f9fa" if row.name % 2 == 1 else "#ffffff"
            return [f"background-color: {color}"] * len(row)

        styled_display = display.style.apply(alternate_row_colors, axis=1)
        st.dataframe(
            styled_display,
            height=500,
            width="stretch",
            hide_index=True,
            column_config=column_config,
        )

    _, col1, col2 = st.columns([3, 1, 1])

    # Add formula and tag columns for both copy and download operations
    enriched_df = add_formula_and_tag_columns(display, formulas_data)

    # tab delimited for copy to clipboard (with Formula and Tag)
    csv_to_copy = enriched_df.to_csv(index=False, sep="\t")

    with col1:
        if st.button(
            type="primary",
            label="Copy to Clipboard",
            width="stretch",
            key=f"{key}_copy",
        ):
            copy_to_clipboard_unsecured(csv_to_copy)
            copy_to_clipboard(csv_to_copy)
            st.toast("Factors copied to clipboard")

    with col2:
        file_name = f"{fl_id}_factors.csv" if fl_id else "factors.csv"
        st.download_button(
            type="primary",
            label="Download CSV",
            data=enriched_df.to_csv(index=False),
            file_name=file_name,
            mime="text/csv",
            width="stretch",
            key=f"{key}_download",
        )


def render_history_table(analyses: list[AnalysisSummary]) -> None:
    fl_id = st.query_params.get("fl_id")
    datasets = BackupDatasetService(fl_id).load_all_versions()

    data = []
    for a in analyses:
        dataset = datasets.get(a.dataset_version)

        if dataset:
            if dataset.type == DatasetType.DATE:
                period_value = (
                    format_date(dataset.asOfDt, "%Y/%m/%d") if dataset.asOfDt else "N/A"
                )
            else:
                period_value = f"{format_date(dataset.startDt, '%Y/%m/%d')} - {format_date(dataset.endDt, '%Y/%m/%d')}"
        else:
            period_value = "N/A"

        data.append(
            {
                "": f"/results?fl_id={a.fl_id}&id={a.id}",
                "Analysis Date": format_timestamp(
                    a.created_at, "%b %d, %Y at %I:%M %p UTC"
                ),
                "Run Time": format_runtime(a.started_at, a.finished_at),
                "Universe": dataset.universeName if dataset else "N/A",
                "Factors": (
                    len(dataset.formulas) if dataset and dataset.formulas else "N/A"
                ),
                "Avg Abs Alpha": (
                    f"{a.avg_abs_alpha:.2f}%" if a.avg_abs_alpha is not None else "N/A"
                ),
                "Period": period_value,
                "Dataset Created": format_timestamp(a.dataset_version)
                + (" 🟢" if dataset and dataset.active else ""),
                "Status": a.status.display,
                "Notes": a.notes or "",
                "_dataset_version": a.dataset_version,
            }
        )

    df = pd.DataFrame(data)

    # alternate color mapping to differenciate between datasets
    unique_versions = df["_dataset_version"].unique()
    version_colors = {
        v: "#f5f5f5" if i % 2 == 0 else "#ffffff" for i, v in enumerate(unique_versions)
    }

    # map each row index to its color based on dataset
    row_colors = df["_dataset_version"].map(version_colors)
    display_df = df.drop(columns=["_dataset_version"])

    def color_row(row: pd.Series) -> list[str]:
        color = row_colors[row.name]
        return [f"background-color: {color}"] * len(row)

    styled_df = display_df.style.apply(color_row, axis=1)

    st.dataframe(
        styled_df,
        height=400,
        width="stretch",
        hide_index=True,
        column_config={
            "": st.column_config.LinkColumn("", display_text="View →", width="small"),
            "Analysis Date": st.column_config.TextColumn(
                "Analysis Date", width="medium"
            ),
            "Run Time": st.column_config.TextColumn("Run Time", width="small"),
            "Universe": st.column_config.TextColumn("Universe", width="medium"),
            "Factors": st.column_config.NumberColumn("Factors", width="small"),
            "Avg Abs Alpha": st.column_config.TextColumn(
                "Avg Abs Alpha", width="small"
            ),
            "Period": st.column_config.TextColumn("Period", width="medium"),
            "Dataset Created": st.column_config.TextColumn(
                "Dataset Created", width="medium"
            ),
            "Status": st.column_config.TextColumn("Status", width="small"),
            "Notes": st.column_config.TextColumn("Notes", width="small"),
        },
    )
