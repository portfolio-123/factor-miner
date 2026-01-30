import streamlit as st
import pandas as pd
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured

from src.core.types import AnalysisSummary
from src.core.utils import add_formula_and_tag_columns, format_date, format_runtime, format_timestamp
from src.services.dataset_service import dataset_service


def show_factors_modal(
    formulas_df: pd.DataFrame,
    stats: dict | None = None,
    preview_df: pd.DataFrame | None = None,
) -> None:
    @st.dialog("Dataset Preview", width="large")
    def _render() -> None:
        show_data_tab = stats is not None and preview_df is not None

        if show_data_tab:
            factors_tab, data_tab = st.tabs(["Factors", "Data"])
        else:
            factors_tab = st.container()

        with factors_tab:
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

        if show_data_tab:
            with data_tab:
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

    if 'rank' not in sorted_metrics.columns:
        sorted_metrics['rank'] = range(1, len(sorted_metrics) + 1)

    display = sorted_metrics.rename(
        columns={
            "column": "Factor",
            "annualized alpha %": "Ann. Alpha %",
            "T Statistic": "T-Statistic",
            "p-value": "P-Value",
            "beta": "Beta",
            "rank": "Rank",
        }
    )

    display = display[["Rank", "Factor", "Ann. Alpha %", "Beta", "T-Statistic", "P-Value"]]

    factor_names = display["Factor"].tolist()

    # format numeric columns as strings
    formatters = {
        "Ann. Alpha %": lambda x: f"{x:.2f}%",
        "Beta": lambda x: f"{x:.4f}",
        "T-Statistic": lambda x: f"{x:.4f}",
        "P-Value": lambda x: f"{x:.6f}",
    }
    for col, fmt in formatters.items():
        display[col] = display[col].apply(fmt)

    column_config = {
        "Rank": st.column_config.NumberColumn("Rank", width="small"),
        "Factor": st.column_config.TextColumn("Factor", width="large"),
        "Ann. Alpha %": st.column_config.TextColumn("Ann. Alpha %", width="small"),
        "Beta": st.column_config.TextColumn("Beta", width="small"),
        "T-Statistic": st.column_config.TextColumn("T-Statistic", width="small"),
        "P-Value": st.column_config.TextColumn("P-Value", width="small"),
    }

    if factor_classifications is not None:
        color_map = {
            "best": "#e8f5e9",  # Light green
            "correlation_conflict": "#ffebee",  # Light red
            "n_limit": "#f5f5f5",  # Light gray
            "below_alpha": "#fff3e0",  # Light orange
        }

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
        st.dataframe(
            display,
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
        if st.button(type="primary", label="Copy to Clipboard", width="stretch", key=f"{key}_copy"):
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
    datasets = dataset_service(fl_id).load_all_versions()

    data = []
    for a in analyses:
        dataset = datasets.get(a.dataset_version)

        data.append(
            {
                "": f"/results?fl_id={a.fl_id}&id={a.id}",
                "Analysis Date": format_date(a.created_at, "%b %d, %Y %H:%M"),
                "Run Time": format_runtime(a.started_at, a.finished_at),
                "Universe": dataset.universeName if dataset else "N/A",
                "Factors": dataset.factorCount if dataset else "N/A",
                "Avg Abs Alpha": (
                    f"{a.avg_abs_alpha:.2f}%" if a.avg_abs_alpha is not None else "N/A"
                ),
                "Period": (
                    f"{format_date(dataset.startDt, '%Y/%m/%d')} - {format_date(dataset.endDt, '%Y/%m/%d')}"
                    if dataset
                    else "N/A"
                ),
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
        v: "#f5f5f5" if i % 2 == 0 else "#ffffff"
        for i, v in enumerate(unique_versions)
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
