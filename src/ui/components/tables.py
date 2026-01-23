import streamlit as st
import pandas as pd
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured



def show_formulas_modal(formulas_df: pd.DataFrame) -> None:
    @st.dialog(f"Dataset Formulas ({len(formulas_df)})", width="large")
    def _render() -> None:
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

        csv_to_copy = formulas_df[["formula", "name", "tag"]].to_csv(index=False, sep="\t")
        csv_to_download = formulas_df[["formula", "name", "tag"]].to_csv(index=False)

        with col1:
            if st.button(type="primary", label="Copy to Clipboard", width="stretch"):
                copy_to_clipboard_unsecured(csv_to_copy)
                copy_to_clipboard(csv_to_copy)
                st.toast("Formulas copied to clipboard")

        with col2:
            st.download_button(
                type="primary",
                label="Download CSV",
                data=csv_to_download,
                file_name="dataset_formulas.csv",
                mime="text/csv",
                width="stretch",
            )

    _render()


def render_results_table(
    best_features: list,
    metrics_df: pd.DataFrame,
) -> pd.DataFrame | None:
    if not best_features:
        st.warning(
            "No features found matching the current criteria."
            "Try adjusting the correlation threshold or minimum alpha."
        )
        return None

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

    return display_df
