import streamlit as st
import pandas as pd



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
