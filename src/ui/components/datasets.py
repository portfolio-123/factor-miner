import pandas as pd
import streamlit as st

from src.ui.components.tables import show_formulas_modal
from src.workers.manager import update_dataset_description
from src.core.types import DatasetConfig, ScopeType
from src.ui.constants import SCALING_LABELS, frequency_map
from src.ui.components.common import (
    section_header,
    render_info_item,
    render_big_info_item,
    render_section_label,
    spacer,
)


def _build_norm_items(normalization) -> list[str]:
    items = [
        ("Scaling", SCALING_LABELS.get(normalization.scaling, str(normalization.scaling)) if normalization.scaling else "None"),
        ("Scope", normalization.scope.title() if normalization.scope else "None"),
        ("Trim", f"{normalization.trimPct}%" if normalization.trimPct is not None else "N/A"),
        ("Outlier", str(normalization.outlierLimit) if normalization.outlierLimit is not None else "None"),
        ("N/A Handling", "Middle" if normalization.naFill else "None"),
    ]

    if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
        items.append(("ML Training End", normalization.mlTrainingEnd))

    return [render_info_item(label, value) for label, value in items]


def render_dataset_statistics(stats: dict, benchmark: str) -> None:
    section_header("Dataset Statistics")

    cols = st.columns([1, 1, 1, 2, 1], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"

    stat_items = [
        ("Rows", stats["num_rows"]),
        ("Dates", stats["num_dates"]),
        ("Columns", stats["num_columns"]),
        ("Period", f"{stats['min_date']} - {stats['max_date']}"),
        ("Benchmark", benchmark or "N/A"),
    ]

    for col, (label, value) in zip(cols, stat_items):
        with col:
            st.badge(label)
            st.html(f"<p style='{stat_style}'>{value}</p>")


def render_dataset_card(dataset_metadata: DatasetConfig) -> None:

    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1.5, 2, 1, 1], vertical_alignment="top")

        big_items = [
            (c1, "Universe", dataset_metadata.universeName),
            (c2, "Period", f"{dataset_metadata.startDt or 'N/A'} - {dataset_metadata.endDt or 'N/A'}"),
            (c3, "Frequency", frequency_map.get(dataset_metadata.frequency, "N/A")),
        ]
        for col, label, value in big_items:
            with col:
                st.html(render_big_info_item(label, value))

        with c4:
            count = dataset_metadata.factorCount
            st.html(
                '<div class="dataset-info-item big view-factors-trigger"><div class="label">FACTORS</div></div>'
            )
            if st.button(
                f"View ({count})",
                key=f"view_factors_{dataset_metadata.version}",
            ):
                show_formulas_modal(pd.DataFrame(dataset_metadata.formulas))

        spacer(21)

        col_left, col_right = st.columns([0.9, 1], vertical_alignment="top")

        with col_left:
            render_section_label("Other Settings")
            items = [
                ("Currency", dataset_metadata.currency),
                ("Benchmark", dataset_metadata.benchmark),
                ("Precision", dataset_metadata.precision),
                ("Pit Method", dataset_metadata.pitMethod),
            ]
            st.html(
                f'<div class="dataset-info-group">{"".join(render_info_item(l, v) for l, v in items)}</div>'
            )

        with col_right:
            if dataset_metadata.normalization:
                render_section_label("Normalization")
                st.html(
                    f'<div class="dataset-info-group">{"".join(_build_norm_items(dataset_metadata.normalization))}</div>'
                )

        spacer(8)


@st.fragment
def render_description_editor(description: str | None, version: str) -> None:
    with st.container(border=True):
        col1, col2 = st.columns([16, 2], vertical_alignment="center", gap="small")
        with col1:
            st.markdown(description or "*No description provided*")
        with col2:
            with st.popover("Edit", width="stretch"):
                new_desc = st.text_input(
                    "Description",
                    value=description,
                    placeholder="Enter dataset description",
                    label_visibility="collapsed",
                )
                if st.button("Save", type="primary", width="stretch"):
                    try:
                        update_dataset_description(version, description=new_desc)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to update description: {e}")


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
    display_df.rename(columns={"index": "Row"}, inplace=True)

    st.dataframe(
        display_df,
        height=500,
        width="stretch",
        hide_index=True,
        column_config={"Row": st.column_config.NumberColumn("Row", width=85)},
    )
