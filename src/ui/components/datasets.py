from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from src.ui.components.tables import show_formulas_modal
from src.core.environment import P123_BASE_URL, FACTOR_LIST_DIR
from src.core.types import DatasetConfig, ScopeType
from src.services.dataset_service import get_active_dataset_metadata
from src.ui.constants import SCALING_LABELS, frequency_map
from src.ui.components.common import (
    render_info_item,
    render_big_info_item,
    get_section_label_html,
    spacer,
)
from src.core.utils import format_date


def load_active_dataset() -> DatasetConfig | None:
    fl_id = st.query_params.get("fl_id")
    dataset_path = Path(FACTOR_LIST_DIR) / fl_id

    if not dataset_path.is_file():
        download_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/download"
        st.warning(f"No dataset found for this Factor List. [Generate]({download_url})")
        return None

    try:
        return get_active_dataset_metadata(fl_id)
    except Exception:
        st.error("Failed to load dataset")
        return None


def _parse_created_timestamp(version: str | None) -> str:
    if not version:
        return "N/A"
    try:
        timestamp = int(version)
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%b %d, %Y at %I:%M %p")
    except ValueError:
        return "N/A"


def _build_norm_items(normalization) -> list[str]:
    items = [
        (
            "Scaling",
            (
                SCALING_LABELS.get(normalization.scaling, str(normalization.scaling))
                if normalization.scaling
                else "None"
            ),
        ),
        ("Scope", normalization.scope.title() if normalization.scope else "None"),
        (
            "Trim",
            f"{normalization.trimPct}%" if normalization.trimPct is not None else "N/A",
        ),
        (
            "Outlier",
            (
                str(normalization.outlierLimit)
                if normalization.outlierLimit is not None
                else "None"
            ),
        ),
        ("N/A Handling", "Middle" if normalization.naFill else "None"),
    ]

    if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
        items.append(("ML Training End", normalization.mlTrainingEnd))

    return [render_info_item(label, value) for label, value in items]


def render_dataset_statistics(stats: dict) -> None:
    cols = st.columns([1, 1, 1, 2], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"

    stat_items = [
        ("Rows", stats["num_rows"]),
        ("Dates", stats["num_dates"]),
        ("Columns", stats["num_columns"]),
        ("Period", f"{stats['min_date']} - {stats['max_date']}"),
    ]

    for col, (label, value) in zip(cols, stat_items):
        with col:
            st.badge(label)
            st.html(f"<p style='{stat_style}'>{value}</p>")


def render_dataset_card(dataset_metadata: DatasetConfig) -> None:

    with st.container(border=True):
        # Header row with title and creation date
        header_left, header_right = st.columns([1, 1], vertical_alignment="center")
        with header_left:
            st.html('<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">Dataset Parameters</p>')
        with header_right:
            created_on = _parse_created_timestamp(dataset_metadata.version)
            st.html(
                f'<p style="text-align: right; color: #666; margin: 0;">Created on: {created_on}</p>'
            )

        c1, c2, c3, c4 = st.columns([0.9, 0.9, 1.3, 0.7], vertical_alignment="top")

        big_items = [
            (c1, "Universe", dataset_metadata.universeName),
            (c2, "Frequency", frequency_map.get(dataset_metadata.frequency, "N/A")),
            (
                c3,
                "Period",
                f"{format_date(dataset_metadata.startDt, '%Y/%m/%d') if dataset_metadata.startDt else 'N/A'} - {format_date(dataset_metadata.endDt, '%Y/%m/%d') if dataset_metadata.endDt else 'N/A'}",
            ),
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

        spacer(6)

        col_left, col_right = st.columns([0.9, 1], vertical_alignment="top")

        with col_left:
            items = [
                ("Currency", dataset_metadata.currency),
                ("Benchmark", dataset_metadata.benchmark),
                ("Precision", dataset_metadata.precision),
                ("Pit Method", dataset_metadata.pitMethod),
            ]
            st.html(
                f'{get_section_label_html("Other Settings")}<div class="dataset-info-group">{"".join(render_info_item(l, v) for l, v in items)}</div>'
            )

        with col_right:
            if dataset_metadata.normalization:
                st.html(
                    f'{get_section_label_html("Normalization")}<div class="dataset-info-group">{"".join(_build_norm_items(dataset_metadata.normalization))}</div>'
                )

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
