from pathlib import Path

import pandas as pd
import streamlit as st
from src.ui.components.tables import show_factors_modal
from src.core.constants import FREQUENCY_LABELS, SCALING_LABELS
from src.core.environment import P123_BASE_URL, FACTOR_LIST_DIR
from src.core.types import DatasetConfig, ScalingMethod, ScopeType
from src.services.dataset_service import dataset_service
from src.ui.components.common import (
    render_info_item,
    render_big_info_item,
    get_section_label_html,
    spacer,
)
from src.core.utils import format_date, format_timestamp


def load_active_dataset() -> DatasetConfig | None:
    fl_id = st.query_params.get("fl_id")
    dataset_path = Path(FACTOR_LIST_DIR) / fl_id

    if not dataset_path.is_file():
        download_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/download"
        st.warning(f"No dataset found for this Factor List. [Generate]({download_url})")
        return None

    try:
        return dataset_service(fl_id).get_metadata()
    except Exception:
        st.error("Failed to load dataset")
        return None


def _build_norm_items(normalization) -> list[str]:
    scaling_label = (
        SCALING_LABELS.get(normalization.scaling, str(normalization.scaling))
        if normalization.scaling
        else "None"
    )

    items = [
        ("Scaling", scaling_label),
        ("Scope", normalization.scope.title() if normalization.scope else "None"),
        ("N/A Handling", "Middle" if normalization.naFill else "None"),
    ]

    if normalization.scaling in (ScalingMethod.NORMAL, ScalingMethod.MINMAX):
        items.insert(
            2,
            (
                "Trim",
                (
                    f"{normalization.trimPct}%"
                    if normalization.trimPct is not None
                    else "N/A"
                ),
            ),
        )

        outlier_label = (
            "Outliers" if normalization.scaling == ScalingMethod.MINMAX else "Outlier"
        )
        items.insert(
            3,
            (
                outlier_label,
                (
                    str(normalization.outlierLimit)
                    if normalization.outlierLimit is not None
                    else "None"
                ),
            ),
        )

    if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
        items.append(
            ("ML Training End", format_date(normalization.mlTrainingEnd, "%Y/%m/%d"))
        )

    return [render_info_item(label, value) for label, value in items]


def render_dataset_card(dataset_metadata: DatasetConfig) -> None:
    with st.container(border=True):
        header_left, _, header_right, header_status = st.columns(
            [3, 1, 0.5, 0.15], vertical_alignment="center"
        )
        created_on = format_timestamp(dataset_metadata.version)
        with header_left:
            st.html(
                f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">Dataset Parameters <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">{created_on}</span></p>'
            )
        with header_right:
            if st.button(
                "Preview",
                width="stretch",
                key=f"preview_dataset_{dataset_metadata.version}",
                type="secondary",
            ):
                if dataset_metadata.active:
                    fl_id = st.query_params.get("fl_id")
                    preview_df, stats = dataset_service(fl_id).get_review_data()
                    show_factors_modal(
                        pd.DataFrame(dataset_metadata.formulas),
                        stats,
                        preview_df,
                    )
                else:
                    show_factors_modal(pd.DataFrame(dataset_metadata.formulas))
        with header_status:
            is_active = dataset_metadata.active
            status_color = "#22c55e" if is_active else "#ef4444"
            status_title = "Active version" if is_active else "Not active version"
            st.html(
                f'<div style="display: flex; justify-content: center;" title="{status_title}">'
                f'<span style="width: 12px; height: 12px; border-radius: 50%; background-color: {status_color};"></span>'
                f'</div>'
            )

        c1, c2, c3 = st.columns([1, 0.5, 2.5], vertical_alignment="top")

        big_items = [
            (c2, "Frequency", FREQUENCY_LABELS.get(dataset_metadata.frequency, "N/A")),
            (
                c1,
                "Period",
                f"{format_date(dataset_metadata.startDt, '%Y/%m/%d') if dataset_metadata.startDt else 'N/A'} - {format_date(dataset_metadata.endDt, '%Y/%m/%d') if dataset_metadata.endDt else 'N/A'}",
            ),
            (c3, "Universe", dataset_metadata.universeName)
        ]
        for col, label, value in big_items:
            with col:
                st.html(render_big_info_item(label, value))

        spacer(6)

        col_left, col_right = st.columns([0.9, 1], vertical_alignment="top")

        with col_left:
            items = [
                ("Currency", dataset_metadata.currency),
                ("Precision", dataset_metadata.precision),
                ("Pit Method", dataset_metadata.pitMethod),
                ("Benchmark", dataset_metadata.benchmark),
            ]
            st.html(
                f'{get_section_label_html("Other Settings")}<div style="display: flex; gap: 24px;">{"".join(render_info_item(l, v) for l, v in items)}</div>'
            )

        with col_right:
            if dataset_metadata.normalization:
                st.html(
                    f'{get_section_label_html("Normalization")}<div style="display: flex; gap: 24px;">{"".join(_build_norm_items(dataset_metadata.normalization))}</div>'
                )
            else:
                st.html(
                    f'{get_section_label_html("Normalization")}<div style="display: flex; gap: 24px;"><span style="font-size: 0.875rem; color: #666;">Raw</span></div>'
                )
