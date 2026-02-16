from pathlib import Path

import pandas as pd
import streamlit as st
from src.ui.components.tables import show_factors_modal
from src.core.config.constants import (
    FREQUENCY_LABELS,
    PIT_METHOD_LABELS,
    SCALING_LABELS,
)
from src.core.config.environment import P123_BASE_URL, FACTOR_LIST_DIR
from src.core.types.models import DatasetConfig, DatasetType, ScalingMethod, ScopeType
from src.services.dataset_service import DatasetService
from src.ui.components.common import (
    render_info_item,
    render_big_info_item,
    get_section_label_html,
    spacer,
)
from src.core.utils.common import format_date, format_timestamp


def load_active_dataset() -> DatasetConfig | None:
    fl_id = st.query_params.get("fl_id")
    dataset_path = Path(FACTOR_LIST_DIR) / fl_id

    if not dataset_path.is_file():
        download_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/generate"
        st.warning(f"No dataset found for this Factor List. [Generate]({download_url})")
        return None

    try:
        with DatasetService(fl_id) as svc:
            return svc.get_metadata()
    except Exception as e:
        st.error(f"Failed to load dataset: {e}")
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

        if normalization.scaling == ScalingMethod.MINMAX:
            outlier_label = "Outliers"
            outlier_value = (
                normalization.outliers.title() if normalization.outliers else "None"
            )
        else:
            outlier_label = "Outlier Limit"
            outlier_value = (
                str(normalization.outlierLimit)
                if normalization.outlierLimit is not None
                else "None"
            )

        items.insert(3, (outlier_label, outlier_value))

    if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
        items.append(
            ("ML Training End", format_date(normalization.mlTrainingEnd, "%Y/%m/%d"))
        )

    return [render_info_item(label, value) for label, value in items]


def render_dataset_card(dataset_metadata: DatasetConfig) -> None:
    with st.container(border=True):
        is_active = dataset_metadata.active
        if is_active:
            header_left, _, header_formulas, header_preview, header_status = st.columns(
                [3, 0.5, 0.6, 0.5, 0.15], vertical_alignment="center"
            )
        else:
            header_left, _, header_formulas, header_status = st.columns(
                [3, 1, 0.6, 0.15], vertical_alignment="center"
            )
        created_on = format_timestamp(dataset_metadata.version)
        fl_id = st.query_params.get("fl_id")
        fl_link = f"{P123_BASE_URL}/sv/factorList/{fl_id}"
        with header_left:
            st.html(
                f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">Dataset <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">Generated using <a href="{fl_link}" target="_blank" style="color: #666;">{st.session_state.get("fl_name")}</a></span></p>'
            )
        formula_count = (
            len(dataset_metadata.formulas) if dataset_metadata.formulas else 0
        )
        with header_formulas:
            if st.button(
                f"Formulas ({formula_count})",
                width="stretch",
                key=f"formulas_{dataset_metadata.version}",
                type="secondary",
            ):
                show_factors_modal(
                    dataset_metadata.formulas_df, title="Dataset Formulas"
                )
        if is_active:
            with header_preview:
                if st.button(
                    "Preview",
                    width="stretch",
                    key=f"preview_dataset_{dataset_metadata.version}",
                    type="secondary",
                ):
                    fl_id = st.query_params.get("fl_id")
                    with DatasetService(fl_id) as svc:
                        preview_df, stats = svc.get_review_data()
                    show_factors_modal(
                        dataset_metadata.formulas_df,
                        stats,
                        preview_df,
                        title="Dataset Preview",
                    )
        with header_status:
            status_color = "#22c55e" if is_active else "#ef4444"
            status_title = "Active version" if is_active else "Not active version"
            st.html(
                f'<div style="display: flex; justify-content: center;" title="{status_title}">'
                f'<span style="width: 12px; height: 12px; border-radius: 50%; background-color: {status_color};"></span>'
                f"</div>"
            )

        c1, c2, c3, c4 = st.columns([1, 0.5, 1.5, 1], vertical_alignment="top")

        if dataset_metadata.type == DatasetType.DATE:
            date_label = "Date"
            date_value = (
                format_date(dataset_metadata.asOfDt, "%Y/%m/%d")
                if dataset_metadata.asOfDt
                else "N/A"
            )
        else:
            date_label = "Period"
            date_value = f"{format_date(dataset_metadata.startDt, '%Y/%m/%d') if dataset_metadata.startDt else 'N/A'} - {format_date(dataset_metadata.endDt, '%Y/%m/%d') if dataset_metadata.endDt else 'N/A'}"

        big_items = [
            (c1, date_label, date_value),
            (c2, "Frequency", FREQUENCY_LABELS.get(dataset_metadata.frequency, "N/A")),
            (c3, "Universe", dataset_metadata.universeName),
            (c4, "Generated", created_on),
        ]
        for col, label, value in big_items:
            with col:
                st.html(render_big_info_item(label, value))

        spacer(6)

        has_normalization = dataset_metadata.normalization is not None

        if has_normalization:
            col_left, col_right = st.columns([0.9, 1], vertical_alignment="top")
        else:
            col_left = st.container()

        with col_left:
            items = [
                ("Currency", dataset_metadata.currency),
                (
                    "Pit Method",
                    PIT_METHOD_LABELS.get(
                        dataset_metadata.pitMethod, dataset_metadata.pitMethod
                    ),
                ),
                ("Benchmark", dataset_metadata.benchmark),
            ]
            if not has_normalization:
                items.append(("Normalization", "Raw"))
            st.html(
                f'{get_section_label_html("Other Settings")}<div style="display: flex; gap: 24px;">{"".join(render_info_item(l, v) for l, v in items)}</div>'
            )

        if has_normalization:
            with col_right:
                norm_content = "".join(
                    _build_norm_items(dataset_metadata.normalization)
                )
                st.html(
                    f'{get_section_label_html("Normalization")}<div style="display: flex; gap: 24px;">{norm_content}</div>'
                )
