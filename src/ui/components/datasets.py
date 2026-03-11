from pathlib import Path

import streamlit as st
from src.ui.components.tables import show_factors_modal
from src.core.config.constants import (
    FREQUENCY_LABELS,
    PIT_METHOD_LABELS,
    SCALING_LABELS,
)
from src.core.config.environment import FACTOR_LIST_DIR, INTERNAL_MODE
from src.internal.links import p123_link
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
    user_uid = st.session_state.get("user_uid") if INTERNAL_MODE else None

    # Build the dataset path based on mode
    if not INTERNAL_MODE:
        dataset_path = Path(FACTOR_LIST_DIR) / f"{fl_id}.parquet"
    else:
        dataset_path = Path(FACTOR_LIST_DIR) / user_uid / fl_id

    if not dataset_path.is_file():
        if link := p123_link(fl_id, "generate"):
            st.warning(f"No dataset found for this Factor List. [Generate]({link})")
        else:
            st.warning("No dataset found. Please select a valid .parquet file.")
        return None

    try:
        with DatasetService(fl_id, user_uid) as svc:
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
    ]

    if normalization.scaling in (ScalingMethod.NORMAL, ScalingMethod.MINMAX):
        trim_value = f"{normalization.trimPct}%" if normalization.trimPct is not None else "N/A"
        items.append(("Trim", trim_value))

        if normalization.scaling == ScalingMethod.MINMAX:
            outlier_value = normalization.outliers.title() if normalization.outliers else "N/A"
            items.append(("Outliers", outlier_value))
        else:
            outlier_value = str(normalization.outlierLimit) if normalization.outlierLimit is not None else "N/A"
            items.append(("Outlier Limit", outlier_value))

    items.append(("N/A Handling", "Middle" if normalization.naFill else "None"))

    if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
        items.append(("ML Training End", format_date(normalization.mlTrainingEnd, "%Y-%m-%d")))

    return [render_info_item(label, value) for label, value in items]


def _get_date_display(dataset_metadata: DatasetConfig) -> tuple[str, str]:
    fmt = "%Y-%m-%d"
    if dataset_metadata.type == DatasetType.DATE:
        value = format_date(dataset_metadata.asOfDt, fmt) if dataset_metadata.asOfDt else "N/A"
        return "Date", value
    start = format_date(dataset_metadata.startDt, fmt) if dataset_metadata.startDt else "N/A"
    end = format_date(dataset_metadata.endDt, fmt) if dataset_metadata.endDt else "N/A"
    return "Period", f"{start} — {end}"


def render_dataset_card(dataset_metadata: DatasetConfig) -> None:
    fl_id = st.query_params.get("fl_id")
    user_uid = st.session_state.get("user_uid") if INTERNAL_MODE else None

    with st.container(border=True):
        is_active = dataset_metadata.active
        if is_active:
            header_left, _, header_formulas, header_preview, header_status = st.columns(
                [3, 0.4, 0.9, 0.65, 0.15], vertical_alignment="center"
            )
        else:
            header_left, _, header_formulas, header_status = st.columns(
                [3, 0.9, 0.9, 0.15], vertical_alignment="center"
            )
        with header_left:
            fl_name = st.session_state.get("fl_name", fl_id)
            if fl_link := p123_link(fl_id):  # internal
                subtitle = f'Generated using <a href="{fl_link}" target="_blank" style="color: #666;">{fl_name}</a>'
            else:  # external
                subtitle = fl_name
            st.html(
                f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">Dataset <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">{subtitle}</span></p>'
            )

        formula_count = len(dataset_metadata.formulas) if dataset_metadata.formulas else 0
        with header_formulas:
            if st.button(
                f"Formulas ({formula_count})",
                width="stretch",
                key=f"formulas_{dataset_metadata.version}",
                type="secondary",
            ):
                show_factors_modal(dataset_metadata.formulas_df, title="Dataset Formulas")

        if is_active:
            with header_preview:
                if st.button(
                    "Preview",
                    width="stretch",
                    key=f"preview_dataset_{dataset_metadata.version}",
                    type="secondary",
                ):
                    with DatasetService(fl_id, user_uid) as svc:
                        preview_df, stats = svc.get_review_data()
                    show_factors_modal(
                        dataset_metadata.formulas_df, stats, preview_df, title="Dataset Preview"
                    )

        with header_status:
            status_color = "#22c55e" if is_active else "#ef4444"
            status_title = "Active version" if is_active else "Not active version"
            st.html(
                f'<div style="display: flex; justify-content: center;" title="{status_title}">'
                f'<span style="width: 12px; height: 12px; border-radius: 50%; background-color: {status_color};"></span>'
                f"</div>"
            )

        c1, c2, c3, c4 = st.columns([1, 0.5, 1.25, 1.5], vertical_alignment="top")
        date_label, date_value = _get_date_display(dataset_metadata)

        big_items = [
            (c1, date_label, date_value),
            (c2, "Frequency", FREQUENCY_LABELS.get(dataset_metadata.frequency, "N/A")),
            (c3, "Generated", format_timestamp(dataset_metadata.version)),
            (c4, "Universe", dataset_metadata.universeName),
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
