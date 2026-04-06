import streamlit as st
from src.internal.links import p123_link
from src.ui.components.tables import show_formulas_modal, show_preview_modal
from src.core.config.constants import (
    FREQUENCY_LABELS,
    PIT_METHOD_LABELS,
    PRICE_COLUMN,
    SCALING_LABELS,
)
from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import (
    DatasetConfig,
    NormalizationConfig,
    ScalingMethod,
    ScopeType,
)
from src.services.dataset_service import DatasetService
from src.ui.components.common import (
    render_info_item,
    render_big_info_item,
    render_section_label_html,
    spacer,
)
from src.core.utils.common import (
    escape_html,
    escape_html_attr,
    format_date,
    format_timestamp,
)

DATE_FORMAT = "%Y-%m-%d"


def _build_norm_items(normalization: NormalizationConfig) -> list[str]:
    items = [
        ("Scaling", SCALING_LABELS[normalization.scaling]),
        ("Scope", normalization.scope.title()),
    ]

    needs_trim = normalization.scaling in (ScalingMethod.NORMAL, ScalingMethod.MINMAX)
    if needs_trim:

        items.append(("Trim", f"{normalization.trimPct}%"))

        if normalization.scaling == ScalingMethod.MINMAX:
            assert normalization.outliers
            items.append(("Outliers", normalization.outliers.title()))
        else:
            items.append(("Outlier Limit", str(normalization.outlierLimit)))

    items.append(("N/A Handling", "Middle"))

    if normalization.scope == ScopeType.DATASET and normalization.mlTrainingEnd:
        items.append(
            ("ML Training End", format_date(normalization.mlTrainingEnd, DATE_FORMAT))
        )

    return [render_info_item(label, value) for label, value in items]


def _get_date_range(metadata: DatasetConfig) -> str:
    start = format_date(metadata.startDt, DATE_FORMAT) or "N/A"
    end = format_date(metadata.endDt, DATE_FORMAT) or "N/A"
    return f"{start} – {end}"


def _render_header(metadata: DatasetConfig, fl_id: str) -> None:
    is_active = metadata.active
    formula_count = sum(1 for f in metadata.formulas if f.get("name") != PRICE_COLUMN)

    if is_active:
        col_title, _, col_formulas, col_preview, col_status = st.columns(
            [3, 0.4, 0.9, 0.65, 0.15], vertical_alignment="center"
        )
    else:
        col_title, _, col_formulas, col_status = st.columns(
            [3, 0.9, 0.9, 0.15], vertical_alignment="center"
        )
        col_preview = None

    fl_name = st.session_state.get("fl_name", fl_id)
    subtitle = (
        f'Generated using <a href="{escape_html_attr(p123_link(fl_id))}" target="_blank" style="color: #666;">{escape_html(fl_name)}</a>'
        if INTERNAL_MODE
        else escape_html(fl_name)
    )
    with col_title:
        st.html(
            f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">'
            f'Dataset <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">{subtitle}</span>'
            f"</p>"
        )

    with col_formulas:
        if st.button(
            f"Formulas ({formula_count})",
            width="stretch",
            key=f"formulas_{metadata.version}",
            type="secondary",
        ):
            show_formulas_modal(metadata.formulas_df)

    if col_preview is not None:
        with col_preview:
            if st.button(
                "Preview",
                width="stretch",
                key=f"preview_dataset_{metadata.version}",
                type="secondary",
            ):
                with DatasetService(st.session_state["dataset_details"]) as svc:
                    preview = svc.get_preview_data()
                show_preview_modal(**preview, formula_count=formula_count)

    if is_active:
        status_color = "#22c55e"
        status_title = "Active version"
    else:
        status_color = "#ef4444"
        status_title = "Prior version"
    with col_status:
        st.html(
            f'<div style="display: flex; justify-content: center;" title="{status_title}">'
            f'<span style="width: 12px; height: 12px; border-radius: 50%; background-color: {status_color};"></span>'
            f"</div>"
        )


def _render_summary_row(metadata: DatasetConfig) -> None:
    c1, c2, c3, c4 = st.columns([1, 0.5, 1.25, 1.5], vertical_alignment="top")

    big_items = [
        (c1, "Period", _get_date_range(metadata)),
        (c2, "Frequency", FREQUENCY_LABELS.get(metadata.frequency, "N/A")),
        (c3, "Generated", format_timestamp(metadata.version)),
        (c4, "Universe", metadata.universeName),
    ]
    for col, label, value in big_items:
        with col:
            st.html(render_big_info_item(label, value))


def _render_settings(metadata: DatasetConfig) -> None:
    has_norm = bool(metadata.normalization and metadata.preprocessor is not None)

    settings_items: list[tuple[str, str | int]] = [
        ("Currency", metadata.currency),
        ("Pit Method", PIT_METHOD_LABELS.get(metadata.pitMethod, metadata.pitMethod)),
        ("Benchmark", metadata.benchName),
    ]
    if not has_norm:
        settings_items.append(("Normalization", "Raw"))

    settings_html = (
        f'{render_section_label_html("Other Settings")}'
        f'<div style="display: flex; gap: 24px;">'
        f'{"".join(render_info_item(l, v) for l, v in settings_items)}'
        f"</div>"
    )

    if has_norm and metadata.preprocessor:
        col_left, col_right = st.columns([0.9, 1], vertical_alignment="top")
        norm_html = (
            f'{render_section_label_html("Normalization")}'
            f'<div style="display: flex; gap: 24px;">'
            f'{"".join(_build_norm_items(metadata.preprocessor))}'
            f"</div>"
        )
    else:
        col_left = st.container()
        col_right = None
        norm_html = None

    with col_left:
        st.html(settings_html)

    if col_right is not None:
        with col_right:
            st.html(norm_html)


def render_dataset_card(dataset_metadata: DatasetConfig) -> None:
    fl_id = st.query_params.get("fl_id")
    assert fl_id

    with st.container(border=True):
        _render_header(dataset_metadata, fl_id)
        _render_summary_row(dataset_metadata)
        spacer(6)
        _render_settings(dataset_metadata)
