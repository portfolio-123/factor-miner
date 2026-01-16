from typing import Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from src.core.context import get_state, update_state
from src.workers.manager import update_dataset_info
from src.core.types import DatasetConfig, ScopeType
from src.services.readers import ParquetDataReader
from src.services.parquet_utils import get_dataset_file_path, get_file_version
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


def render_dataset_info_row(
    config: DatasetConfig,
    ds_ver: str | None = None,
) -> None:
    from src.ui.dialogs import show_formulas_modal

    c1, c2, c3, c4 = st.columns([1.5, 2, 1, 1], vertical_alignment="top")

    big_items = [
        (c1, "Universe", config.universeName),
        (c2, "Period", f"{config.startDt or 'N/A'} - {config.endDt or 'N/A'}"),
        (c3, "Frequency", frequency_map.get(config.frequency, "N/A")),
    ]
    for col, label, value in big_items:
        with col:
            st.html(render_big_info_item(label, value))

    with c4:
        count = config.factorCount
        st.html(
            '<div class="dataset-info-item big view-factors-trigger"><div class="label">FACTORS</div></div>'
        )
        st.button(
            f"View ({count})",
            key=f"view_factors_{ds_ver}",
            on_click=lambda ds_ver=ds_ver: update_state(formulas_ds_ver=ds_ver),
        )

        state = get_state()
        if state.formulas_ds_ver == ds_ver:
            show_formulas_modal(pd.DataFrame(config.formulas))
            update_state(formulas_ds_ver=None)

    spacer(21)

    col_left, col_right = st.columns([0.9, 1], vertical_alignment="top")

    with col_left:
        render_section_label("Other Settings")
        items = [
            ("Currency", config.currency),
            ("Benchmark", config.benchmark),
            ("Precision", config.precision),
            ("Pit Method", config.pitMethod),
        ]
        st.html(
            f'<div class="dataset-info-group">{"".join(render_info_item(l, v) for l, v in items)}</div>'
        )

    with col_right:
        if config.normalization:
            render_section_label("Normalization")
            st.html(
                f'<div class="dataset-info-group">{"".join(_build_norm_items(config.normalization))}</div>'
            )


def render_current_dataset() -> None:
    from src.ui.components.analyses import render_analysis_params

    state = get_state()

    path: Optional[str] = None
    ds_ver: Optional[str] = None

    # if this is a results or new analysis page, try to load from analysis backup
    if state.current_analysis_id:
        try:
            parts = state.current_analysis_id.split("/")
            if len(parts) >= 2:
                ds_ver = parts[1]
                backup_path = get_dataset_file_path(state.factor_list_uid, ds_ver)
                if backup_path.exists():
                    path = str(backup_path)
        except Exception:
            pass

    # fallback to live dataset
    if not path and state.dataset_path:
        try:
            path = state.dataset_path
            ds_ver = get_file_version(path)
        except Exception:
            pass

    dataset_info = ParquetDataReader(path).get_dataset_info()
    config = dataset_info or DatasetConfig()

    with st.container(border=True):
        render_dataset_info_row(config, ds_ver)
        spacer(8)

    if state.page == "results":
        analysis_params = {
            "min_alpha": state.min_alpha,
            "top_x_pct": state.top_x_pct,
            "bottom_x_pct": state.bottom_x_pct,
        }
        render_analysis_params(analysis_params)


@st.fragment
def render_description_editor(description: str, ds_ver: str, edit_mode: bool) -> None:
    with st.container(border=True):
        if edit_mode:
            with st.form("edit_description_form", border=False):
                st.html(
                    "<style>.st-key-hidden_save_btn { display: none; }</style>"
                )
                save_enter = st.form_submit_button(
                    "Save", type="primary", key="hidden_save_btn"
                )
                col1, col2, col3 = st.columns(
                    [16, 2, 2], vertical_alignment="center", gap="small"
                )
                with col1:
                    new_desc = st.text_input(
                        "Description",
                        value=description,
                        placeholder="Enter dataset description",
                        key="edit_description_input",
                        label_visibility="collapsed",
                    )
                # unreliable, could research a better way
                components.html(
                    """
                    <script>
                        var input = window.parent.document.querySelector(
                            'input[placeholder="Enter dataset description"]'
                        );
                        if (input) input.focus();
                    </script>
                    """,
                    height=0,
                )
                with col2:
                    cancel = st.form_submit_button(
                        "Cancel",
                    )
                with col3:
                    save = st.form_submit_button(
                        "Save",
                        type="primary",
                    )
                if save or save_enter:
                    if update_dataset_info(ds_ver, {"description": new_desc}):
                        update_state(edit_dataset_mode=False)
                        st.rerun()
                    else:
                        st.error("Failed to update description.")
                if cancel:
                    update_state(edit_dataset_mode=False)
                    st.rerun()
        else:
            col1, col2 = st.columns(
                [16, 2], vertical_alignment="center", gap="small"
            )
            with col1:
                st.markdown(description or "*No description provided*")
            with col2:
                st.button(
                    "Edit",
                    key="edit_desc_btn",
                    use_container_width=True,
                    on_click=update_state,
                    kwargs={"edit_dataset_mode": True},
                )
