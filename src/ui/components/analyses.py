import streamlit as st

from src.core.utils import format_date
from src.core.types import Analysis, AnalysisParams
from src.ui.components.common import section_header, render_info_item
from src.workers.manager import update_analysis


def render_analysis_params(params: AnalysisParams) -> None:
    param_config = [
        ("Min Alpha", params.min_alpha),
        ("Top X", params.top_pct),
        ("Bottom X", params.bottom_pct),
    ]
    items = [render_info_item(label, f"{value}%") for label, value in param_config]
    section_header("Analysis Parameters")
    st.html(f'<div class="dataset-info-group">{"".join(items)}</div>')


def render_analysis_notes(analysis: Analysis) -> None:
    section_header("Notes")

    notes_value = st.text_area(
        "Notes",
        value=analysis.notes or "",
        placeholder="Add notes about this analysis...",
        label_visibility="collapsed",
        key=f"notes_{analysis.id}",
    )

    if st.button("Save Notes", key=f"save_notes_{analysis.id}"):
        update_analysis(analysis, notes=notes_value)
        st.success("Notes saved")
        st.rerun()


def _render_card_field(label: str, value: str) -> None:
    st.markdown(
        f'<div style="display:flex;flex-direction:column;justify-content:center;min-height:38px;line-height:1.3;">'
        f'<span style="font-size:11px;color:#6c757d;text-transform:uppercase;letter-spacing:0.5px;">{label}</span>'
        f'<span style="font-size:14px;font-weight:500;">{value}</span>'
        f"</div>",
        unsafe_allow_html=True,
    )


def render_analysis_card(analysis: Analysis) -> None:
    formatted_date = format_date(analysis.created_at, "%b %d, %Y %H:%M:%S")

    with st.container(border=True):
        cols = st.columns([2, 2, 2, 3, 1.5, 1], vertical_alignment="top")

        with cols[0]:
            _render_card_field("Min Alpha", str(analysis.params.min_alpha))

        with cols[1]:
            _render_card_field("Top X", f"{analysis.params.top_pct}%")

        with cols[2]:
            _render_card_field("Bottom X", f"{analysis.params.bottom_pct}%")

        with cols[3]:
            _render_card_field("Date", formatted_date)

        with cols[4]:
            st.html('<span class="analysis-badge-marker"></span>')
            st.badge(analysis.status.display, color=analysis.status.color)

        with cols[5]:
            if st.button(
                "→",
                key=f"analysis_btn_{analysis.id}",
                help="Open analysis results",
                use_container_width=True,
            ):
                st.switch_page(
                    st.session_state["pages"]["results"],
                    query_params={"fl_id": analysis.fl_id, "id": analysis.id},
                )
