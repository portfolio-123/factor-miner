import os
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime

from src.core.context import get_state, update_state, add_debug_log
from src.ui.components import section_header, render_results_table, render_info_item
from src.core.calculations import select_best_features
from src.workers.manager import delete_job, read_job, update_job_name


def render_analysis_name_input() -> None:
    state = get_state()
    job_id = state.current_job_id

    if not job_id:
        return

    job_data = read_job(job_id)
    if not job_data:
        return

    saved_name = job_data.get("name", "")
    created_at = datetime.fromisoformat(job_data["created_at"])
    default_name = created_at.strftime("%b %d, %Y %H:%M:%S")

    st.markdown(
        "<div style='font-size: 14px; font-weight: 500; color: #6b7280; margin-bottom: 4px;'>Analysis Name</div>",
        unsafe_allow_html=True
    )

    col_input, col_btn = st.columns([5, 1])

    with col_input:
        new_name = st.text_input(
            "Analysis Name",
            value=saved_name,
            placeholder=default_name,
            key="analysis_name_input",
            label_visibility="collapsed",
        )

    with col_btn:
        if st.button("Save", type="primary", use_container_width=True):
            update_job_name(job_id, new_name)
            st.rerun()


@st.fragment
def _render_filter_and_results() -> None:
    state = get_state()

    section_header("Filter Parameters")

    # initialize widget keys with state values if not set
    if "filter_correlation" not in st.session_state:
        st.session_state.filter_correlation = state.correlation_threshold
    if "filter_n_features" not in st.session_state:
        st.session_state.filter_n_features = state.n_features

    col1, col2, _ = st.columns([1, 1, 2])

    with col1:
        correlation_threshold = st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            key="filter_correlation",
            step=0.05,
        )

    with col2:
        n_features = st.number_input(
            "Number of Features",
            min_value=1,
            max_value=100,
            key="filter_n_features",
            step=1,
        )

    if (correlation_threshold != state.correlation_threshold or
            n_features != state.n_features):
        update_state(
            correlation_threshold=correlation_threshold,
            n_features=n_features
        )
        add_debug_log(f"Parameters updated - Correlation: {correlation_threshold}, N: {n_features}")

    # calculate best features again with new filter parameters
    best_features = select_best_features(
        state.all_metrics,
        state.all_corr_matrix,
        N=n_features,
        correlation_threshold=correlation_threshold,
        a_min=state.min_alpha
    )

    section_header("Best Performing Factors")

    display_df = render_results_table(best_features, state.all_metrics, state.formulas_data)

    _render_action_buttons(state, display_df)


def _prepare_download_csv(display_df, formulas_data):
    download_df = display_df.copy()

    if formulas_data is not None and 'name' in formulas_data.columns:
        formulas_lookup = formulas_data[['name', 'formula']].drop_duplicates(subset=['name'])
        download_df = download_df.merge(
            formulas_lookup,
            left_on='Factor',
            right_on='name',
            how='left'
        ).drop(columns=['name'])
        download_df = download_df.rename(columns={'formula': 'Formula'})
        # Reorder columns: Factor, Formula, then the rest
        cols = download_df.columns.tolist()
        cols.remove('Formula')
        cols.insert(1, 'Formula')
        download_df = download_df[cols]

    return download_df.to_csv(index=False)


def _render_action_buttons(state, display_df) -> None:
    if display_df is None or display_df.empty:
        return

    col1, _, col3, col4 = st.columns([1, 2, 1, 1])

    # tab delimited for copy to clipboard (without Formula)
    csv_to_copy = display_df.to_csv(index=False, sep='\t')

    # comma delimited for file download (with Formula in second position)
    csv_to_download = _prepare_download_csv(display_df, state.formulas_data)


    # TODO: horrible way to add a copy button. search for a streamlit library or more native solution
    csv_escaped = csv_to_copy.replace('\\', '\\\\').replace("'", "\\'").replace('\n', '\\n').replace('\r', '')

    with col3:
        st.button("Copy to Clipboard", key="copy_clipboard_btn", type="secondary", use_container_width=True)

    components.html(f"""
        <script>
            const csvData = '{csv_escaped}';

            function copyToClipboard(text) {{
                const textarea = window.parent.document.createElement('textarea');
                textarea.value = text;
                textarea.style.position = 'fixed';
                textarea.style.opacity = '0';
                window.parent.document.body.appendChild(textarea);
                textarea.focus();
                textarea.select();
                window.parent.document.execCommand('copy');
                window.parent.document.body.removeChild(textarea);
            }}

            function attachCopyHandler() {{
                const buttons = window.parent.document.querySelectorAll('button');
                for (const btn of buttons) {{
                    const text = btn.textContent.trim();
                    if (text === 'Copy to Clipboard') {{
                        if (btn._copyHandlerAttached) return;
                        btn._copyHandlerAttached = true;

                        btn.addEventListener('click', function(e) {{
                            e.preventDefault();
                            e.stopPropagation();

                            copyToClipboard(csvData);

                            // Find the text element inside the button and change it
                            const textEl = btn.querySelector('p') || btn;
                            textEl.textContent = 'Copied!';
                            setTimeout(() => {{
                                textEl.textContent = 'Copy to Clipboard';
                            }}, 1000);
                        }});
                        break;
                    }}
                }}
            }}

            attachCopyHandler();
            const observer = new MutationObserver(attachCopyHandler);
            observer.observe(window.parent.document.body, {{ childList: true, subtree: true }});
        </script>
    """, height=0)

    with col4:
        st.download_button(
            type="primary",
            label="Download CSV",
            data=csv_to_download,
            file_name=f"{state.factor_list_uid}_best_features.csv",
            mime="text/csv",
            use_container_width=True
        )


def render() -> None:
    _render_filter_and_results()
