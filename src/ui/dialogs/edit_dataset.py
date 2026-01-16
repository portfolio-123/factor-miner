import streamlit as st
from src.core.context import update_state
from src.core.types import DatasetConfig
from src.workers.manager import update_dataset_info


def show_edit_dialog(dataset_info: DatasetConfig | None) -> None:
    @st.dialog("Edit Dataset Details", width="large")
    def _dialog():
        update_state(edit_dataset_mode=False)
        selected_ver = st.session_state.get("selected_dataset_ver")

        with st.form(key="edit_dataset_form", border=False):
            new_desc = st.text_area(
                "Description",
                value=dataset_info.description if dataset_info else "",
                height=120,
                placeholder="Enter dataset description",
            )

            col1, col2 = st.columns(2)
            if col1.form_submit_button("Cancel", width="stretch"):
                st.rerun()
            if col2.form_submit_button("Save Changes", type="primary", width="stretch"):
                if update_dataset_info(selected_ver, {"description": new_desc}):
                    st.rerun()
                else:
                    st.error("Failed to update.")

    _dialog()
