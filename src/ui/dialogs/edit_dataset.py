import streamlit as st
from src.core.context import update_state
from src.workers.manager import update_dataset_info


def show_edit_dialog(selected_ver: str, ds_info_map: dict, dataset_path: str) -> None:
    @st.dialog("Edit Dataset Details", width="large")
    def _dialog():
        update_state(edit_dataset_mode=False)

        curr_info = ds_info_map.get(selected_ver)

        with st.form(key="edit_dataset_form", border=False):
            new_name = st.text_input(
                "Name", value=curr_info.name, placeholder="Enter dataset name"
            )
            new_desc = st.text_area(
                "Description",
                value=curr_info.description,
                height=120,
                placeholder="Enter dataset description",
            )

            col1, col2 = st.columns(2)
            if col1.form_submit_button("Cancel", width="stretch"):
                st.rerun()
            if col2.form_submit_button("Save Changes", type="primary", width="stretch"):
                if update_dataset_info(
                    dataset_path,
                    selected_ver,
                    {"name": new_name, "description": new_desc},
                ):
                    st.rerun()
                else:
                    st.error("Failed to update.")

    _dialog()
