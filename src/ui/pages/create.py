import time
import json

import streamlit as st
from pydantic import ValidationError

from streamlit_local_storage import LocalStorage

from src.core.config.constants import (
    DEFAULT_MIN_ALPHA,
    DEFAULT_TOP_PCT,
    DEFAULT_BOTTOM_PCT,
    DEFAULT_CORRELATION_THRESHOLD,
    DEFAULT_N_FACTORS,
    DEFAULT_MAX_NA_PCT,
    DEFAULT_MIN_IC,
    SETTINGS_STORAGE_KEY,
)
from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import AnalysisParams, SettingsForm
from src.internal.links import p123_link
from src.services.dataset_service import DatasetService
from src.ui.components.common import section_header
from src.ui.components.datasets import render_dataset_card
from src.workers.analysis_service import AnalysisService




def _get_default_settings() -> dict:
    return {
        "rank_by": "Alpha",
        "top_pct": DEFAULT_TOP_PCT,
        "bottom_pct": DEFAULT_BOTTOM_PCT,
        "min_alpha": DEFAULT_MIN_ALPHA,
        "min_ic": DEFAULT_MIN_IC,
        "n_factors": DEFAULT_N_FACTORS,
        "max_na_pct": DEFAULT_MAX_NA_PCT,
        "correlation_threshold": DEFAULT_CORRELATION_THRESHOLD,
    }


def _apply_settings_if_triggered(saved: str | None) -> None:
    if not st.session_state.get("_load_settings_triggered"):
        return
    
    if not saved:
        del st.session_state["_load_settings_triggered"]
        return

    try:
        data = json.loads(saved)
        defaults = _get_default_settings()
        for key in SettingsForm.model_fields:
            if key not in data:
                data[key] = defaults.get(key)
                
        settings = SettingsForm(**data)
        
        for key, value in settings.model_dump().items():
            st.session_state[key] = value
        
        del st.session_state["_load_settings_triggered"]
        st.rerun()
        
    except (json.JSONDecodeError, TypeError, ValueError, ValidationError):
        del st.session_state["_load_settings_triggered"]


def _submit_analysis() -> None:
    fl_id = st.query_params.get("fl_id")
    user_uid = st.session_state.get("user_uid")
    dataset_version = DatasetService(fl_id, user_uid).current_version
    analysis_id = AnalysisService(user_uid).next_analysis_id(fl_id)

    try:
        rank_by = st.session_state.get("rank_by", "Alpha")
        params = AnalysisParams(
            min_alpha=st.session_state.get("min_alpha", DEFAULT_MIN_ALPHA),
            top_pct=st.session_state.get("top_pct"),
            bottom_pct=st.session_state.get("bottom_pct"),
            correlation_threshold=st.session_state.get("correlation_threshold"),
            n_factors=st.session_state.get("n_factors"),
            max_na_pct=st.session_state.get("max_na_pct"),
            min_ic=float(st.session_state.get("min_ic", DEFAULT_MIN_IC)),
            rank_by=rank_by,
            access_token=st.session_state.get("access_token"),
        )
        AnalysisService(user_uid).start(fl_id, analysis_id, dataset_version, params)
        
        # Trigger save settings flow
        st.session_state["_save_settings_triggered"] = True
        st.session_state["_redirect_to_results"] = analysis_id
        
    except Exception as e:
        st.toast(f"Error starting analysis: {e}")


def create_form() -> None:
    ls = LocalStorage(key=SETTINGS_STORAGE_KEY)
    
    if st.session_state.get("_save_settings_triggered"):
        try:
            defaults = _get_default_settings()
            current_settings = {}
            for key in SettingsForm.model_fields:
                val = st.session_state.get(key)
                if val is None:
                    val = defaults.get(key)
                current_settings[key] = val
            
            ls.setItem(SETTINGS_STORAGE_KEY, json.dumps(current_settings))
            
            # have to sleep otherwise the settings are not saved
            time.sleep(0.1)
        except Exception as e:
            st.toast(f"Error saving settings: {e}")
        finally:
            del st.session_state["_save_settings_triggered"]
            st.rerun()

    if analysis_id := st.session_state.pop("_redirect_to_results", None):
        st.switch_page(
            st.session_state["pages"]["results"],
            query_params={
                "fl_id": st.query_params.get("fl_id"),
                "id": analysis_id,
            },
        )

    fl_id = st.query_params.get("fl_id")
    user_uid = st.session_state.get("user_uid") if INTERNAL_MODE else None

    try:
        with DatasetService(fl_id, user_uid) as svc:
            active_dataset_metadata = svc.get_metadata()
    except FileNotFoundError:
        if INTERNAL_MODE:
            st.warning(f"No dataset found for this Factor List. [Generate]({p123_link(fl_id, 'generate')})")
        else:
            st.warning("No dataset found. Please select a valid .parquet file.")
        return
    except Exception as e:
        st.error(f"Failed to load dataset: {e}")
        return

    st.title("Create Analysis")
    render_dataset_card(active_dataset_metadata)

    # Load settings early to ensure component is mounted and syncing
    saved_settings = ls.getItem(SETTINGS_STORAGE_KEY)
    _apply_settings_if_triggered(saved_settings)
    _render_settings()

    _, col_last_settings, col_run = st.columns([3, 1, 1])
    with col_last_settings:
        st.button(
            "Use Last Settings",
            type="secondary",
            on_click=lambda: st.session_state.update(_load_settings_triggered=True),
            width="stretch",
        )
    with col_run:
        st.button(
            "Run Analysis",
            type="primary",
            on_click=_submit_analysis,
            width="stretch",
        )


def _render_settings() -> None:
    defaults = _get_default_settings()

    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

    section_header("Portfolio Settings")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.radio(
            "Rank By",
            options=["Alpha", "Information Coefficient (IC)"],
            key="rank_by",
            horizontal=True,
            help="Select metric to rank factors by",
        )
    with col2:
        st.number_input(
            "Top X (Long) %",
            min_value=1.0,
            max_value=100.0,
            step=1.0,
            key="top_pct",
            help="Percentage of top-ranked stocks to go long",
        )
    with col3:
        st.number_input(
            "Bottom X (Short) %",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            key="bottom_pct",
            help="Percentage of bottom-ranked stocks to short (0 = long-only)",
        )

    section_header("Analysis Filters")
    rank_by = st.session_state.get("rank_by", "Alpha")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if rank_by == "Alpha":
            st.number_input(
                "Min. Abs. Annual Alpha (%)",
                min_value=0.0,
                max_value=100.0,
                step=0.1,
                key="min_alpha",
            )
        else:
            st.number_input(
                "Min. IC",
                min_value=0.0,
                max_value=1.0,
                step=0.01,
                key="min_ic",
            )
    with col2:
        st.number_input(
            "Max. Factors",
            min_value=1,
            max_value=100,
            step=1,
            key="n_factors",
            help="Maximum number of 'Best Factors' to select",
        )
    with col3:
        st.number_input(
            "Max. NA (%)",
            min_value=0.0,
            max_value=100.0,
            step=1.0,
            key="max_na_pct",
            help="If a factor has a higher percentage of NAs, it will be excluded",
        )
    with col4:
        st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            step=0.05,
            key="correlation_threshold",
            help="Maximum allowed correlation between selected factors",
        )
