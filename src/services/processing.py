import json
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import streamlit as st

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import detect_file_type, get_local_storage
from src.core.validation import validate_inputs
from src.services.readers import get_data_reader
from src.services.p123_client import fetch_benchmark_data
from src.core.calculations import get_dataset_date_range
from src.core.constants import FileType, DEFAULT_BENCHMARK


def load_formulas_data(
    dataset_path: Union[str, Path],
    file_type: FileType,
    fl_id: Optional[str] = None,
    formulas_path: Optional[Union[str, Path]] = None
) -> Optional[pd.DataFrame]:
    try:
        reader = get_data_reader(dataset_path, file_type=file_type)
        if file_type == FileType.PARQUET:
            return reader.get_formulas_from_metadata()
        # CSV
        state = get_state()
        meta_path: Path = None
        if state.is_internal_app:
            meta_path = Path(dataset_path).parent / f"{fl_id}_meta"
        else:
            meta_path = Path(formulas_path)
        if meta_path.exists():
            return get_data_reader(str(meta_path), file_type=FileType.CSV).read_full()
    except Exception as e:
        add_debug_log(f"Error loading formulas data: {e}")
    return None


def process_step1() -> bool:
    state = get_state()

    is_valid, error_msg = validate_inputs()
    if not is_valid:
        st.session_state['step1_error'] = error_msg
        return False

    st.session_state['step1_error'] = None

    benchmark_ticker = st.session_state.get('benchmark_ticker', DEFAULT_BENCHMARK).strip() or DEFAULT_BENCHMARK
    api_id = st.session_state.get('api_id', '').strip() or None
    api_key = st.session_state.get('api_key', '').strip()
    min_alpha = st.session_state.get('min_alpha', 0.5)
    top_x_pct = st.session_state.get('top_x_pct', 20)
    bottom_x_pct = st.session_state.get('bottom_x_pct', 20)

    add_debug_log(f"Processing Step 1: Benchmark={benchmark_ticker}")
    add_debug_log(f"Analysis Parameters - Min Alpha: {min_alpha}%, Top X: {top_x_pct}%, Bottom X: {bottom_x_pct}%")

    try:
        if state.is_internal_app:
            dataset_file = state.dataset_path
            file_type = state.file_type
            if file_type != FileType.PARQUET:
                formulas_file = state.formulas_path
            else:
                formulas_file = None
        else:
            dataset_path = st.session_state.get('dataset_path', '').strip()
            dataset_file = Path(dataset_path)
            if not dataset_file.is_absolute():
                dataset_file = dataset_file.resolve()
            file_type = detect_file_type(dataset_file)
            if file_type != FileType.PARQUET:
                formulas_path = st.session_state.get('formulas_path', '').strip()
                formulas_file = Path(formulas_path)
                if not formulas_file.is_absolute():
                    formulas_file = formulas_file.resolve()
            else:
                formulas_file = None

        add_debug_log(f"Dataset file: {dataset_file}")
        add_debug_log(f"Formulas file: {formulas_file}")
        add_debug_log(f"Detected file type: {file_type}")

        dataset_reader = get_data_reader(dataset_file, file_type=file_type)

        is_valid, validation_error = dataset_reader.validate()
        if not is_valid:
            st.session_state['step1_error'] = f"Invalid dataset: {validation_error}"
            return False

        if file_type == FileType.PARQUET:
            raw_data = None
            metadata = dataset_reader.get_metadata()
            add_debug_log(f"Parquet validated: {metadata['num_rows']:,} rows, {metadata['num_columns']} columns")
        else:
            raw_data = dataset_reader.read_full()
            add_debug_log(f"CSV loaded: {len(raw_data):,} rows")

        formulas_data = load_formulas_data(
            dataset_path=dataset_file,
            file_type=file_type,
            fl_id=state.factor_list_uid,
            formulas_path=str(formulas_file) if formulas_file else None
        )
        if formulas_data is None:
            if file_type == FileType.PARQUET:
                st.session_state['step1_error'] = "Parquet file missing 'features' metadata with formula definitions"
            else:
                st.session_state['step1_error'] = "Formulas CSV not found or failed to load"
            return False
        add_debug_log(f"Formulas loaded: {len(formulas_data)} formulas")

        add_debug_log("Getting date range from dataset...")
        if file_type == FileType.PARQUET:
            date_df = dataset_reader.read_columns(['Date'])
        else:
            date_df = raw_data

        try:
            start_date, end_date = get_dataset_date_range(date_df)
            add_debug_log(f"Date range: {start_date} to {end_date}")
        except ValueError as e:
            st.session_state['step1_error'] = f"Error getting date range: {str(e)}"
            return False

        add_debug_log(f"Fetching benchmark data for {benchmark_ticker}...")
        benchmark_data, error = fetch_benchmark_data(
            benchmark_ticker,
            api_key,
            start_date,
            end_date,
            api_id
        )

        if error:
            add_debug_log(f"Benchmark fetch failed: {error}")
            st.session_state['step1_error'] = f"Error fetching benchmark data: {error}"
            return False

        add_debug_log("Benchmark data fetched successfully")

        state_updates = dict(
            dataset_path=dataset_file,
            formulas_path=formulas_file,
            file_type=file_type,
            formulas_data=formulas_data,
            benchmark_data=benchmark_data,
            benchmark_ticker=benchmark_ticker,
            api_id=api_id,
            api_key=api_key,
            min_alpha=min_alpha,
            top_x_pct=float(top_x_pct),
            bottom_x_pct=float(bottom_x_pct)
        )
        # store original paths that the user entered for form restoration, not the resolved ones
        if not state.is_internal_app:
            state_updates['dataset_path_input'] = st.session_state.get('dataset_path', '')
            state_updates['formulas_path_input'] = st.session_state.get('formulas_path', '')
        update_state(**state_updates)

        # save all settings to localStorage as single json object
        settings_to_save = {
            'api_key': api_key,
            'api_id': api_id or '',
        }
        get_local_storage().setItem('factor_eval_settings', json.dumps(settings_to_save))

        state.completed_steps.add(1)
        state.current_step = 2

        add_debug_log("Step 1 complete - Proceeding to Step 2")
        return True

    except Exception as e:
        add_debug_log(f"ERROR: {str(e)}")
        st.session_state['step1_error'] = f"Error processing data: {str(e)}"
        return False


