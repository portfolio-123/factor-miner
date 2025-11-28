import json
import streamlit as st
from pathlib import Path

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import detect_file_type, get_local_storage
from src.ui.components import section_header
from src.data.readers import get_data_reader
from src.logic.calculations import fetch_benchmark_data, get_dataset_date_range

# disable continue button if required fields are not filled
def _check_required_fields() -> bool:
    state = get_state()
    api_key = st.session_state.get('api_key', '')
    api_id = st.session_state.get('api_id', '')

    if state.is_internal_app:
        return all([api_key.strip(), api_id.strip()]) and state.files_verified
    else:
        dataset = st.session_state.get('dataset_path', '')
        formulas = st.session_state.get('formulas_path', '')
        return all([dataset.strip(), formulas.strip(), api_key.strip(), api_id.strip()])


# validate all required inputs after continue button is clicked
def _validate_inputs() -> tuple[bool, str]:
    state = get_state()

    api_key = st.session_state.get('api_key', '')
    api_id = st.session_state.get('api_id', '')

    if state.is_internal_app:
        # Internal mode: check files verified, API key, and API ID
        if not state.files_verified:
            return False, state.files_verification_error or "Files not verified"

        if not api_key.strip():
            return False, "API Key is required"
        if not api_id.strip():
            return False, "API ID is required"
    else:
        # External mode: check dataset, formulas, API key, and API ID
        dataset_path = st.session_state.get('dataset_path', '')
        formulas_path = st.session_state.get('formulas_path', '')

        if not dataset_path.strip():
            return False, "Dataset path is required"
        if not formulas_path.strip():
            return False, "Formulas path is required"
        if not api_key.strip():
            return False, "API Key is required"
        if not api_id.strip():
            return False, "API ID is required"

        # Validate dataset file exists
        dataset_file = Path(dataset_path.strip())
        if not dataset_file.is_absolute():
            dataset_file = dataset_file.resolve()
        if not dataset_file.exists():
            return False, f"Dataset file not found: {dataset_path}"

        # Validate formulas file exists
        formulas_file = Path(formulas_path.strip())
        if not formulas_file.is_absolute():
            formulas_file = formulas_file.resolve()
        if not formulas_file.exists():
            return False, f"Formulas file not found: {formulas_path}"

    return True, ""


def _process_continue() -> bool:
    state = get_state()

    is_valid, error_msg = _validate_inputs()
    if not is_valid:
        st.session_state['step1_error'] = error_msg
        return False

    # clear any previous error on successful validation
    st.session_state['step1_error'] = None

    benchmark_ticker = st.session_state.get('benchmark_ticker', 'SPY:USA').strip() or 'SPY:USA'
    api_id = st.session_state.get('api_id', '').strip() or None
    api_key = st.session_state.get('api_key', '').strip()
    min_alpha = st.session_state.get('min_alpha', 0.5)
    top_x_pct = st.session_state.get('top_x_pct', 20)
    bottom_x_pct = st.session_state.get('bottom_x_pct', 20)

    add_debug_log(f"Processing Step 1: Benchmark={benchmark_ticker}")
    add_debug_log(f"Analysis Parameters - Min Alpha: {min_alpha}%, Top X: {top_x_pct}%, Bottom X: {bottom_x_pct}%")

    try:
        if state.is_internal_app:
            # Internal mode: use file-system located files
            dataset_file = state.auto_dataset_path
            formulas_file = state.auto_formulas_path
            file_type = state.auto_dataset_file_type

            

        else:
            # External mode: resolve paths provided by user
            dataset_path = st.session_state.get('dataset_path', '').strip()
            formulas_path = st.session_state.get('formulas_path', '').strip()

            dataset_file = Path(dataset_path)
            if not dataset_file.is_absolute():
                dataset_file = dataset_file.resolve()

            formulas_file = Path(formulas_path)
            if not formulas_file.is_absolute():
                formulas_file = formulas_file.resolve()

            file_type = detect_file_type(dataset_file)

        add_debug_log(f"Dataset file: {dataset_file}")
        add_debug_log(f"Formulas file: {formulas_file}")
        add_debug_log(f"Detected file type: {file_type}")

        
        # Validate dataset
        if state.is_internal_app and state.auto_dataset_file_type:
            dataset_reader = get_data_reader(dataset_file, file_type=state.auto_dataset_file_type)
        else:
            dataset_reader = get_data_reader(dataset_file)

        is_valid, validation_error = dataset_reader.validate()
        if not is_valid:
            st.session_state['step1_error'] = f"Invalid dataset: {validation_error}"
            return False

        # Load data based on file type
        if file_type == 'parquet':
            raw_data = None  # will be loaded on-demand in step 2
            metadata = dataset_reader.get_metadata()
            add_debug_log(f"Parquet validated: {metadata['num_rows']:,} rows, {metadata['num_columns']} columns")
            if state.is_internal_app:
                custom_metadata = dataset_reader.get_custom_metadata()
                if custom_metadata:
                    add_debug_log(f"Parquet custom metadata: {json.dumps(custom_metadata, indent=4)}")
        else:
            raw_data = dataset_reader.read_full()
            add_debug_log(f"CSV loaded: {len(raw_data):,} rows")

        # Load formulas
        if state.is_internal_app:
            formulas_reader = get_data_reader(formulas_file, file_type='csv')
        else:
            formulas_reader = get_data_reader(formulas_file)

        formulas_data = formulas_reader.read_full()
        add_debug_log(f"Formulas loaded: {len(formulas_data)} formulas")

        add_debug_log("Getting date range from dataset...")
        if file_type == 'parquet':
            date_df = dataset_reader.read_columns(['Date'])
        else:
            date_df = raw_data

        try:
            start_date, end_date = get_dataset_date_range(date_df)
            add_debug_log(f"Date range: {start_date} to {end_date}")
        except ValueError as e:
            st.session_state['step1_error'] = f"Error getting date range: {str(e)}"
            return False

        # Fetch benchmark data (validates API credentials)
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
            raw_data=raw_data,
            formulas_data=formulas_data,
            benchmark_data=benchmark_data,
            price_column=state.PRICE_COLUMN,
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
            'benchmark_ticker': benchmark_ticker,
            'min_alpha': min_alpha,
            'top_x_pct': top_x_pct,
            'bottom_x_pct': bottom_x_pct,
        }
        if not state.is_internal_app:
            settings_to_save['dataset_path'] = st.session_state.get('dataset_path', '')
            settings_to_save['formulas_path'] = st.session_state.get('formulas_path', '')
        get_local_storage().setItem('factor_eval_settings', json.dumps(settings_to_save))

        state.completed_steps.add(1)
        state.current_step = 2

        add_debug_log("Step 1 complete - Proceeding to Step 2")
        return True

    except Exception as e:
        add_debug_log(f"ERROR: {str(e)}")
        st.session_state['step1_error'] = f"Error processing data: {str(e)}"
        return False


def _load_saved_settings() -> dict:
    saved_data = get_local_storage().getAll() or {}
    try:
        return json.loads(saved_data.get('factor_eval_settings', '')) or {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _apply_saved_settings(saved: dict, is_internal: bool) -> None:
    for key in ('api_key', 'api_id', 'benchmark_ticker', "api_id"):
        if saved.get(key):
            st.session_state[key] = saved[key]

    if not is_internal:
        for key in ('dataset_path', 'formulas_path'):
            if saved.get(key):
                st.session_state[key] = saved[key]

    if saved.get('min_alpha') is not None:
        st.session_state['min_alpha'] = float(saved['min_alpha'])
    for key in ('top_x_pct', 'bottom_x_pct'):
        if saved.get(key) is not None:
            st.session_state[key] = int(saved[key])

    st.session_state['step1_error'] = None


def _restore_session_defaults(state) -> None:
    defaults = {
        'api_key': state.api_key,
        'api_id': state.api_id,
        'benchmark_ticker': state.benchmark_ticker or "SPY:USA",
        'min_alpha': state.min_alpha,
        'top_x_pct': int(state.top_x_pct),
        'bottom_x_pct': int(state.bottom_x_pct),
    }

    for key, value in defaults.items():
        if key not in st.session_state and value:
            st.session_state[key] = value

    if not state.is_internal_app:
        if 'dataset_path' not in st.session_state and state.dataset_path_input:
            st.session_state['dataset_path'] = state.dataset_path_input
        if 'formulas_path' not in st.session_state and state.formulas_path_input:
            st.session_state['formulas_path'] = state.formulas_path_input


def render() -> None:
    state = get_state()
    saved = _load_saved_settings()
    has_saved = bool(saved.get('api_key') or saved.get('dataset_path') or saved.get('formulas_path') or saved.get('api_id'))

    if st.session_state.pop('_apply_saved_settings', False):
        _apply_saved_settings(saved, state.is_internal_app)

    _restore_session_defaults(state)

    # Data Sources Section
    section_header("Data Sources")

    if state.is_internal_app:
        # Internal app mode - show auto-detected files
        st.text_input(
            "Factor List UID",
            value=state.factor_list_uid or "",
            disabled=True,
            key="factor_list_uid"
        )

        if state.files_verified:
            dataset_name = state.auto_dataset_path.stem if state.auto_dataset_path else ""
            formulas_display = state.auto_formulas_path.name if state.auto_formulas_path else ""

            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Dataset", value=dataset_name, disabled=True)
            with col2:
                st.text_input("Formulas", value=formulas_display, disabled=True)
        elif state.files_verification_error:
            st.error(state.files_verification_error)
        else:
            st.warning("No fl_id provided in URL")
    else:
        # External app mode - file path inputs
        col1, col2 = st.columns(2)

        with col1:
            st.text_input(
                "Dataset Path",
                placeholder="data/dataset.parquet",
                key="dataset_path",
            )

        with col2:
            st.text_input(
                "Formulas Path",
                placeholder="data/formulas.csv",
                key="formulas_path",
            )

        st.caption(
            "**Note:** Your dataset must contain a column named 'Last Close' for price data. We support both CSV and Parquet file formats."
        )

    section_header("Configuration")

    col1, col2 = st.columns(2)
    with col1:
        st.text_input(
            "Benchmark Ticker",
            placeholder="SPY:USA",
            key="benchmark_ticker",
        )
    with col2:
        st.number_input(
            "Min Absolute Alpha (%)",
            min_value=0.0,
            max_value=100.0,
            step=0.1,
            key="min_alpha",
        )

    col1, col2 = st.columns(2)
    with col1:
        st.number_input(
            "Top X (%)",
            min_value=1,
            max_value=100,
            step=1,
            key="top_x_pct",
        )
    with col2:
        st.number_input(
            "Bottom X (%)",
            min_value=1,
            max_value=100,
            step=1,
            key="bottom_x_pct",
        )

    section_header("Authentication")

    col1, col2 = st.columns([2, 1])
    with col1:
        st.text_input(
            "API Key",
            placeholder="Enter API Key",
            key="api_key",
        )
    with col2:
        st.text_input(
            "API ID",
            placeholder="Enter API ID",
            key="api_id",
        )

    error_placeholder = st.empty()

    # display any existing error
    if st.session_state.get('step1_error'):
        error_placeholder.error(st.session_state['step1_error'])

    # check if required fields are filled to enable Continue button
    can_continue = _check_required_fields()

    col1, col2, col3 = st.columns([2, 1, 1])
    with col2:
        if has_saved:
            if st.button("Use saved settings", width='stretch'):
                # set flag to apply settings on next render (before widgets are created)
                st.session_state['_apply_saved_settings'] = True
                st.rerun()
    with col3:
        is_loading = st.session_state.get('step1_loading', False)

        if is_loading:
            st.markdown('''
            <div class="spinner-button">
                <div class="spinner"></div>
            </div>
            ''', unsafe_allow_html=True)
        else:
            if st.button("Continue", type="primary", width='stretch', disabled=not can_continue):
                st.session_state.step1_loading = True
                st.rerun()

    # process continue action after rerun if loading flag is set
    if st.session_state.get('step1_loading', False):
        st.session_state.step1_loading = False
        _process_continue()
        # always rerun to update UI (show button again, display any errors)
        st.rerun()
