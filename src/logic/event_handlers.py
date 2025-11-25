from src.data.readers import get_data_reader
from src.logic.calculations import (
    fetch_benchmark_data,
    get_dataset_date_range,
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
    select_best_features
)
from src.ui.components import show_error
from src.core.utils import log_debug
from src.core.context import state
from pathlib import Path


def handle_step_navigation(step_num: int) -> None:
    # only allow navigation to completed steps
    if step_num not in state.completed_steps:
        return

    # already on this step
    if state.current_step == step_num:
        return

    log_debug(f"Navigating from step {state.current_step} to step {step_num}")

    # hide current step
    if state.current_step == 1 and state.step1_container:
        state.step1_container.layout.display = 'none'
    elif state.current_step == 2 and state.step2_container:
        state.step2_container.layout.display = 'none'
    elif state.current_step == 3 and state.step3_container:
        state.step3_container.layout.display = 'none'

    # update current step
    state.current_step = step_num

    # rebuild target step (without recalculating)
    if step_num == 1:
        if state.continue_button:
            state.continue_button.description = 'Continue'
            state.continue_button.disabled = False
            from src.logic.event_handlers import update_continue_button
            state.suppress_invalidation = True
            update_continue_button()

        from src.ui.step_builders import build_step1
        build_step1(revisit=True)

        # allow invalidation after render completes
        state.suppress_invalidation = False
    elif step_num == 2:
        if state.analyze_button:
            state.analyze_button.description = 'Analyze'
            state.analyze_button.disabled = False

        from src.ui.step_builders import build_step2
        # load formulas data for display
        formulas_file = Path(state.formulas_input.value.strip())
        formulas_reader = get_data_reader(formulas_file)
        formulas_data = formulas_reader.read_full()

        build_step2(
            state.raw_data,
            formulas_data,
            state.benchmark_ticker,
            state.step1_container,
            state.step2_container,
            revisit=True
        )
    elif step_num == 3:
        from src.ui.step_builders import build_step3
        # recalculate best features with current parameters
        best_features = select_best_features(
            state.all_metrics,
            state.all_corr_matrix,
            N=state.n_features,
            correlation_threshold=state.correlation_threshold,
            a_min=state.min_alpha
        )
        # build_step3 with revisit=True already handles showing the container
        build_step3(best_features, state.all_metrics, state.step2_container, revisit=True)


def update_continue_button() -> None:
    """Enable/disable Continue button based on form inputs.

    Also handles invalidation of future steps if step 1 data changes
    """

    # Check required fields based on mode
    if state.is_internal_app:
        # Internal mode: require files to be verified and API key
        has_api_key = bool(state.api_key_input.value and state.api_key_input.value.strip())
        state.continue_button.disabled = not (state.files_verified and has_api_key)
    else:
        # External mode: require dataset, formulas, and API key
        has_dataset = bool(state.dataset_input.value and state.dataset_input.value.strip())
        has_formulas = bool(state.formulas_input.value and state.formulas_input.value.strip())
        has_api_key = bool(state.api_key_input.value and state.api_key_input.value.strip())
        state.continue_button.disabled = not (has_dataset and has_formulas and has_api_key)

    # only invalidate if on step 1, not suppressed, and actual changes detected
    if (
        state.current_step == 1
        and not state.suppress_invalidation
        and 1 in state.completed_steps
        and (2 in state.completed_steps or 3 in state.completed_steps)
    ):
        def normalize_path(p: str) -> str:
            try:
                path_obj = Path(p)
                if not path_obj.is_absolute():
                    path_obj = path_obj.resolve()
                return str(path_obj)
            except Exception:
                return (p or "").strip()

        # current values (conditional on mode)
        if state.is_internal_app:
            curr_dataset = ""
            curr_formulas = ""
            curr_factor_list = (state.factor_list_uid_input.value or "").strip() if state.factor_list_uid_input else ""
        else:
            curr_dataset = (state.dataset_input.value or "").strip()
            curr_formulas = (state.formulas_input.value or "").strip()
            curr_factor_list = ""

        curr_benchmark = (state.benchmark_input.value or "").strip() if state.benchmark_input else ""
        curr_api_id = (state.api_id_input.value or "").strip() if state.api_id_input else ""
        curr_api_key = (state.api_key_input.value or "").strip() if state.api_key_input else ""
        curr_min_alpha = float(state.min_alpha_input.value) if state.min_alpha_input is not None else state.min_alpha
        curr_top_x = int(state.top_x_input.value) if state.top_x_input is not None else int(state.top_x_pct)
        curr_bottom_x = int(state.bottom_x_input.value) if state.bottom_x_input is not None else int(state.bottom_x_pct)

        # values you had when you submitted step 1
        baseline_dataset = str(state.dataset_path) if state.dataset_path else ""
        baseline_formulas = str(getattr(state, 'formulas_path', "") or "")
        baseline_benchmark = state.benchmark_ticker or ""
        baseline_api_id = state.api_id or ""
        baseline_api_key = state.api_key or ""
        baseline_min_alpha = state.min_alpha
        baseline_top_x = int(state.top_x_pct)
        baseline_bottom_x = int(state.bottom_x_pct)


        # check if any of the values have changed
        changed = (
            normalize_path(curr_dataset) != normalize_path(baseline_dataset)
            or normalize_path(curr_formulas) != normalize_path(baseline_formulas)
            or curr_benchmark != baseline_benchmark
            or curr_api_id != baseline_api_id
            or curr_api_key != baseline_api_key
            or curr_min_alpha != baseline_min_alpha
            or curr_top_x != baseline_top_x
            or curr_bottom_x != baseline_bottom_x
        )

        if changed:
            log_debug("Step 1 data changed - invalidating Steps 2 and 3")
            state.completed_steps.discard(2)
            state.completed_steps.discard(3)

            # clear results
            state.all_metrics = None
            state.all_corr_matrix = None
            state.raw_data = None
            state.benchmark_data = None

            # build header again, because values have changed and user shouldn't be allowed to navigate to step 2 or 3
            if state.current_step == 1:
                from src.ui.step_builders import build_step1
                build_step1(revisit=True)


def setup_event_handlers() -> None:
    """Setup observers for form inputs."""

    # External mode: observe dataset and formulas inputs
    if state.dataset_input:
        state.dataset_input.observe(
            lambda change: update_continue_button(),
            names='value'
        )

    if state.formulas_input:
        state.formulas_input.observe(
            lambda change: update_continue_button(),
            names='value'
        )

    # Internal mode: observe factor list UID input
    if state.factor_list_uid_input:
        state.factor_list_uid_input.observe(
            lambda change: update_continue_button(),
            names='value'
        )

    state.api_key_input.observe(
        lambda change: [
            update_continue_button(),
            setattr(state.form_error, 'value', '')
        ],
        names='value'
    )

    state.benchmark_input.observe(lambda change: update_continue_button(), names='value')

    state.api_id_input.observe(lambda change: update_continue_button(), names='value')

    state.min_alpha_input.observe(lambda change: update_continue_button(), names='value')

    state.top_x_input.observe(lambda change: update_continue_button(), names='value')

    state.bottom_x_input.observe(lambda change: update_continue_button(), names='value')


def handle_continue_click() -> None:
    """Handle Continue button click in Step 1."""
    try:
        log_debug("Continue button clicked - Processing Step 1...")

        state.continue_button.description = 'Loading...'
        state.continue_button.disabled = True
        state.form_error.value = ''

        benchmark_ticker = state.benchmark_input.value.strip() if state.benchmark_input.value.strip() else 'SPY:USA'
        api_id = state.api_id_input.value.strip() if state.api_id_input.value.strip() else None
        api_key = state.api_key_input.value.strip()

        state.min_alpha = state.min_alpha_input.value
        state.top_x_pct = state.top_x_input.value
        state.bottom_x_pct = state.bottom_x_input.value

        log_debug(f"Benchmark: {benchmark_ticker}")
        log_debug(f"API ID: {api_id}")
        log_debug("API Key configured")
        log_debug(f"Analysis Parameters - Min Alpha: {state.min_alpha}%, Top X: {state.top_x_pct}%, Bottom X: {state.bottom_x_pct}%")

        # Internal app mode: use auto-located files
        if state.is_internal_app:
            if not state.files_verified:
                error_msg = state.files_verification_error or "Files not verified"
                log_debug(f"ERROR: {error_msg}")
                show_error(error_msg, state.form_error)
                state.continue_button.description = 'Continue'
                update_continue_button()
                return

            dataset_file = state.auto_dataset_path
            formulas_file = state.auto_formulas_path
            log_debug(f"Using auto-located files for fl_id: {state.factor_list_uid}")
        else:
            # External app mode: get paths from text inputs
            dataset_path = state.dataset_input.value.strip()
            formulas_path = state.formulas_input.value.strip()

            # validate file paths (support both absolute and relative)
            dataset_file = Path(dataset_path)
            if not dataset_file.is_absolute():
                dataset_file = dataset_file.resolve()

            if not dataset_file.exists():
                error_msg = f"Dataset file not found: {dataset_path}"
                log_debug(f"ERROR: {error_msg}")
                show_error(error_msg, state.form_error)
                state.continue_button.description = 'Continue'
                update_continue_button()
                return

            formulas_file = Path(formulas_path)
            if not formulas_file.is_absolute():
                formulas_file = formulas_file.resolve()

            if not formulas_file.exists():
                error_msg = f"Formulas file not found: {formulas_path}"
                log_debug(f"ERROR: {error_msg}")
                show_error(error_msg, state.form_error)
                state.continue_button.description = 'Continue'
                update_continue_button()
                return

        log_debug(f"Dataset file: {dataset_file}")
        log_debug(f"Formulas file: {formulas_file}")

        # get_data_reader() returns a csv or parquet reader based on the file type
        # In internal app mode, use pre-detected file type (from magic bytes)
        try:
            if state.is_internal_app and state.auto_dataset_file_type:
                dataset_reader = get_data_reader(dataset_file, file_type=state.auto_dataset_file_type)
            else:
                dataset_reader = get_data_reader(dataset_file)
        except ValueError as e:
            log_debug(f"ERROR: {str(e)}")
            show_error(str(e), state.form_error)
            state.continue_button.description = 'Continue'
            update_continue_button()
            return

        # Set file type: use pre-detected type in internal mode, otherwise from extension
        if state.is_internal_app and state.auto_dataset_file_type:
            state.file_type = state.auto_dataset_file_type
        else:
            state.file_type = 'parquet' if dataset_file.suffix.lower() == '.parquet' else 'csv'
        log_debug(f"Detected file type: {state.file_type}")

        # validate required columns are present
        is_valid, error_msg = dataset_reader.validate()
        if not is_valid:
            log_debug(f"ERROR: Validation failed: {error_msg}")
            show_error(f"Invalid file: {error_msg}", state.form_error)
            state.continue_button.description = 'Continue'
            update_continue_button()
            return

        if state.file_type == 'parquet':
            state.dataset_path = dataset_file

            metadata = dataset_reader.get_metadata()
            log_debug("Parquet file validated successfully")
            log_debug(f"Rows: {metadata['num_rows']:,}, Columns: {metadata['num_columns']}")

            raw_data = None  # will be loaded on-demand in step 2
        else:
            state.dataset_path = dataset_file
            raw_data = dataset_reader.read_full()

        try:
            # In internal app mode, formulas files are always CSV (no extension)
            if state.is_internal_app:
                formulas_reader = get_data_reader(formulas_file, file_type='csv')
            else:
                formulas_reader = get_data_reader(formulas_file)
        except ValueError as e:
            log_debug(f"ERROR: {str(e)}")
            show_error(str(e), state.form_error)
            state.continue_button.description = 'Continue'
            update_continue_button()
            return

        formulas_data = formulas_reader.read_full()
        log_debug(f"Formulas file loaded: {formulas_file.name}")

        state.raw_data = raw_data
        state.price_column = state.PRICE_COLUMN
        state.benchmark_ticker = benchmark_ticker
        state.api_id = api_id
        state.api_key = api_key
        state.formulas_path = formulas_file

        # sync step 1 inputs to values you initially submitted
        state.benchmark_input.value = benchmark_ticker
        state.api_id_input.value = api_id or ''
        state.api_key_input.value = api_key

        log_debug(f"Using price column: {state.PRICE_COLUMN}")
        log_debug("Step 1 complete - Proceeding to Step 2...")

        state.completed_steps.add(1)

        from src.ui.step_builders import build_step2
        build_step2(
            raw_data,
            formulas_data,
            benchmark_ticker,
            state.step1_container,
            state.step2_container
        )

    except Exception as e:
        log_debug(f"ERROR: Unexpected error: {str(e)}")
        import traceback
        with state.debug_output:
            traceback.print_exc()

        show_error(f"Unexpected error: {str(e)}", state.form_error)
        state.continue_button.description = 'Continue'
        update_continue_button()


def handle_analyze_factors_click() -> None:
    """Handle Analyze button click in Step 2."""

    try:
        log_debug("Analyze Factors button clicked - Starting analysis...")

        state.analyze_button.description = 'Analyzing...'
        state.analyze_button.disabled = True

        raw_data = state.raw_data
        benchmark_ticker = state.benchmark_ticker
        api_id = state.api_id
        api_key = state.api_key
        price_column = state.PRICE_COLUMN
        file_type = state.file_type

        # read date fields from the parquet file
        if file_type == 'parquet':
            log_debug("Loading Parquet file for date range calculation...")
            reader = get_data_reader(state.dataset_path)
            date_df = reader.read_columns(['Date'])
        else:
            if raw_data is None or raw_data.empty:
                log_debug("ERROR: No data available for analysis")
                state.analyze_button.description = 'Analyze'
                state.analyze_button.disabled = False
                return
            date_df = raw_data

        # to fetch benchmark data, first check what date range the dataset has, to match it on the fetch request
        try:
            start_date, end_date = get_dataset_date_range(date_df)
            log_debug(f"Date range: {start_date} to {end_date}")
        except ValueError as e:
            log_debug(f"ERROR: Error getting date range: {str(e)}")
            state.analyze_button.description = 'Analyze'
            state.analyze_button.disabled = False
            return

        log_debug(f"Fetching benchmark data for {benchmark_ticker}...")

        benchmark_data, error = fetch_benchmark_data(
            benchmark_ticker,
            api_key,
            start_date,
            end_date,
            api_id
        )

        if error:
            log_debug(f"ERROR: Error fetching benchmark data: {error}")
            state.analyze_button.description = 'Analyze'
            state.analyze_button.disabled = False
            return

        log_debug("Benchmark data fetched successfully")

        log_debug("Calculating future performance...")

        def progress_callback(msg):
            log_debug(msg)

        if file_type == 'parquet':
            # for parquet: only load required columns
            future_perf_df = calculate_future_performance(
                None,
                price_column,
                parquet_path=state.dataset_path,
                progress_callback=progress_callback
            )

            # Get factor columns
            reader = get_data_reader(state.dataset_path)
            all_columns = reader.get_column_names()
            excluded_columns = ['Date', 'Ticker', 'P123 ID', price_column]
            factor_columns = [col for col in all_columns if col not in excluded_columns]

            log_debug(f"Identified {len(factor_columns)} factor columns for analysis")
        else:
            # CSV path - calculate benchmark returns first
            log_debug("Calculating benchmark returns...")
            raw_data, _ = calculate_benchmark_returns(raw_data, benchmark_data)

            future_perf_df = calculate_future_performance(
                raw_data,
                price_column,
                progress_callback=progress_callback
            )

            # Merge future perf back into raw_data
            raw_data = raw_data.merge(future_perf_df, on=['Date', 'Ticker'], how='left')

        state.benchmark_data = benchmark_data

        log_debug("Analyzing factors...")

        if file_type == 'parquet':
            results_df = analyze_factors(
                None,
                progress_callback,
                future_perf_df=future_perf_df,
                parquet_path=state.dataset_path,
                factor_columns=factor_columns,
                top_pct=state.top_x_pct,
                bottom_pct=state.bottom_x_pct
            )

            log_debug("Preparing benchmark data for metrics calculation...")

            reader = get_data_reader(state.dataset_path)
            date_ticker_df = reader.read_columns(['Date', 'Ticker'])
            date_ticker_df, _ = calculate_benchmark_returns(date_ticker_df, benchmark_data)
            raw_data = date_ticker_df
        else:
            results_df = analyze_factors(
                raw_data,
                progress_callback,
                top_pct=state.top_x_pct,
                bottom_pct=state.bottom_x_pct
            )

        if results_df.empty:
            log_debug("WARNING: No results from factor analysis")
            state.analyze_button.description = 'Analyze'
            state.analyze_button.disabled = False
            return

        log_debug("Calculating factor metrics...")
        metrics_df = calculate_factor_metrics(results_df, raw_data)

        log_debug("Calculating correlation matrix...")
        corr_matrix = calculate_correlation_matrix(results_df)

        # store full results in state for Step 3 filtering
        state.all_metrics = metrics_df
        state.all_corr_matrix = corr_matrix

        log_debug("Selecting best features...")
        best_features = select_best_features(
            metrics_df,
            corr_matrix,
            N=state.n_features,
            correlation_threshold=state.correlation_threshold,
            a_min=state.min_alpha
        )

        log_debug(f"Analysis complete! Found {len(best_features)} best features")
        log_debug(f"Best features: {', '.join(best_features)}")

        state.current_step = 3

        # Mark Steps 2 and 3 as completed
        state.completed_steps.add(2)
        state.completed_steps.add(3)

        from src.ui.step_builders import build_step3
        step3 = build_step3(best_features, metrics_df, state.step2_container)

        state.step3_container.children = [step3]
        state.step3_container.layout.display = 'flex'

        log_debug("Analysis complete! Proceeding to Step 3...")

    except Exception as e:
        log_debug(f"ERROR: Error during factor analysis: {str(e)}")
        import traceback
        with state.debug_output:
            traceback.print_exc()

        state.analyze_button.description = 'Analyze'
        state.analyze_button.disabled = False
