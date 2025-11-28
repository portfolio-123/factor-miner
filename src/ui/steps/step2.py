import streamlit as st
import pandas as pd

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import format_date
from src.ui.components import (
    section_header,
    render_formulas_grid,
    render_dataset_preview
)
from src.data.readers import get_data_reader
from src.logic.calculations import (
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
    select_best_features
)


def _set_error(message: str) -> None:
    st.session_state['step2_error'] = message


def _run_analysis() -> bool:
    state = get_state()

    st.session_state['step2_error'] = None

    add_debug_log("Starting factor analysis...")

    try:
        raw_data = state.raw_data
        price_column = state.PRICE_COLUMN
        file_type = state.file_type
        benchmark_data = state.benchmark_data

        # read only required columns for parquet files
        if file_type == 'parquet':
            reader = get_data_reader(state.dataset_path)
            perf_data = reader.read_columns(['Date', 'Ticker', price_column])
            columns = reader.get_column_names()
            excluded_columns = ['Date', 'Ticker', 'P123 ID', price_column]
            factor_columns = [col for col in columns if col not in excluded_columns]
        else:
            perf_data = raw_data
            factor_columns = None

        future_perf_df = calculate_future_performance(perf_data, price_column)

        add_debug_log("Analyzing factors...")

        if file_type == 'parquet':
            results_df = analyze_factors(
                None,
                future_perf_df,
                parquet_path=state.dataset_path,
                factor_columns=factor_columns,
                top_pct=state.top_x_pct,
                bottom_pct=state.bottom_x_pct
            )
            # Read Date/Ticker for benchmark calculation
            date_ticker_df = reader.read_columns(['Date', 'Ticker'])
            raw_data = date_ticker_df
        else:
            results_df = analyze_factors(
                raw_data,
                future_perf_df,
                top_pct=state.top_x_pct,
                bottom_pct=state.bottom_x_pct
            )

        add_debug_log("Calculating benchmark returns...")
        raw_data, _ = calculate_benchmark_returns(raw_data, benchmark_data)

        if results_df.empty:
            _set_error("No results from factor analysis")
            return False

        add_debug_log("Calculating factor metrics...")
        metrics_df = calculate_factor_metrics(results_df, raw_data)

        add_debug_log("Calculating correlation matrix...")
        corr_matrix = calculate_correlation_matrix(results_df)

        # store results in state to access in step 3
        update_state(
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            raw_data=raw_data
        )

        # select best features
        add_debug_log("Selecting best features...")
        best_features = select_best_features(
            metrics_df,
            corr_matrix,
            N=state.n_features,
            correlation_threshold=state.correlation_threshold,
            a_min=state.min_alpha
        )

        add_debug_log(f"Analysis complete! Found {len(best_features)} best features")

        state.completed_steps.add(2)
        state.completed_steps.add(3)
        state.current_step = 3

        return True

    except Exception as e:
        add_debug_log(f"ERROR: {str(e)}")
        import traceback
        add_debug_log(traceback.format_exc())
        _set_error(f"Error during analysis: {str(e)}")
        return False


def render() -> None:
    state = get_state()

    # load preview data if needed
    if state.file_type == 'parquet':
        reader = get_data_reader(state.dataset_path)
        preview_df = reader.read_preview(num_rows=10)
        metadata = reader.get_metadata()
        actual_row_count = metadata.get('num_rows', len(preview_df))
        unique_dates = metadata.get('unique_dates')
    else:
        preview_df = state.raw_data
        actual_row_count = len(preview_df) if preview_df is not None else 0
        unique_dates = None

    if preview_df is None or preview_df.empty:
        st.error("No data available for preview")
        return

    # Calculate statistics
    num_rows = actual_row_count
    num_columns = len(preview_df.columns)
    dates = pd.to_datetime(preview_df['Date'])
    num_unique_dates = unique_dates if unique_dates is not None else dates.nunique()
    min_date = format_date(dates.min())
    max_date = format_date(dates.max())

    section_header("Dataset Statistics")

    cols = st.columns([1,1,1,2,1], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"
    stats = [
        ("Rows", num_rows),
        ("Dates", num_unique_dates),
        ("Columns", num_columns),
        ("Period", f"{min_date} - {max_date}"),
        ("Benchmark", state.benchmark_ticker or "N/A"),
    ]
    for col, (label, value) in zip(cols, stats):
        with col:
            st.badge(label)
            st.markdown(f"<p style='{stat_style}'>{value}</p>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Formulas", "Dataset Preview"])

    with tab1:
        if state.formulas_data is not None:
            render_formulas_grid(state.formulas_data)
        else:
            st.info("No formulas data available")

    with tab2:
        render_dataset_preview(preview_df, actual_row_count)

    # display error message above button (if any)
    if st.session_state.get('step2_error'):
        st.error(st.session_state['step2_error'])

    _, _, col3 = st.columns([2, 1, 1])
    with col3:
        is_running = st.session_state.get('analysis_running', False)

        if is_running:
            st.markdown('''
            <div class="spinner-button">
                <div class="spinner"></div>
                <span>Analyzing</span>
            </div>
            ''', unsafe_allow_html=True)
        else:
            if st.button("Run Analysis", type="primary", width='stretch'):
                st.session_state.analysis_running = True
                st.rerun()

    # run analysis after rerun if flagged
    if st.session_state.get('analysis_running', False):
        if _run_analysis():
            st.session_state.analysis_running = False
            st.rerun()
        else:
            st.session_state.analysis_running = False
