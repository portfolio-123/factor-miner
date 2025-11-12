import ipywidgets as widgets
import pandas as pd
from typing import Optional
from ipydatagrid import TextRenderer

from src.ui.components import (
    step_indicator,
    file_path_input,
    text_input,
    number_input,
    button,
    centered_container,
    brand_section,
    section_header,
    create_data_grid,
    review_stat_box
)
from src.data.readers import get_data_reader
from src.core.utils import log_debug, format_date
from src.core.context import state

def build_header(current_step: int, width: str = '700px', update_only: bool = False) -> widgets.HBox:
    """
    Reusable header with brand, step indicator, and logs button.

    Args:
        current_step: The current step number (1, 2, or 3)
        width: Width of the header
        update_only: If True, only update the step indicator without rebuilding the entire header

    Returns:
        HBox widget containing the header
    """

    # if we have an existing header, just update the step indicator
    if update_only and state.header_container is not None and state.step_indicator_widget is not None:
        new_step_indicator = step_indicator(current_step, state.completed_steps)

        # only replace the middle element (which is the step indicator)
        children = list(state.header_container.children)
        children[1] = new_step_indicator
        state.header_container.children = children

        # update stored reference
        state.step_indicator_widget = new_step_indicator

        return state.header_container

    brand = brand_section()
    step_ind = step_indicator(current_step, state.completed_steps)

    header = widgets.HBox(
        [brand, step_ind, state.debug_toggle_button],
        layout=widgets.Layout(
            justify_content='space-between',
            align_items='center',
            width=width,
            margin='0 0 15px 0'
        )
    )

    state.header_container = header
    state.step_indicator_widget = step_ind

    return header


def build_step1(revisit: bool = False) -> widgets.VBox:
    """Build Step 1 UI (data configuration).

    Args:
        revisit: If True, populate fields with existing state values

    Returns:
        VBox widget containing Step 1
    """

    # if revisiting and container exists, just show it with updated header
    if revisit and state.step1_container is not None:
        new_header = build_header(1, update_only=False)
        root_box = state.step1_container.children[0]
        root_children = list(root_box.children)
        root_children[0] = new_header
        root_box.children = tuple(root_children)

        state.step1_container.layout.display = 'flex'
        return state.step1_container

    # Conditional UI based on INTERNAL_APP mode
    if state.is_internal_app:
        factor_list_uid_input = text_input(
            'Factor List UID',
            placeholder='Enter Factor List UID'
        )

        # auto-populate from url parameter if available
        if state.factor_list_uid:
            factor_list_uid_input.value = state.factor_list_uid

        state.dataset_input = None
        state.formulas_input = None
        state.factor_list_uid_input = factor_list_uid_input

        data_sources_widgets = [factor_list_uid_input]
    else:
        # External app mode: original dataset and formulas inputs
        dataset_input, dataset_row = file_path_input('Dataset', placeholder='data/sp500.parquet')
        formulas_input, formulas_row = file_path_input('Formulas', placeholder='formulas.csv')

        state.dataset_input = dataset_input
        state.formulas_input = formulas_input

        data_sources_widgets = [dataset_row, formulas_row]

    benchmark_input = text_input('Benchmark', placeholder='SPY:USA')

    # auto-populate benchmark from url parameter if available
    if state.benchmark_ticker:
        benchmark_input.value = state.benchmark_ticker
    api_id_input = text_input('API ID', placeholder='Enter API ID')
    api_key_input = text_input('API Key', placeholder='Enter API Key')

    # restore values you had when you submitted step 1
    if revisit:
        if state.dataset_input and state.dataset_input.value:
            state.dataset_input.value = state.dataset_input.value
        if state.formulas_input and state.formulas_input.value:
            state.formulas_input.value = state.formulas_input.value
        if state.benchmark_input and state.benchmark_input.value:
            benchmark_input.value = state.benchmark_input.value
        if state.api_id_input and state.api_id_input.value:
            api_id_input.value = state.api_id_input.value
        if state.api_key_input and state.api_key_input.value:
            api_key_input.value = state.api_key_input.value

    min_alpha_value = state.min_alpha if revisit and state.min_alpha is not None else 0.5
    top_x_value = int(state.top_x_pct) if revisit and state.top_x_pct is not None else 20
    bottom_x_value = int(state.bottom_x_pct) if revisit and state.bottom_x_pct is not None else 20

    min_alpha_input = number_input('Min Absolute Alpha (%)', value=min_alpha_value, min_value=0.0, max_value=100.0, step=0.1)
    top_x_input = widgets.BoundedIntText(
        value=top_x_value,
        min=1,
        max=100,
        step=1,
        description='Top X (%):',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='700px', margin='5px 0')
    )
    bottom_x_input = widgets.BoundedIntText(
        value=bottom_x_value,
        min=1,
        max=100,
        step=1,
        description='Bottom X (%):',
        style={'description_width': '150px'},
        layout=widgets.Layout(width='700px', margin='5px 0')
    )

    # for displaying api errors in the bottom of the form
    form_error = widgets.HTML(value='', layout=widgets.Layout(width='700px'))

    from src.logic.event_handlers import setup_event_handlers, handle_continue_click

    continue_button = button(
        'Continue',
        lambda b: handle_continue_click(),
        button_color='#2196F3'
    )
    continue_button.disabled = True

    # store all widgets in state to access them in event handlers
    state.benchmark_input = benchmark_input
    state.api_id_input = api_id_input
    state.api_key_input = api_key_input
    state.min_alpha_input = min_alpha_input
    state.top_x_input = top_x_input
    state.bottom_x_input = bottom_x_input
    state.form_error = form_error
    state.continue_button = continue_button

    button_container = widgets.HBox(
        [continue_button],
        layout=widgets.Layout(justify_content='flex-end', width='700px', margin='10px 0 0 0')
    )

    # event handlers for form validation
    setup_event_handlers()

    header = build_header(state.current_step)

    # note about required column
    price_note = widgets.HTML(
        value='''<div style="color: #666; font-size: 12px; font-style: italic; margin: -5px 0 10px 0; width: 700px;">
            <strong>Note:</strong>
            <ul style="margin: 5px 0 0 0; padding-left: 20px;">
                <li>Your dataset must contain a column named "Last Close" for price data</li>
                <li>We support relative and absolute file paths for parquet/csv file formats</li>
            </ul>
        </div>'''
    )

    # build form with conditional data sources
    form_children = [section_header('Data Sources')]
    form_children.extend(data_sources_widgets)

    # only show price note for external app mode (when showing dataset input)
    if not state.is_internal_app:
        form_children.append(price_note)

    form_children.extend([
        section_header('Configuration'),
        benchmark_input,
        section_header('Authentication'),
        api_id_input,
        api_key_input,
        section_header('Analysis Parameters'),
        min_alpha_input,
        top_x_input,
        bottom_x_input,
        form_error,
        button_container
    ])

    form_fields = widgets.VBox(form_children)


    step1_container = centered_container([widgets.VBox([
        header,
        form_fields
    ])])

    return step1_container


def generate_step2_content(
    raw_data: pd.DataFrame,
    formulas_data: pd.DataFrame,
    benchmark_ticker: str,
    actual_row_count: int = None,
    unique_dates_count: int = None
) -> widgets.VBox:
    """
    Generate Step 2 review content with dataset stats and tables.

    Args:
        raw_data: Dataset to display (may be preview for Parquet files)
        formulas_data: Formulas to display
        benchmark_ticker: Benchmark ticker name
        actual_row_count: Actual total row count (for Parquet previews)
        unique_dates_count: Actual unique dates count (for Parquet previews)

    Returns:
        VBox widget with review content
    """
    num_rows = actual_row_count if actual_row_count is not None else len(raw_data)
    num_columns = len(raw_data.columns)

    dates = pd.to_datetime(raw_data['Date'])
    # Use passed unique_dates_count if available, otherwise calculate from data
    unique_dates = unique_dates_count if unique_dates_count is not None else dates.nunique()
    min_date = format_date(dates.min())
    max_date = format_date(dates.max())

    rows_box = review_stat_box("Rows", f"{num_rows:,}")
    dates_box = review_stat_box("Dates", str(unique_dates))
    columns_box = review_stat_box("Columns", str(num_columns))
    period_box = review_stat_box("Period", f"{min_date} - {max_date}")
    benchmark_box = review_stat_box("Benchmark", benchmark_ticker)

    review_section = widgets.GridBox(
        children=[rows_box, dates_box, columns_box, period_box, benchmark_box],
        layout=widgets.Layout(
            grid_template_columns='repeat(3, 1fr)',
            grid_gap='10px',
            padding='15px',
            margin='0',
            background_color='#ffffff',
            width='auto',
            box_shadow='0 2px 8px rgba(0, 0, 0, 0.1)'
        )
    )

    # Create formulas DataGrid
    if formulas_data is not None and not formulas_data.empty:
        formulas_display = formulas_data[['formula', 'name', 'tag']].copy()

        formulas_grid = create_data_grid(
            formulas_display,
            height='400px',
            width='100%',
            base_row_size=40,
            auto_fit_columns=False,
            column_widths={
                'index': 80,
                'formula': 250,
                'name': 200,
                'tag': 110
            }
        )

        formulas_grid.transform(
            [{"type": "text", "columnIndex": 0, "font": "monospace", "fontSize": 13}]
        )

        tag_renderer = TextRenderer(
            text_color='#2196F3',
            background_color='#e3f2fd',
            font_size=12
        )
        formulas_grid.renderers = {'tag': tag_renderer}

        formulas_table = widgets.VBox(
            [formulas_grid],
            layout=widgets.Layout(
                margin='20px 0 0 0',
                border='1px solid #cbd5e0',
                border_radius='10px',
                overflow='hidden',
                box_shadow='0 2px 8px rgba(0, 0, 0, 0.1)'
            )
        )
    else:
        formulas_table = widgets.HTML(
            value='<div style="padding: 20px;">No formulas available</div>'
        )

    formulas_tab = widgets.VBox([formulas_table])

    first_10 = raw_data.head(10)
    last_10 = raw_data.tail(10)
    total_rows = len(raw_data)

    if total_rows > 20:
        preview_df = pd.concat([first_10, last_10], ignore_index=False)
    else:
        preview_df = raw_data.copy()

    preview_message = f"Showing first 10 and last 10 rows"

    dataset_grid = create_data_grid(
        preview_df,
        height='500px',
        width='100%',
        base_row_size=35
    )

    dataset_preview = widgets.VBox(
        [
            widgets.HTML(
                value=f'<div style="padding: 8px; background-color: #f0f0f0; text-align: center; font-size: 12px; color: #666;">{preview_message}</div>'
            ),
            dataset_grid
        ],
        layout=widgets.Layout(
            margin='20px 0 0 0',
            border='1px solid #cbd5e0',
            border_radius='10px',
            overflow='hidden',
            box_shadow='0 2px 8px rgba(0, 0, 0, 0.1)'
        )
    )

    dataset_tab = widgets.VBox([dataset_preview])

    tab = widgets.Tab()
    tab.children = [formulas_tab, dataset_tab]
    tab.set_title(0, 'Formulas')
    tab.set_title(1, 'Dataset')

    return widgets.VBox(
        children=[review_section, tab],
        layout=widgets.Layout(
            width='700px',
            margin='0',
            padding='0'
        )
    )


def build_step2(
    raw_data: Optional[pd.DataFrame],
    formulas_data: pd.DataFrame,
    benchmark_ticker: str,
    step1_container: widgets.VBox,
    step2_container: widgets.VBox,
    revisit: bool = False
) -> None:
    """
    Build Step 2 UI (review data).

    Args:
        raw_data: Dataset to display (None for Parquet files, triggers preview load)
        formulas_data: Formulas DataFrame
        benchmark_ticker: Benchmark ticker name
        step1_container: Reference to step 1 container (to hide it)
        step2_container: Reference to step 2 container (to populate and show it)
        revisit: If True, just show existing container without rebuilding

    Returns:
        None (modifies step2_container in place)
    """
    from src.logic.event_handlers import handle_analyze_factors_click

    # update step
    state.current_step = 2

    state.completed_steps.add(2)

    # if revisiting, just show existing container with updated header
    if revisit and step2_container.children:
        new_header = build_header(2, width='700px', update_only=False)
        root_box = step2_container.children[0]
        inner_vbox = root_box.children[0]
        inner_children = list(inner_vbox.children)
        inner_children[0] = new_header
        inner_vbox.children = tuple(inner_children)

        step1_container.layout.display = 'none'
        step2_container.layout.display = 'flex'
        return

    actual_row_count = None
    unique_dates = None
    if state.file_type == 'parquet' and raw_data is None:
        log_debug("Loading Parquet preview (first 10 + last 10 rows)...")

        reader = get_data_reader(state.dataset_path)
        preview_df = reader.read_preview(num_rows=10)

        log_debug(f"Preview loaded: {len(preview_df)} rows")

        metadata = reader.get_metadata()
        actual_row_count = metadata.get('num_rows', len(preview_df))
        unique_dates = metadata.get('unique_dates')

        display_df = preview_df
        # display_df would be the dataset but only with top 10 and bottom 10 rows here
    else:
        # here it's everything, because it's a csv and not memory optimized
        display_df = raw_data

    header = build_header(2, width='700px')

    step2_html = generate_step2_content(display_df, formulas_data, benchmark_ticker, actual_row_count, unique_dates)

    analyze_button = button(
        'Analyze',
        lambda b: handle_analyze_factors_click(),
        button_color='#2196F3'
    )

    state.analyze_button = analyze_button

    button_container = widgets.HBox(
        [analyze_button],
        layout=widgets.Layout(justify_content='flex-end', width='700px', margin='20px 0 0 0')
    )

    step2_content = widgets.VBox([
        header,
        step2_html,
        button_container
    ])

    step2 = centered_container([step2_content])

    # hide step 1, show step 2
    step1_container.layout.display = 'none'

    step2_container.children = [step2]
    step2_container.layout.display = 'flex'


def build_results_table(best_features: list, metrics_df: pd.DataFrame) -> widgets.Widget:
    """
    Helper function to build the results table.

    Args:
        best_features: List of best feature names
        metrics_df: DataFrame with factor metrics

    Returns:
        Widget containing the results table
    """
    if best_features:
        best_metrics_df = metrics_df[metrics_df['column'].isin(best_features)]
        best_metrics_df = best_metrics_df.sort_values(by='annualized alpha %', key=abs, ascending=False)

        display_df = best_metrics_df.copy()
        display_df = display_df.rename(columns={
            'column': 'Factor',
            'annualized alpha %': 'Ann. Alpha %',
            'T Statistic': 'T-Statistic',
            'p-value': 'p-value'
        })

        display_df['Ann. Alpha %'] = display_df['Ann. Alpha %'].apply(lambda x: f"{x:.2f}%")
        display_df['T-Statistic'] = display_df['T-Statistic'].apply(lambda x: f"{x:.4f}")
        display_df['p-value'] = display_df['p-value'].apply(lambda x: f"{x:.6f}")

        display_df = display_df[['Factor', 'Ann. Alpha %', 'T-Statistic', 'p-value']]

        metrics_grid = create_data_grid(
            display_df,
            height='400px',
            width='100%',
            base_row_size=40,
            alternating_row_color='#ffffff',
            row_background_color='#ffffff',
            auto_fit_columns=False,
            column_widths={
                'index': 80,
                'Factor': 200,
                'Ann. Alpha %': 150,
                'T-Statistic': 150,
                'p-value': 110
            }
        )

        return widgets.VBox(
            [metrics_grid],
            layout=widgets.Layout(
                width='100%',
                border_radius='8px',
                overflow='hidden',
                background_color='#fff'
            )
        )
    else:
        return widgets.HTML(
            value='<div style="padding: 20px; background-color: #fff3cd; border-left: 4px solid #ffc107; border-radius: 4px;"><strong>No features found matching the current criteria. Try adjusting the parameters.</strong></div>'
        )


def build_step3(
    best_features: list,
    metrics_df: pd.DataFrame,
    step2_container: widgets.VBox,
    revisit: bool = False
) -> widgets.VBox:
    """
    Build Step 3 UI (results) with interactive controls.

    Args:
        best_features: List of best feature names
        metrics_df: DataFrame with factor metrics
        step2_container: Reference to step 2 container (to hide it)
        revisit: If True, just show existing container without rebuilding

    Returns:
        VBox widget containing Step 3
    """
    from src.logic.calculations import select_best_features

    # if revisiting and widgets exist, just update header and results
    if revisit and state.step3_container and state.step3_container.children:
        state.completed_steps.add(3)
        new_header = build_header(3, width='700px', update_only=False)
        centered = state.step3_container.children[0]
        step3_content_box = centered.children[0]
        inner_children = list(step3_content_box.children)
        inner_children[0] = new_header
        step3_content_box.children = tuple(inner_children)

        # update the results table with current best features
        best_features_widget = build_results_table(best_features, metrics_df)
        state.results_container.children = [best_features_widget]

        step2_container.layout.display = 'none'
        state.step3_container.layout.display = 'flex'
        return state.step3_container

    state.completed_steps.add(3)

    header = build_header(3, width='700px')

    correlation_input = widgets.FloatText(
        value=state.correlation_threshold,
        min=0.0,
        max=1.0,
        step=0.05,
        description='Correlation Threshold:',
        style={'description_width': '155px'},
        layout=widgets.Layout(width='220px', margin='0 15px 0 0')
    )

    n_features_input = widgets.IntText(
        value=state.n_features,
        min=1,
        max=50,
        description='Number of Features:',
        style={'description_width': '155px'},
        layout=widgets.Layout(width='220px', margin='0')
    )

    state.correlation_slider = correlation_input
    state.n_features_input = n_features_input

    best_features_widget = build_results_table(best_features, metrics_df)

    results_container = widgets.VBox(
        [best_features_widget],
        layout=widgets.Layout(width='100%')
    )
    state.results_container = results_container

    # event handler for parameter changes
    def on_parameter_change(_change):
        log_debug(f"Parameters changed - Correlation: {correlation_input.value}, N: {n_features_input.value}")

        state.correlation_threshold = correlation_input.value
        state.n_features = n_features_input.value

        # re-filter features with new parameters
        new_best_features = select_best_features(
            state.all_metrics,
            state.all_corr_matrix,
            N=state.n_features,
            correlation_threshold=state.correlation_threshold,
            a_min=state.min_alpha
        )

        log_debug(f"Re-filtered: Found {len(new_best_features)} features")

        new_table = build_results_table(new_best_features, state.all_metrics)
        results_container.children = [new_table]

    correlation_input.observe(on_parameter_change, names='value')
    n_features_input.observe(on_parameter_change, names='value')

    inputs_row = widgets.HBox([
        correlation_input,
        n_features_input,
    ], layout=widgets.Layout(width='700px', margin='10px 0', overflow='hidden'))

    controls_section = widgets.VBox([
        section_header('Filter Parameters'),
        inputs_row,
    ], layout=widgets.Layout(width='700px'))

    statistics_content = widgets.VBox(
        [
           section_header('Best Performing Factors'),
            results_container
        ],
        layout=widgets.Layout(width='700px')
    )

    step3_content = widgets.VBox(
        [header, controls_section, statistics_content],
        layout=widgets.Layout(width='700px')
    )

    step3_container = centered_container([step3_content])

    # hide step 2
    step2_container.layout.display = 'none'

    return step3_container
