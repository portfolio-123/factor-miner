from typing import List, Callable, Tuple, Optional, Dict
import ipywidgets as widgets
import pandas as pd
from ipydatagrid import DataGrid, Expr


def step_indicator(current_step: int, completed_steps: set = None) -> widgets.HBox:
    """Create step indicator with clickable completed steps to go back and forth

    Args:
        current_step: The current step number (1, 2, or 3)
        completed_steps: Set of completed step numbers

    Returns:
        HBox widget containing the step indicator
    """
    from src.core.context import state
    from src.logic.event_handlers import handle_step_navigation

    from src.core.utils import log_debug

    if completed_steps is None:
        completed_steps = state.completed_steps if hasattr(state, 'completed_steps') else set()

    log_debug(f"step_indicator called: current_step={current_step}, completed_steps={completed_steps}")

    steps: List[str] = ['Settings', 'Review', 'Run']
    step_widgets: List[widgets.Widget] = []

    for i, step in enumerate(steps, 1):
        is_completed = i in completed_steps
        is_clickable = is_completed and i != current_step

        # define button styling based on state
        if i == current_step:
            # current step - blue, disabled
            button_bg_color = '#2196F3'
            button_text_color = '#ffffff'
            border_style = '2px solid #1976D2'
            font_weight = '600'
            tooltip_text = f'Current Step: {step}'
        elif is_completed:
            # completed step - green, enabled, clickable
            button_bg_color = '#e8f5e9'
            button_text_color = '#2e7d32'
            border_style = '2px solid #4CAF50'
            font_weight = '500'
            tooltip_text = f'Click to go to Step {i}: {step}'
        else:
            # future step - gray, disabled
            button_bg_color = '#f5f5f5'
            button_text_color = '#9e9e9e'
            border_style = '2px solid #e0e0e0'
            font_weight = '400'
            tooltip_text = f'Complete previous steps first'

        step_button = widgets.Button(
            description=f'{i}. {step}',
            tooltip=tooltip_text,
            disabled=not is_clickable,
            layout=widgets.Layout(
                width='auto',
                height='40px',
                padding='0 12px',
                border=border_style,
                border_radius='20px'
            ),
            style=widgets.ButtonStyle(
                button_color=button_bg_color,
                font_weight=font_weight,
                text_color=button_text_color
            )
        )

        # only attach click handler for clickable steps
        if is_clickable:
            step_button.on_click(lambda b, step_num=i: handle_step_navigation(step_num))

        step_widgets.append(step_button)

        if i < len(steps):
            next_step_completed = (i + 1) in completed_steps
            next_is_current = (i + 1) == current_step

            if next_is_current:
                arrow_color = '#2196F3'
            elif next_step_completed:
                arrow_color = '#4CAF50'
            else:
                arrow_color = '#e0e0e0'

            arrow = widgets.HTML(
                value=f'<div style="color: {arrow_color}; margin: auto 20px; font-size: 16px; font-weight: 600;">→</div>'
            )
            step_widgets.append(arrow)

    return widgets.HBox(
        step_widgets,
        layout=widgets.Layout(justify_content='center')
    )


def path_input_field(
    placeholder: str = 'data/dataset.parquet, formulas.csv',
    width: str = '550px'
) -> Tuple[widgets.Text, widgets.VBox]:
    path_input = widgets.Text(
        placeholder=placeholder,
        layout=widgets.Layout(width=width)
    )

    container = widgets.VBox(
        [path_input],
        layout=widgets.Layout(width=width, overflow='visible')
    )

    return path_input, container


def labeled_row(
    label_text: str,
    widget: widgets.Widget,
    label_width: str = '150px',
    total_width: str = '700px'
) -> widgets.HBox:
    label = widgets.HTML(
        value=f'<div style="text-align: right; padding-right: 8px;">{label_text}:</div>',
        layout=widgets.Layout(width=label_width)
    )

    return widgets.HBox(
        [label, widget],
        layout=widgets.Layout(width=total_width, margin='5px 0')
    )


def file_path_input(
    label: str,
    placeholder: str = 'data/dataset.parquet, formulas.csv',
    label_width: str = '150px',
    field_width: str = '542px',
    total_width: str = '700px'
) -> Tuple[widgets.Text, widgets.HBox]:
    path_input, input_container = path_input_field(placeholder, field_width)
    row = labeled_row(label, input_container, label_width, total_width)
    return path_input, row


def dropdown(
    description: str,
    options: List[str],
    default_value: Optional[str] = None,
    description_width: str = '150px',
    width: str = '700px'
) -> widgets.Dropdown:
    value = default_value if default_value is not None else options[0]

    return widgets.Dropdown(
        options=options,
        value=value,
        description=f'{description}:',
        style={'description_width': description_width},
        layout=widgets.Layout(width=width, margin='5px 0')
    )


def text_input(
    description: str,
    placeholder: str = '',
    description_width: str = '150px',
    width: str = '700px'
) -> widgets.Text:
    return widgets.Text(
        placeholder=placeholder,
        description=f'{description}:',
        style={'description_width': description_width},
        layout=widgets.Layout(width=width, margin='5px 0')
    )


def number_input(
    description: str,
    value: float = 0.0,
    min_value: float = 0.0,
    max_value: float = 100.0,
    step: float = 0.1,
    description_width: str = '150px',
    width: str = '700px'
) -> widgets.FloatText:
    return widgets.FloatText(
        value=value,
        min=min_value,
        max=max_value,
        step=step,
        description=f'{description}:',
        style={'description_width': description_width},
        layout=widgets.Layout(width=width, margin='5px 0')
    )


def button(
    description: str,
    on_click: Callable,
    button_style: str = 'primary',
    width: str = '120px',
    height: str = '40px',
    button_color: str = None
) -> widgets.Button:
    button = widgets.Button(
        description=description,
        button_style=button_style,
        layout=widgets.Layout(
            width=width,
            height=height,
            border_radius='20px'
        )
    )
    if button_color:
        button.style.button_color = button_color
    button.style.font_weight = '600'
    button.style.font_size = '14px'
    button.on_click(on_click)
    return button


def radio_buttons(
    description: str,
    options: List[str],
    default_value: Optional[str] = None,
    label_width: str = '150px',
    total_width: str = '700px'
) -> Tuple[widgets.HBox, widgets.RadioButtons]:
    value = default_value if default_value is not None else options[0]

    label = widgets.HTML(
        value=f'<div style="text-align: right; padding-right: 8px;">{description}:</div>',
        layout=widgets.Layout(width=label_width)
    )

    radio = widgets.RadioButtons(
        options=options,
        value=value,
        description='',
        disabled=False,
        layout=widgets.Layout(flex='1')
    )

    radio.add_class('horizontal-radio')

    row = widgets.HBox(
        [label, radio],
        layout=widgets.Layout(width=total_width, margin='5px 0', align_items='center')
    )

    return row, radio


def centered_container(
    children: List[widgets.Widget],
) -> widgets.HBox:
    return widgets.HBox(
        children,
        layout=widgets.Layout(
            justify_content='center',
            align_items='center',
            width='100%'
        )
    )


def brand_section() -> widgets.HTML:
    return widgets.HTML(
        value='''
            <div style="padding: 5px 0;">
                <div class="brand-title">Portfolio123</div>
                <div class="brand-subtitle">Factor Evaluator</div>
            </div>
        '''
    )


def section_header(title: str) -> widgets.HTML:
    return widgets.HTML(
        value=f'<div class="section-header">{title}</div>',
        layout=widgets.Layout(width='700px')
    )


def review_stat_box(label: str, value: str) -> widgets.VBox:
    label_widget = widgets.HTML(
        value=f'<div style="font-weight: 700; color: #2196F3; margin-bottom: 3px; font-size: 12px; line-height: 1.2; text-transform: uppercase; letter-spacing: 0.5px;">{label}</div>'
    )
    value_widget = widgets.HTML(
        value=f'<div style="color: #1a202c; line-height: 1.2; font-size: 15px; font-weight: 600;">{value}</div>'
    )
    return widgets.VBox([label_widget, value_widget])


def show_error(message: str, form_error_widget: widgets.HTML) -> None:
    form_error_widget.value = f'<div class="error-box">{message}</div>'


def debug_console() -> Tuple[widgets.VBox, widgets.Output, widgets.Button]:
    from datetime import datetime

    debug_output = widgets.Output(
        layout=widgets.Layout(
            width='100%',
            height='auto',
            max_height='calc(90vh - 150px)',
            overflow_y='auto',
            overflow_x='hidden',
            border='1px solid #ddd',
            padding='10px',
            background_color='#f8f9fa',
            border_radius='4px',
            flex='1 1 auto'
        )
    )

    toggle_button = widgets.Button(
        description='Logs',
        layout=widgets.Layout(width='50px', height='32px'),
        style={'button_color': '#2196F3', 'text_color': 'white'},
    )
    close_button = widgets.Button(
        description='Close',
        button_style='',
        layout=widgets.Layout(width='100px', height='35px', margin='0 0 0 10px')
    )

    clear_button = widgets.Button(
        description='Clear Logs',
        button_style='warning',
        layout=widgets.Layout(width='120px', height='35px')
    )

    header = widgets.HTML(
        value='<h3 style="margin: 0 0 15px 0;">Logs</h3>'
    )

    footer = widgets.HBox(
        [clear_button, close_button],
        layout=widgets.Layout(
            justify_content='flex-end',
            width='100%',
            align_items='center',
        )
    )

    console_content = widgets.VBox(
        [header, debug_output, footer],
        layout=widgets.Layout(
            width='auto',
            max_width='min(900px, 95vw)',
            max_height='90vh',
            padding='20px',
            border='2px solid #dee2e6',
            border_radius='8px',
            overflow='visible',
            display='flex'
        )
    )

    console_content.add_class('debug-console-content')

    console_wrapper = widgets.HBox(
        [console_content],
        layout=widgets.Layout(
            width='100%',
            height='100%',
            justify_content='center',
            align_items='center',
            padding='20px',
            overflow='hidden'
        )
    )

    overlay_container = widgets.VBox(
        [console_wrapper],
        layout=widgets.Layout(
            width='100%',
            height='100vh',
            display='none',
            overflow_y='hidden',
            overflow_x='hidden'
        )
    )

    overlay_container.add_class('debug-overlay')

    overlay_css = widgets.HTML(value='''
        <style>
        .debug-overlay {
            position: fixed !important;
            top: 0 !important;
            left: 0 !important;
            z-index: 9999 !important;
            background-color: rgba(0, 0, 0, 0.5) !important;
        }
        .debug-console-content {
            background-color: #ffffff !important;
            box-sizing: border-box !important;
            display: flex !important;
            flex-direction: column !important;
            max-height: 90vh !important;
        }
        .debug-console-content > * {
            flex-shrink: 0 !important;
        }
        .debug-console-content .widget-output {
            flex: 1 1 auto !important;
            min-height: 0 !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
            display: block !important;
        }
        .debug-console-content .jp-OutputArea {
            max-height: 100% !important;
            overflow-y: auto !important;
            overflow-x: hidden !important;
        }
        </style>
    ''')

    overlay_with_css = widgets.VBox([overlay_css, overlay_container])

    def show_console(_):
        overlay_container.layout.display = 'flex'

    def hide_console(_):
        overlay_container.layout.display = 'none'

    def clear_console(_):
        debug_output.clear_output()
        with debug_output:
            timestamp = datetime.now().strftime('%H:%M:%S')
            print(f"[{timestamp}] Logs cleared")

    toggle_button.on_click(show_console)
    close_button.on_click(hide_console)
    clear_button.on_click(clear_console)

    with debug_output:
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] Logs initialized")

    return overlay_with_css, debug_output, toggle_button


def create_data_grid(
    df: pd.DataFrame,
    height: str = '400px',
    width: str = '100%',
    column_widths: Optional[Dict[str, int]] = None,
    header_background_color: str = '#2196F3',
    row_background_color: str = '#ffffff',
    alternating_row_color: Optional[str] = '#f8f9fa',
    selection_mode: str = 'row',
    base_row_size: int = 32,
    **kwargs
) -> DataGrid:
    """
    Create a styled DataGrid widget with consistent styling.

    Args:
        df: DataFrame to display
        height: Grid height (CSS value)
        width: Grid width (CSS value)
        column_widths: Optional dict mapping column names to widths in pixels
        header_background_color: Header background color
        row_background_color: Default row background color
        alternating_row_color: Alternate row background color (None to disable)
        selection_mode: Selection mode ('row', 'cell', 'none')
        base_row_size: Row height in pixels
        **kwargs: Additional DataGrid parameters

    Returns:
        Configured DataGrid widget
    """
    # Auto fit columns by default
    auto_fit = kwargs.pop('auto_fit_columns', True)

    grid = DataGrid(
        df,
        selection_mode=selection_mode,
        base_row_size=base_row_size,
        header_visibility='all',
        auto_fit_columns=auto_fit,
        layout=widgets.Layout(
            height=height,
            width=width
        ),
        **kwargs
    )

    if column_widths:
        widths_to_apply = dict(column_widths)
        if 'index' in widths_to_apply and 'key' not in widths_to_apply:
            widths_to_apply['key'] = widths_to_apply['index']

        try:
            existing = dict(getattr(grid, 'column_widths', {}) or {})
            existing.update(widths_to_apply)
            grid.column_widths = existing
        except Exception:
            for name, width in widths_to_apply.items():
                try:
                    grid.column_widths[name] = width
                except Exception:
                    pass

    if alternating_row_color:
        grid.default_renderer.background_color = Expr(
            f"'{alternating_row_color}' if cell.row % 2 == 0 else '{row_background_color}'"
        )

    return grid
