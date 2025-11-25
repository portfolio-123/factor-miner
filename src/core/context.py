import pandas as pd
import ipywidgets as widgets
from pathlib import Path
from typing import Optional
from dataclasses import dataclass


@dataclass
class AppState:
    """Central state management for the application."""
    current_step: int = 1
    completed_steps: set = None

    PRICE_COLUMN: str = "Last Close"

    def __post_init__(self):
        if self.completed_steps is None:
            self.completed_steps = set()

    # internal app config
    is_internal_app: bool = False
    factor_list_uid: Optional[str] = None

    # auto-located file paths (internal app mode)
    auto_dataset_path: Optional[Path] = None
    auto_formulas_path: Optional[Path] = None
    files_verified: bool = False
    files_verification_error: Optional[str] = None

    # data state
    benchmark_data: Optional[pd.DataFrame] = None
    raw_data: Optional[pd.DataFrame] = None
    price_column: Optional[str] = None
    benchmark_ticker: Optional[str] = None
    api_id: Optional[str] = None
    api_key: Optional[str] = None

    file_type: Optional[str] = None  # 'csv' or 'parquet'
    dataset_path: Optional[Path] = None

    # calculation parameters
    min_alpha: float = 0.5
    top_x_pct: float = 20.0
    bottom_x_pct: float = 20.0
    correlation_threshold: float = 0.5
    n_features: int = 10

    # results storage for step 3 filtering
    all_metrics: Optional[pd.DataFrame] = None
    all_corr_matrix: Optional[pd.DataFrame] = None

    # debug widgets
    debug_output: Optional[widgets.Output] = None
    debug_toggle_button: Optional[widgets.Button] = None

    # step 1 widgets
    dataset_input: Optional[widgets.Text] = None
    formulas_input: Optional[widgets.Text] = None
    factor_list_uid_input: Optional[widgets.Text] = None
    benchmark_input: Optional[widgets.Text] = None
    api_id_input: Optional[widgets.Text] = None
    api_key_input: Optional[widgets.Text] = None
    min_alpha_input: Optional[widgets.FloatText] = None
    top_x_input: Optional[widgets.FloatText] = None
    bottom_x_input: Optional[widgets.FloatText] = None
    form_error: Optional[widgets.HTML] = None
    continue_button: Optional[widgets.Button] = None

    # step 2 widgets
    analyze_button: Optional[widgets.Button] = None

    # step 3 widgets
    correlation_slider: Optional[widgets.FloatSlider] = None
    n_features_input: Optional[widgets.IntText] = None
    results_container: Optional[widgets.VBox] = None

    step1_container: Optional[widgets.VBox] = None
    step2_container: Optional[widgets.VBox] = None
    step3_container: Optional[widgets.VBox] = None

    # header components to avoid re-rendering
    header_container: Optional[widgets.HBox] = None
    step_indicator_widget: Optional[widgets.HBox] = None

    # control flags
    suppress_invalidation: bool = False


# global state instance
state = AppState()
