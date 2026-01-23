import streamlit as st


def load_global_css() -> None:
    st.html(
        """
    <style>
    :root {
        --primary: #2196F3;
        --primary-hover: #1976D2;
        --text-dark: #212529;
        --text-muted: #6c757d;
        --text-secondary: #424242;
        --bg-secondary: #E8E8E8;
        --bg-hover: #BDBDBD;
        --bg-disabled: #F5F5F5;
        --border-light: #e5e7eb;
        --radius: 8px;
        --radius-sm: 6px;
    }

    /* Reduce main content padding */
    .stMainBlockContainer,
    .block-container,
    [data-testid="stMainBlockContainer"] {
        padding: 0.5rem 2rem 2rem 2rem !important;
        max-width: 1000px !important;
        margin: 0 auto !important;
    }

    /* Button styling - consistent for all buttons */
    .stButton > button {
        border-radius: 6px;
        font-weight: 500;
    }

    .stButton > button[kind="primary"] {
        background-color: var(--primary);
    }
    .stButton > button[kind="primary"]:disabled {
        color: white;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: var(--primary-hover);
    }

    /* Dataframe styling */
    .stDataFrame {
        border-radius: var(--radius);
        overflow: hidden;
    }

    /* Slider styling */
    .stSlider [data-baseweb="slider"] [role="slider"] {
        background-color: var(--primary) !important;
    }

    /* ===== Debug Modal Styling ===== */
    div[data-testid="stModal"] > div {
        max-height: 95vh !important;
        max-width: 95vw !important;
    }

    div[data-testid="stModal"] [data-testid="stVerticalBlock"] {
        max-height: 85vh;
        overflow-y: auto;
    }

    /* ===== Step 0 / History Page Styles ===== */
    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }

    div[data-testid="stColumn"]:has(.analysis-badge-marker) > div[data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    .element-container:has(.analysis-badge-marker) + .element-container {
        margin-top: 8px !important;
    }

    /* Reduce divider margins */
    div[data-testid="stElementContainer"]:has(hr) {
        margin-top: 0 !important;
        margin-bottom: 0 !important;
    }
    hr {
        margin: 0.25rem 0 0.5rem 0 !important;
    }

    /* ===== Dataset Info Styles ===== */
    .dataset-info-group {
        display: flex;
        gap: 24px;
    }
    .dataset-info-item .label {
        font-size: 11px;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }
    .dataset-info-item .value {
        font-size: 14px;
        font-weight: 500;
        color: var(--text-dark);
    }
    .dataset-info-item.big .label {
        font-size: 12px;
        font-weight: 600;
        color: var(--primary);
        margin-bottom: 1px;
        text-transform: uppercase;
    }
    .dataset-info-item.big .value {
        font-size: 18px;
        font-weight: 600;
        color: #1a1a1a;
        line-height: 1.2;
    }

    .dataset-info-item .value.muted {
        color: var(--text-muted);
    }

    /* New Analysis button alignment and styling */
    div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"]:last-child .stButton {
        display: flex;
        justify-content: flex-end;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"]:last-child .stButton button {
        font-weight: 400;
    }

    /* ===== View Factors Button (link style) ===== */
    /* Remove gap in the FACTORS column container only */
    div[data-testid="stColumn"]:has(.view-factors-trigger) > div[data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    .element-container:has(.view-factors-trigger) + .element-container {
        margin-top: -2px !important;
    }
    .element-container:has(.view-factors-trigger) + .element-container button {
        background: none !important;
        border: none !important;
        padding: 0 !important;
        box-shadow: none !important;
        min-height: auto !important;
        height: auto !important;
        width: auto !important;
    }
    .element-container:has(.view-factors-trigger) + .element-container button p {
        color: var(--text-dark) !important;
        font-size: 17px !important;
        font-weight: 600 !important;
        text-decoration: underline !important;
        margin: 0 !important;
        padding: 0 !important;
        line-height: 1 !important;
        font-family: "Source Sans";
    }
    .element-container:has(.view-factors-trigger) + .element-container button:hover p {
        color: var(--primary) !important;
    }
    </style>
    """
    )
