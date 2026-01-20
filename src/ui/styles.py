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

    /* ===== Analysis Card Content Styles ===== */
    .analysis-card-content {
        display: flex;
        align-items: center;
        gap: 24px;
        padding: 0 16px; /* Vertical padding handled by height */
        height: 52px;
        box-sizing: border-box;
        /* Matches link style minus border/hover */
        border: 1px solid transparent; /* Placeholder to match height */
        border-radius: var(--radius);
        background-color: white;
    }
    .analysis-card-name {
        font-size: 14px;
        font-weight: 600;
        color: var(--text-dark);
        white-space: nowrap;
    }
    .analysis-card-params {
        display: flex;
        gap: 24px;
        align-items: center;
        flex: 1;
    }
    .analysis-card-right {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-left: auto;
    }
    .analysis-card-date {
        font-size: 13px;
        font-weight: 400;
        color: #9ca3af;
        white-space: nowrap;
    }
    .analysis-card-param {
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .analysis-card-param .label {
        font-size: 11px;
        color: #64748b;
        text-transform: uppercase;
        margin-right: 6px;
    }
    .analysis-card-param .value {
        font-size: 13px;
        font-weight: 500;
        color: #333;
    }
    .analysis-card-status {
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 10px;
        font-weight: 600;
        text-transform: capitalize;
        letter-spacing: 0.4px;
        white-space: nowrap;
    }

    /* ANALYSIS CARD BUTTON OVERLAY STYLES */
    /* 1. Target the markdown container to remove bottom margin */
    .element-container:has(.analysis-card-trigger) {
        margin-bottom: 0 !important;
    }

    /* 2. Target button following the trigger */
    .element-container:has(.analysis-card-trigger) + div button {
        /* Position to cover the card */
        margin-top: -52px !important; /* Exact match for height */
        height: 52px !important;
        width: 100% !important;
        display: block !important;
        cursor: pointer !important;

        /* Visuals */
        background-color: transparent !important; /* Start transparent */
        border: 1px solid var(--border-light) !important;
        border-radius: var(--radius) !important;
        transition: all 0.2s ease !important;
        z-index: 5;

        /* Text handling */
        color: transparent !important; /* Hide button label */

        /* Spacing for next card */
        margin-bottom: 6px !important;
    }

    .element-container:has(.analysis-card-trigger) + div button:hover {
        border-color: var(--primary) !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
        background-color: rgba(255, 255, 255, 0) !important; /* Ensure transparent bg */
    }

    .element-container:has(.analysis-card-trigger) + div button:focus {
        border-color: var(--primary) !important;
        box-shadow: none !important;
        color: transparent !important;
    }

    .element-container:has(.analysis-card-trigger) + div button:active {
        background-color: rgba(33, 150, 243, 0.05) !important;
        border-color: var(--primary) !important;
        color: transparent !important;
    }

    /* Ensure inner p is hidden */
    .element-container:has(.analysis-card-trigger) + div button p {
        display: none;
    }

    /* New Analysis button alignment and styling */
    div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"]:last-child .stButton {
        display: flex;
        justify-content: flex-end;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"]:last-child .stButton button {
        font-weight: 400;
    }


    .breadcrumb {
        font-size: 14px;
        margin-bottom: -15px;
    }
    .breadcrumb a {
        text-decoration: none;
        color: #666;
    }
    .breadcrumb a:hover {
        text-decoration: underline;
        color: var(--primary);
    }
    .breadcrumb span {
        font-weight: bold;
        color: #333;
    }

    .stApp [data-testid="stToolbar"] {
        display: none;
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

    /* ===== Copy Button Styles ===== */
    .copy-btn {
        border-radius: var(--radius-sm);
        font-weight: 500;
        font-size: 14px;
        padding: 8px 16px;
        cursor: pointer;
        transition: all 0.2s ease;
        font-family: "Source Sans Pro", sans-serif;
    }

    .copy-btn.primary {
        background-color: var(--primary);
        color: white;
        border: none;
    }

    .copy-btn.primary:hover {
        background-color: var(--primary-hover);
    }

    .copy-btn.secondary {
        background-color: var(--bg-secondary);
        color: var(--text-secondary);
        border: 1px solid var(--border-light);
    }

    .copy-btn.secondary:hover {
        background-color: var(--bg-hover);
    }

    .copy-btn.stretch {
        width: 100%;
    }

    .copy-btn.copied {
        background-color: #4CAF50;
    }

    </style>
    """
    )
