import streamlit as st


def load_global_css() -> None:
    st.markdown(
        """
    <style>
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
        background-color: #2196F3;
    }
    .stButton > button[kind="primary"]:disabled {
        color: white;
    }

    .stButton > button[kind="primary"]:hover {
        background-color: #1976D2;
    }

    /* Dataframe styling */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 8px 16px;
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        font-weight: 500;
    }

    /* Input field styling */
    .stTextInput > div > div > input {
        border-radius: 8px;
    }

    .stNumberInput > div > div > input {
        border-radius: 8px;
    }

    /* Slider styling */
    .stSlider [data-baseweb="slider"] [role="slider"] {
        background-color: #2196F3 !important;
    }

    /* ===== Arrow Breadcrumb Navigation ===== */
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    div[data-testid="stHorizontalBlock"] {
        gap: 0 !important;
    }

    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"] button {
        border-radius: 0 !important;
        border: none !important;
        margin: 0 !important;
        padding: 10px 20px !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        height: 40px !important;
        position: relative;
        clip-path: polygon(0 0, calc(100% - 10px) 0, 100% 50%, calc(100% - 10px) 100%, 0 100%, 10px 50%);
    }

    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:first-child button {
        clip-path: polygon(0 0, calc(100% - 10px) 0, 100% 50%, calc(100% - 10px) 100%, 0 100%);
        border-radius: 4px 0 0 4px !important;
    }

    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:last-child button {
        clip-path: polygon(0 0, 100% 0, 100% 100%, 0 100%, 10px 50%);
        border-radius: 0 4px 4px 0 !important;
    }

    /* Primary (current step) - blue */
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    button[kind="primary"] {
        background-color: #2196F3 !important;
        color: white !important;
        font-weight: 600 !important;
    }

    /* Secondary (other steps) - style based on state */
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    button[kind="secondary"] {
        background-color: #E8E8E8 !important;
        color: #424242 !important;
    }

    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    button[kind="secondary"]:hover:not(:disabled) {
        background-color: #BDBDBD !important;
    }

    /* Disabled buttons - locked state */
    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(2)
    button:disabled {
        background-color: #F5F5F5 !important;
        color: #BDBDBD !important;
    }

    div[data-testid="stHorizontalBlock"] > div[data-testid="stColumn"]:nth-of-type(3) button {
        padding: 10px 20px !important;
        font-size: 14px !important;
        font-weight: 500 !important;
        height: 40px !important;
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
        color: #6c757d;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }
    .dataset-info-item .value {
        font-size: 14px;
        font-weight: 500;
        color: #212529;
    }
    .dataset-info-item.big .label {
        font-size: 12px;
        font-weight: 600;
        color: #2196F3;
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
        color: #6c757d;
    }

    /* ===== Job Card Content Styles ===== */
    .job-card-content {
        display: flex;
        align-items: center;
        gap: 24px;
        padding: 0 16px; /* Vertical padding handled by height */
        height: 52px;
        box-sizing: border-box;
        /* Matches link style minus border/hover */
        border: 1px solid transparent; /* Placeholder to match height */
        border-radius: 8px;
        background-color: white;
    }
    .job-card-name {
        font-size: 14px;
        font-weight: 600;
        color: #212529;
        white-space: nowrap;
    }
    .job-card-params {
        display: flex;
        gap: 24px;
        align-items: center;
        flex: 1;
    }
    .job-card-right {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-left: auto;
    }
    .job-card-date {
        font-size: 13px;
        font-weight: 400;
        color: #9ca3af;
        white-space: nowrap;
    }
    .job-card-param {
        display: flex;
        align-items: center;
        gap: 6px;
    }
    .job-card-param .label {
        font-size: 11px;
        color: #64748b;
        text-transform: uppercase;
        margin-right: 6px;
    }
    .job-card-param .value {
        font-size: 13px;
        font-weight: 500;
        color: #333;
    }
    .job-card-status {
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 10px;
        font-weight: 600;
        text-transform: capitalize;
        letter-spacing: 0.4px;
        white-space: nowrap;
    }

    /* JOB CARD BUTTON OVERLAY STYLES */
    /* 1. Target the markdown container to remove bottom margin */
    .element-container:has(.job-card-trigger) {
        margin-bottom: 0 !important;
    }

    /* 2. Target button following the trigger */
    .element-container:has(.job-card-trigger) + div button {
        /* Position to cover the card */
        margin-top: -52px !important; /* Exact match for height */
        height: 52px !important;
        width: 100% !important;
        display: block !important;
        cursor: pointer !important;
        
        /* Visuals */
        background-color: transparent !important; /* Start transparent */
        border: 1px solid #e5e7eb !important;
        border-radius: 8px !important;
        transition: all 0.2s ease !important;
        z-index: 5;
        
        /* Text handling */
        color: transparent !important; /* Hide button label */
        
        /* Spacing for next card */
        margin-bottom: 6px !important;
    }
    
    .element-container:has(.job-card-trigger) + div button:hover {
        border-color: #2196F3 !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
        background-color: rgba(255, 255, 255, 0) !important; /* Ensure transparent bg */
    }
    
    .element-container:has(.job-card-trigger) + div button:focus {
        border-color: #2196F3 !important;
        box-shadow: none !important;
        color: transparent !important;
    }
    
    .element-container:has(.job-card-trigger) + div button:active {
        background-color: rgba(33, 150, 243, 0.05) !important;
        border-color: #2196F3 !important;
        color: transparent !important;
    }
    
    /* Ensure inner p is hidden */
    .element-container:has(.job-card-trigger) + div button p {
        display: none;
    }

    /* New Analysis button alignment and styling */
    div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"]:last-child .stButton {
        display: flex;
        justify-content: flex-end;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stColumn"]:last-child .stButton button {
        font-weight: 400 !important;
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
        color: #2196F3
    }
    .breadcrumb span {
        font-weight: bold;
        color: #333;
    }

    .stApp [data-testid="stToolbar"] {
        display: none;
    }

    /* ===== View Factors Button (link style) ===== */
    .element-container:has(.view-factors-trigger) + .element-container button,
    div[data-testid="stElementContainer"]:has(.view-factors-trigger) + div[data-testid="stElementContainer"] button {
        background: none !important;
        border: none !important;
        padding: 0 !important;
        box-shadow: none !important;
        min-height: auto !important;
        height: auto !important;
        width: auto !important;
    }
    .element-container:has(.view-factors-trigger) + .element-container button p,
    div[data-testid="stElementContainer"]:has(.view-factors-trigger) + div[data-testid="stElementContainer"] button p {
        color: #212529 !important;
        font-size: 18px !important;
        font-weight: 600 !important;
        text-decoration: underline !important;
        margin: 0 !important;
        padding: 0 !important;
        line-height: 1.2 !important;
        font-family: "Source Sans";
    }
    .element-container:has(.view-factors-trigger) + .element-container button:hover p,
    div[data-testid="stElementContainer"]:has(.view-factors-trigger) + div[data-testid="stElementContainer"] button:hover p {
        color: #2196F3 !important;
    }
    </style>
    """,
        unsafe_allow_html=True,
    )
