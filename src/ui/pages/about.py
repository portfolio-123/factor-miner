import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.internal.content import get_about_intro
from src.internal.links import p123_link


def about() -> None:
    st.title("FactorMiner")

    fl_id = st.query_params.get("fl_id")

    if INTERNAL_MODE:
        st.markdown(get_about_intro(fl_id, p123_link(fl_id)))
    else:
        st.markdown(
            f"""
FactorMiner is a tool designed to run analyses over datasets and identify the factors with highest returns relative to a benchmark. It automates the process of analyzing factor performance, determining alpha and beta, and selecting uncorrelated factors in your portfolio.

**Getting Started:**
1. Select a dataset from the sidebar dropdown
2. Click on [New Analysis](/create?fl_id={fl_id}) to run your analysis
3. View your results in **Your Results**
"""
        )
    st.markdown("""
### Analysis Parameters

When configuring a new analysis, you can adjust the following parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| **Rank By** | Alpha | Metric to rank factors by (Alpha or IC) |
| **Top X (%)** | 10.0 | Percentage of top-ranked stocks to include in the long portfolio |
| **Bottom X (%)** | 10.0 | Percentage of bottom-ranked stocks to include in the short portfolio |
| **Min Absolute Alpha (%)** | 0.5 | Minimum alpha threshold - factors with absolute alpha below this are filtered out |
| **Min IC** | 0.015 | Minimum information coefficient threshold for factor selection |
| **Max. Factors** | 10 | Maximum number of factors to select in the final result set |
| **Max NA (%)** | 40.0 | Maximum percentage of missing values allowed before a factor is excluded |
| **Correlation Threshold** | 0.5 | Maximum allowed correlation between selected factors to ensure diversification |

### Analysis Results

After running an analysis, you'll be able to access the following results:

- **Best Factors** - The factors with the highest absolute annualized alpha, after alpha and correlation filters.
- **All Factors** - All factors ranked by abs. annual alpha, despite alpha or correlation filters.
- **Correlation Matrix** - The correlation matrix for the best performing factors.
""")
