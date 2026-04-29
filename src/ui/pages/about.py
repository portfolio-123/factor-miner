import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.internal.content import get_about_intro
from src.internal.links import p123_link


def about() -> None:
    st.title("FactorMiner")

    fl_id = st.query_params.get("fl_id")
    if not fl_id:
        st.warning("No Factor List selected. Please select a Factor List to view analysis history.")
        return

    if INTERNAL_MODE:
        st.markdown(get_about_intro(fl_id, p123_link(fl_id)))
    else:
        st.markdown(f"""
FactorMiner is a tool designed to run analyses over datasets and identify the factors with highest returns relative to a benchmark. It automates the process of analyzing factor performance, determining alpha and beta, and selecting uncorrelated factors in your portfolio.

**Getting Started:**
1. Select a dataset from the sidebar dropdown
2. Click on [New Analysis](/create?fl_id={fl_id}) to run your analysis
3. View your results in **Your Results**
""")
    st.markdown("""
### Analysis Parameters

When configuring a new analysis, you can adjust the following parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| **Rank By** | Alpha | Metric to rank factors by (Alpha or IC) |
| **High Quantile (%)** | 10.0 | Percentage of top-ranked stocks to include in the high quantile portfolio |
| **Low Quantile (%)** | 10.0 | Percentage of bottom-ranked stocks to include in the low quantile portfolio |
| **Min/Max. Alpha (%)** | 0.5 | Minimum/Maximum alpha threshold - factors with alpha below this are filtered out |
| **Min. IC** | 0.01 | Minimum information coefficient threshold for factor selection |
| **Max. Factors** | 10 | Maximum number of factors to select in the final result set |
| **Max. NA (%)** | 40.0 | Maximum percentage of missing values allowed before a factor is excluded |
| **Correlation Threshold** | 0.5 | Maximum allowed correlation between selected factors to ensure diversification |

### Analysis Results

After running an analysis, you'll be able to access the following results:

- **Best Factors** - The factors with the highest rank metric, after filters.
- **All Factors** - All factors ranked by the rank metric, with their respective classification.
- **Correlations** - The correlation matrix for the best factors and a conflict visualizer.
""")
